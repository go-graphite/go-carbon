package persister

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func incrementalPersisterFixture(t *testing.T) (*Whisper, *fakeCache, *store.Store, string) {
	t.Helper()
	return incrementalPersisterFixtureWith(t, &whisperio.ContinuationCodec{})
}

// incrementalPersisterFixtureWith is incrementalPersisterFixture with codec;
// its metric has an out-of-order sidecar.
func incrementalPersisterFixtureWith(t *testing.T, codec *whisperio.ContinuationCodec) (*Whisper, *fakeCache, *store.Store, string) {
	t.Helper()
	root := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
	p.SetFLock(true)
	now := time.Now().Unix()
	for _, point := range []struct {
		timestamp int64
		value     float64
	}{{now - 20, 1}, {now - 10, .1}, {now - 5, 2}} {
		cache.add("metric", point.timestamp, point.value)
		p.store("metric")
	}
	cache.add("metric", now-20, 9.25)
	p.store("metric")
	destination := filepath.Join(root, "exact")
	db, err := store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: codec, IncrementalSource: p.rootPath, SyncInterval: time.Hour}))
	if err != nil {
		t.Fatal(err)
	}
	p.SetMetricStore(db)
	if err := p.SetIncrementalMigration(db, destination); err != nil {
		t.Fatal(err)
	}
	return p, cache, db, destination
}

// TestIncrementalMigrationFoldsSidecarAcrossCrashBoundaries converts a metric
// with an out-of-order sidecar under direct late writes, whose import folds
// the sidecar into the converted blocks, interrupting it at each durable
// boundary and resuming it in a new process. The resumed conversion must
// publish (verifying the folded physical state against the source) and keep
// every value, and a later write must land as on the original file.
func TestIncrementalMigrationFoldsSidecarAcrossCrashBoundaries(t *testing.T) {
	codec := func() *whisperio.ContinuationCodec { return &whisperio.ContinuationCodec{DirectLateWrites: true} }
	for _, boundary := range []string{"staged", "owned", "unlink-0", "directory-synced"} {
		t.Run(boundary, func(t *testing.T) {
			p, cache, db, destination := incrementalPersisterFixtureWith(t, codec())
			ctx := context.Background()
			path := filepath.Join(p.rootPath, "metric.wsp")
			if _, err := os.Stat(whisper.OutOfOrderSidecarPath(path)); err != nil {
				t.Fatalf("fixture has no sidecar: %v", err)
			}
			want, err := whisperio.ReadSnapshot(ctx, "metric", path)
			if err != nil {
				t.Fatal(err)
			}
			folds := whisperio.ImportSidecarFolds(whisper.InPlaced)
			stop := errors.New("simulated interrupted conversion")
			if _, err := p.convertFile(ctx, "metric", func(stage string) error {
				if stage == boundary {
					return stop
				}
				return nil
			}); !errors.Is(err, stop) {
				t.Fatalf("boundary not reached: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: codec(), IncrementalSource: p.rootPath, SyncInterval: time.Hour}))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			p.SetMetricStore(db)
			if err := p.SetIncrementalMigration(db, destination); err != nil {
				t.Fatal(err)
			}
			if _, err := p.ConvertFile(ctx, "metric"); err != nil {
				t.Fatal(err)
			}
			if n := whisperio.ImportSidecarFolds(whisper.InPlaced) - folds; n == 0 {
				t.Fatal("the import did not fold the sidecar")
			}
			got, err := db.Snapshot(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			assertOriginalsReclaimed(t, p, "metric")
			now := time.Now().Unix()
			cache.add("metric", now-1, 42.5)
			p.store("metric")
			series, err := db.Fetch(ctx, "metric", int(now-60), int(now))
			if err != nil {
				t.Fatal(err)
			}
			if v := series.Values[(int(now-1)-series.FromTime)/series.Step]; v != 42.5 {
				t.Fatalf("write after conversion reads %v", v)
			}
		})
	}
}

func assertOriginalsReclaimed(t *testing.T, p *Whisper, metric string) {
	t.Helper()
	path, err := p.incrementalPath(metric)
	if err != nil {
		t.Fatal(err)
	}
	for _, original := range originalPaths(path) {
		if _, err := os.Lstat(original); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("original retained/recreated: %s %v", original, err)
		}
	}
}

func TestIncrementalMigrationPreservesFileActivityAcrossStagedRestart(t *testing.T) {
	p, cache, db, destination := incrementalPersisterFixture(t)
	ctx := context.Background()
	path := filepath.Join(p.rootPath, "metric.wsp")
	old := time.Now().Add(-6 * 24 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	want := info.ModTime()
	interrupted := errors.New("stop before publication")
	_, err = p.convertFile(ctx, "metric", func(stage string) error {
		if stage == "staged" {
			return interrupted
		}
		return nil
	})
	if !errors.Is(err, interrupted) {
		t.Fatalf("did not interrupt staged conversion: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath, SyncInterval: time.Hour}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p.SetMetricStore(db)
	if err := p.SetIncrementalMigration(db, destination); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConvertFile(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	metadata, err := db.Metadata(ctx, "metric")
	if err != nil || !metadata.LastUpdate.Equal(want) {
		t.Fatalf("conversion reset source expiration activity: %s want %s, %v", metadata.LastUpdate, want, err)
	}
	assertOriginalsReclaimed(t, p, "metric")
	beforeWrite := time.Now()
	cache.add("metric", beforeWrite.Unix()-1, 17.25)
	p.store("metric")
	metadata, err = db.Metadata(ctx, "metric")
	if err != nil || metadata.LastUpdate.Before(beforeWrite) || metadata.LastUpdate.After(time.Now()) {
		t.Fatalf("shared write did not refresh activity: %s, %v", metadata.LastUpdate, err)
	}
}

func TestIncrementalMigrationCrashBoundariesResumeWithoutReimport(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"staged", "owned", "unlink-0", "unlink-1", "unlink-2", "unlink-3", "directory-synced"} {
		t.Run(boundary, func(t *testing.T) {
			p, _, db, destination := incrementalPersisterFixture(t)
			ctx := context.Background()
			want, err := whisperio.ReadSnapshot(ctx, "metric", filepath.Join(p.rootPath, "metric.wsp"))
			if err != nil {
				t.Fatal(err)
			}
			stop := errors.New("simulated interrupted conversion")
			_, err = p.convertFile(ctx, "metric", func(stage string) error {
				if stage == boundary {
					return stop
				}
				return nil
			})
			if !errors.Is(err, stop) {
				t.Fatalf("boundary not reached: %v", err)
			}
			if boundary == "staged" {
				if _, err := db.Metadata(ctx, "metric"); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("staged candidate became visible: %v", err)
				}
				if _, err := os.Stat(filepath.Join(p.rootPath, "metric.wsp")); err != nil {
					t.Fatal("original was removed before ownership")
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath, SyncInterval: time.Hour}))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			p.SetMetricStore(db)
			if err := p.SetIncrementalMigration(db, destination); err != nil {
				t.Fatal(err)
			}
			result, err := p.ConvertFile(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			if result.PreviouslyOwned != (boundary != "staged") {
				t.Fatal("lost durable per-metric progress")
			}
			got, err := db.Snapshot(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			assertOriginalsReclaimed(t, p, "metric")
			pending, err := db.PendingFileCleanup(ctx, 100)
			if err != nil || len(pending) != 0 {
				t.Fatalf("cleanup queue not drained: %+v %v", pending, err)
			}
			if boundary != "staged" && got.Metadata.Generation != 1 {
				t.Fatal("restart decoded/replaced an already owned metric")
			}
		})
	}
}

func TestIncrementalMigrationKeepsLegacyWritesUntilPublication(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	now := time.Now().Unix()
	cache.add("metric", now-2, 77)
	p.store("metric")
	if _, err := db.Metadata(context.Background(), "metric"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("unconverted write accidentally created shared ownership")
	}
	want, err := whisperio.ReadSnapshot(context.Background(), "metric", filepath.Join(p.rootPath, "metric.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConvertFile(context.Background(), "metric"); err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(context.Background(), "metric")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("conversion lost writes accepted while migration was pending: ", err)
	}
}

func TestIncrementalMigrationWritesAfterReclamationMatchCWhisper(t *testing.T) {
	now := storageTestClock(t)
	root := t.TempDir()
	leftCache, rightCache := &fakeCache{}, &fakeCache{}
	left := newOOOTestPersister(t, filepath.Join(root, "original"), leftCache)
	right := newOOOTestPersister(t, filepath.Join(root, "control"), rightCache)
	left.SetFLock(true)
	right.SetFLock(true)
	// Include propagated rollups and an existing OOO sidecar in the handoff.
	retentions, err := ParseRetentionDefs("1s:2h,60s:7d")
	if err != nil {
		t.Fatal(err)
	}
	left.schemas[0].Retentions = retentions
	right.schemas[0].Retentions = retentions
	write := func(timestamp int64, value float64) {
		leftCache.add("metric", timestamp, value)
		rightCache.add("metric", timestamp, value)
		left.store("metric")
		right.store("metric")
	}
	for i := int64(1); i <= 300; i++ {
		write(now.Load()-600+i, float64(i))
	}
	write(now.Load()-500, .1)
	destination := filepath.Join(root, "exact")
	db, err := store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: left.rootPath, Now: whisper.Now, SyncInterval: time.Hour}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	left.SetMetricStore(db)
	if err := left.SetIncrementalMigration(db, destination); err != nil {
		t.Fatal(err)
	}
	if _, err := left.ConvertFile(context.Background(), "metric"); err != nil {
		t.Fatal(err)
	}
	for i, value := range []float64{99, math.Copysign(0, -1), .1, 1e-300, 1e100, 9007199254740992, -9007199254740992} {
		write(now.Load()-int64(50+i), value)
		write(now.Load()-int64(500+i), value)
	}
	want, err := whisperio.ReadSnapshot(context.Background(), "metric", filepath.Join(right.rootPath, "metric.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(context.Background(), "metric")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("shared-only OOO writes differ from actual CWhisper: ", err)
	}
	assertOriginalsReclaimed(t, left, "metric")
	if leftCache.confirmedPoints() != rightCache.confirmedPoints() {
		t.Fatal("write confirmation changed")
	}
	leftCache.add("brand.new", now.Load()-1, 42)
	left.store("brand.new")
	metadata, err := db.Metadata(context.Background(), "brand.new")
	if err != nil || metadata.FileMigration != store.FileReclaimed {
		t.Fatalf("new metric did not start shared-only: %+v %v", metadata, err)
	}
	assertOriginalsReclaimed(t, left, "brand.new")
}

func TestIncrementalPublicationWaitsForOriginalReadersAndCanCancel(t *testing.T) {
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	release := db.OriginalReadGuard("metric")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := p.ConvertFile(ctx, "metric"); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("publication ignored live reader: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publication deadlocked behind an original reader")
	}
	release()
	if _, err := os.Stat(filepath.Join(p.rootPath, "metric.wsp")); err != nil {
		t.Fatal("cancelled publication removed original")
	}
	if _, err := p.ConvertFile(context.Background(), "metric"); err != nil {
		t.Fatal(err)
	}
	assertOriginalsReclaimed(t, p, "metric")
}

func TestIncrementalMigrationAbruptExitHelper(t *testing.T) {
	original := os.Getenv("GO_CARBON_INCREMENTAL_CRASH_ORIGINAL")
	if original == "" {
		return
	}
	destination := os.Getenv("GO_CARBON_INCREMENTAL_CRASH_DESTINATION")
	p := newOOOTestPersister(t, original, &fakeCache{})
	p.SetFLock(true)
	db, err := store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: original, SyncInterval: time.Hour}))
	if err != nil {
		t.Fatal(err)
	}
	p.SetMetricStore(db)
	if err := p.SetIncrementalMigration(db, destination); err != nil {
		t.Fatal(err)
	}
	_, err = p.convertFile(context.Background(), "metric", func(stage string) error {
		if stage == os.Getenv("GO_CARBON_INCREMENTAL_CRASH_BOUNDARY") {
			os.Exit(79) // Deliberately skip every defer, including database close.
		}
		return nil
	})
	t.Fatalf("did not terminate at the requested boundary: %v", err)
}

func TestIncrementalMigrationRecoversAfterAbruptProcessExit(t *testing.T) {
	t.Parallel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, boundary := range []string{"staged", "owned", "unlink-0", "unlink-1", "unlink-2", "unlink-3", "directory-synced"} {
		t.Run(boundary, func(t *testing.T) {
			p, _, db, destination := incrementalPersisterFixture(t)
			want, err := whisperio.ReadSnapshot(context.Background(), "metric", filepath.Join(p.rootPath, "metric.wsp"))
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(executable, "-test.run=^TestIncrementalMigrationAbruptExitHelper$", "-test.count=1")
			cmd.Env = append(os.Environ(), "GO_CARBON_INCREMENTAL_CRASH_ORIGINAL="+p.rootPath,
				"GO_CARBON_INCREMENTAL_CRASH_DESTINATION="+destination, "GO_CARBON_INCREMENTAL_CRASH_BOUNDARY="+boundary)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 79 {
				t.Fatalf("unexpected child result: %v\n%s", err, output)
			}
			db, err = store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath, SyncInterval: time.Hour}))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			p.SetMetricStore(db)
			if err := p.SetIncrementalMigration(db, destination); err != nil {
				t.Fatal(err)
			}
			result, err := p.ConvertFile(context.Background(), "metric")
			if err != nil {
				t.Fatal(err)
			}
			if result.PreviouslyOwned != (boundary != "staged") {
				t.Fatalf("incorrect recovered owner after %s", boundary)
			}
			got, err := db.Snapshot(context.Background(), "metric")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal("crash lost stored history: ", err)
			}
			assertOriginalsReclaimed(t, p, "metric")
		})
	}
}

func TestIncrementalCleanupResumesWhenEmptyParentWasReaped(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	p.SetMetricStore(nil) // Existing source created before incremental activation.
	cache.add("nested.metric", time.Now().Unix()-1, 42)
	p.store("nested.metric")
	p.SetMetricStore(db)
	stop := errors.New("crash before directory sync")
	_, err := p.convertFile(context.Background(), "nested.metric", func(stage string) error {
		if stage == "unlink-3" {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(p.rootPath, "nested")); err != nil {
		t.Fatal(err)
	}
	result, err := p.ConvertFile(context.Background(), "nested.metric")
	if err != nil || !result.PreviouslyOwned {
		t.Fatalf("could not finish cleanup after parent removal: %+v %v", result, err)
	}
	metadata, err := db.Metadata(context.Background(), "nested.metric")
	if err != nil || metadata.FileMigration != store.FileReclaimed {
		t.Fatalf("cleanup not completed: %+v %v", metadata, err)
	}
}

func TestIncrementalUnlinkFailureKeepsSharedOwnerAndPendingCleanup(t *testing.T) {
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	path := filepath.Join(p.rootPath, "metric.wsp")
	want, err := whisperio.ReadSnapshot(ctx, "metric", path)
	if err != nil {
		t.Fatal(err)
	}
	sidecar := whisper.OutOfOrderSidecarPath(path)
	_, err = p.convertFile(ctx, "metric", func(stage string) error {
		if stage != "owned" {
			return nil
		}
		if err := os.Remove(sidecar); err != nil {
			return err
		}
		return os.Mkdir(sidecar, 0750) // Reclamation must refuse nonregular paths.
	})
	if err == nil {
		t.Fatal("reclaimed a nonregular path")
	}
	metadata, err := db.Metadata(ctx, "metric")
	if err != nil || metadata.FileMigration != store.FileCleanupPending {
		t.Fatalf("cleanup error reverted ownership: %+v %v", metadata, err)
	}
	got, err := db.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("cleanup error damaged shared history: ", err)
	}
	if err := os.Remove(sidecar); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConvertFile(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	assertOriginalsReclaimed(t, p, "metric")
}

// GO_CARBON_TEST_STORE_INGEST=1 runs every incremental conversion test through
// ingested file stages instead of memtable batches; GO_CARBON_TEST_TIME_BUCKETS=1
// through the time-partitioned (v7) block layout.
func withStageIngest(o store.Options) store.Options {
	o.IngestFileStages = os.Getenv("GO_CARBON_TEST_STORE_INGEST") == "1"
	o.TimeBuckets = os.Getenv("GO_CARBON_TEST_TIME_BUCKETS") == "1"
	return o
}

func TestIncrementalConversionSkipsMetricAlreadyConverting(t *testing.T) {
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	// A write hint and the cold scan can pick the same metric at once.
	p.converting.Store("metric", struct{}{})
	if _, err := p.ConvertFile(ctx, "metric"); !errors.Is(err, ErrConversionInProgress) {
		t.Fatalf("duplicate conversion: %v", err)
	}
	p.converting.Delete("metric")
	if _, err := db.Metadata(ctx, "metric"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("skipped conversion published an owner: %v", err)
	}
	if _, err := p.ConvertFile(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	assertOriginalsReclaimed(t, p, "metric")
}

// Once the migration is complete, writes skip the ownership route. A staged
// import whose original was removed before publication must not block a new
// metric of that name, and converted metrics keep receiving writes.
func TestIncrementalMigrationCompleteRoutesWritesToStore(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	stop := errors.New("simulated interrupted conversion")
	if _, err := p.convertFile(ctx, "metric", func(stage string) error {
		if stage == "staged" {
			return stop
		}
		return nil
	}); !errors.Is(err, stop) {
		t.Fatalf("staged boundary not reached: %v", err)
	}
	path, err := p.incrementalPath("metric")
	if err != nil {
		t.Fatal(err)
	}
	for _, original := range originalPaths(path) {
		if err := os.Remove(original); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	p.SetIncrementalMigrationComplete()
	now := time.Now().Unix()
	for i, name := range []string{"metric", "fresh"} {
		cache.add(name, now-1, float64(40+i))
		p.store(name)
		if _, err := db.Metadata(ctx, name); err != nil {
			t.Fatalf("%s: write after completion did not reach the store: %v", name, err)
		}
	}
	snapshot, err := db.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range snapshot.Archives[0].Points {
		if point.Timestamp < now-1 && point.Timestamp != 0 {
			t.Fatalf("stale staged history survived: %+v", point)
		}
	}
}

// A metric name the store refuses must not stay in the cache to be retried on
// every writeout: its points are dropped and counted, as when a Whisper file
// cannot be created.
func TestSharedWriteDropsNamesTooLong(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	p.SetIncrementalMigrationComplete()
	name := "general.flink." + strings.Repeat("x", 300) + ".count"
	now := time.Now().Unix()
	cache.add(name, now-2, 1)
	cache.add(name, now-1, 2)
	p.store(name)
	if _, ok := cache.data[name]; ok {
		t.Fatal("points of a refused name stayed cached")
	}
	if got := atomic.LoadUint32(&p.nameTooLong); got != 2 {
		t.Fatalf("dropped points counted %d, want 2", got)
	}
	if _, err := db.Metadata(context.Background(), name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("refused name reached the catalog: %v", err)
	}
	if n := cache.notConfirmedPoints(); n != 0 {
		t.Fatalf("%d dropped points left unconfirmed", n)
	}
}

type metadataCountingStore struct {
	*store.Store
	calls atomic.Int32
}

func (s *metadataCountingStore) Metadata(ctx context.Context, name string) (store.Metadata, error) {
	s.calls.Add(1)
	return s.Store.Metadata(ctx, name)
}

// Without a mirror, a new metric is created when the write reports it missing,
// after its points were popped; they land in the store and are confirmed. The
// write needs no separate catalog lookup.
func TestSharedWriteCreatesMissingMetricAfterPop(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	p.SetIncrementalMigrationComplete()
	counting := &metadataCountingStore{Store: db}
	p.SetMetricStore(counting)
	name := "new.metric.after.pop"
	now := time.Now().Unix()
	cache.add(name, now-2, 1)
	cache.add(name, now-1, 2)
	before := cache.confirmedPoints()
	p.store(name)
	if _, ok := cache.data[name]; ok {
		t.Fatal("points of a created metric stayed cached")
	}
	if got := cache.confirmedPoints() - before; got != 2 || cache.notConfirmedPoints() != 0 {
		t.Fatalf("confirmed %d (unconfirmed %d), want 2 (0)", got, cache.notConfirmedPoints())
	}
	cache.add(name, now, 3)
	// Existing metric policy belongs to the store. A later writeout must not
	// match either configuration again, even after the prior cache pop.
	p.schemas = WhisperSchemas{{}}
	p.aggregation = &WhisperAggregation{Data: []*WhisperAggregationItem{{}}}
	p.store(name)
	if n := counting.calls.Load(); n != 0 {
		t.Fatalf("writes made %d separate catalog lookups, want 0", n)
	}
	if _, err := db.Metadata(context.Background(), name); err != nil {
		t.Fatalf("missing metric was not created: %v", err)
	}
	values, err := db.Fetch(context.Background(), name, int(now-10), int(now))
	if err != nil {
		t.Fatal(err)
	}
	present := 0
	for _, v := range values.Values {
		if !math.IsNaN(v) {
			present++
		}
	}
	if present != 3 {
		t.Fatalf("store holds %d points, want 3", present)
	}
}

func TestIncrementalConversionDefersWritesThroughPublication(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	path := filepath.Join(p.rootPath, "metric.wsp")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if _, err := p.convertFile(ctx, "metric", func(stage string) error {
		switch stage {
		case "staged":
			cache.add("metric", now-2, 123.25)
		case "owned":
			cache.add("metric", now-1, 234.5)
		default:
			return nil
		}
		p.store("metric")
		after, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(before, after) {
			t.Fatal("persister changed original while conversion was fenced")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Deferred points route to the durable shared owner on the next writeout.
	p.store("metric")
	got, err := db.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, point := range got.Archives[0].Points {
		if point.Timestamp == now-2 && point.Value == 123.25 || point.Timestamp == now-1 && point.Value == 234.5 {
			found++
		}
	}
	if found != 2 {
		t.Fatal("write arriving during conversion was lost")
	}
}
