package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func checkpointOptions() Options {
	return Options{MetadataCacheSize: 2 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(20, 0) }}
}
func seedCheckpoint(t *testing.T, dir string) {
	t.Helper()
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c"} {
		if _, err := db.Create(context.Background(), MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Average}); err != nil && !errors.Is(err, ErrExists) {
			t.Fatal(err)
		}
		if _, err := db.Metadata(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.MetadataCheckpointError(); err != nil {
		t.Fatal(err)
	}
}

// The old format makes no claim that saved records were authoritative.
func legacyCheckpoint(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, checkpointFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copy(data[:8], checkpointLegacyMagic)
	binary.LittleEndian.PutUint32(data[48:52], crc32.ChecksumIEEE(data[:48]))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataCheckpointValidation(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	legacyCheckpoint(t, dir)
	opts := checkpointOptions()
	opts.MetadataCacheCheckpointLazy = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err != nil || !stats.Valid || stats.Entries != 3 {
		t.Fatal(stats, err)
	}
	// Bypass the store mutation path to exercise lazy validation independently
	// of the normal writer's mandatory cache invalidation.
	m, err := metadataFrom(db.db, "a")
	if err != nil {
		t.Fatal(err)
	}
	m.Revision++
	if err := db.db.Set(revisionKey(m), revisionBytes(m), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	got, err := db.Metadata(context.Background(), "a")
	if err != nil || got.Revision != m.Revision {
		t.Fatal("stale checkpoint used", got, err)
	}
	validated, err := db.ValidateMetadataCheckpoint(context.Background(), func(yield func(string) bool) { yield("b") }, PrewarmOptions{})
	if err != nil || validated.Validated != 1 {
		t.Fatal(validated, err)
	}
	if _, err := db.Metadata(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	sh := &db.stateCache.shards[db.metricStripe("b")]
	sh.mu.Lock()
	_, slot := sh.find(db.stateCache.hash("b"), "b")
	provisional := sh.slots[slot].epoch&provisionalState != 0
	sh.mu.Unlock()
	if provisional {
		t.Fatal("validated state not promoted")
	}
}

func TestMetadataCheckpointCrashDivergenceAndCorruption(t *testing.T) {
	for _, mode := range []string{"crash", "divergence", "foreign", "truncated", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			seedCheckpoint(t, dir)
			if mode == "crash" {
				db, err := Open(dir, checkpointOptions())
				if err != nil {
					t.Fatal(err)
				}
				db.checkpoint.enabled = false // emulate an open followed by no clean checkpoint
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "divergence" {
				opts := checkpointOptions()
				opts.MetadataCacheCheckpoint = false
				db, err := Open(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.Create(context.Background(), MetricConfig{Name: "new", Retentions: []Retention{{Step: 1, Count: 10}}, AggregationMethod: Average})
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "foreign" {
				other := t.TempDir()
				seedCheckpoint(t, other)
				data, _ := os.ReadFile(filepath.Join(other, checkpointFile))
				if err := os.WriteFile(filepath.Join(dir, checkpointFile), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "truncated" || mode == "corrupt" {
				path := filepath.Join(dir, checkpointFile)
				data, _ := os.ReadFile(path)
				if mode == "truncated" {
					data = data[:len(data)-9]
				} else {
					data[65] ^= 1
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			db, err := Open(dir, checkpointOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
			if stats.Valid {
				t.Fatal("unusable checkpoint accepted", mode, stats, err)
			}
			if _, err := db.Metadata(context.Background(), "a"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMetadataCheckpointBudgetCancelConcurrent(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	opts := checkpointOptions()
	opts.MetadataCacheSize = 1
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err != nil || !stats.Valid || stats.Entries != 0 || db.stateCache.bytes.Load() > db.stateCache.budget() {
		t.Fatal(stats, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	seedCheckpoint(t, dir)
	db, err = Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := db.LoadMetadataCheckpoint(context.Background(), func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() })
		done <- err
	}()
	<-entered
	db.StopBackground()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A live load and ordinary readers/writers share the existing epochs.
	seedCheckpoint(t, dir)
	db, err = Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
			t.Error(err)
		}
	})
	for _, name := range []string{"a", "b", "c"} {
		wg.Go(func() {
			for i := range 10 {
				if err := db.UpdateMany(context.Background(), name, []Point{{Timestamp: int64(i + 1), Value: float64(i)}}); err != nil {
					t.Error(err)
				}
				if _, err := db.Metadata(context.Background(), name); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	for _, name := range []string{"a", "b", "c"} {
		snap, err := db.Snapshot(context.Background(), name)
		if err != nil || len(snap.Archives[0].Points) != 10 {
			t.Fatal(name, snap, err)
		}
	}
}

func TestMetadataCheckpointPreparedAndChanged(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			dir := t.TempDir()
			seedCheckpoint(t, dir)
			db, err := Open(dir, checkpointOptions())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Metadata(context.Background(), "a"); err != nil {
				t.Fatal(err)
			}
			if err := db.PrepareMetadataCheckpoint(context.Background()); err != nil {
				t.Fatal(err)
			}
			if changed {
				if err := db.UpdateMany(context.Background(), "a", []Point{{Timestamp: 20, Value: 3}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.CloseForExit(); err != nil {
				t.Fatal(err)
			}
			if (db.MetadataCheckpointError() != nil) != changed {
				t.Fatal(db.MetadataCheckpointError())
			}
			db, err = Open(dir, checkpointOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
			if err != nil || stats.Valid == changed {
				t.Fatal(stats, err)
			}
		})
	}
}

func TestMetadataCheckpointPrepareCancellation(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Metadata(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.PrepareMetadataCheckpoint(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := db.CloseForExit(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err != nil || stats.Valid {
		t.Fatal(stats, err)
	}
}

func TestMetadataCheckpointDeleteRecreate(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if stats, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil || !stats.Valid {
		t.Fatal(stats, err)
	}
	old, err := metadataFrom(db.db, "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	next, err := db.Create(context.Background(), MetricConfig{Name: "a", Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Average})
	if err != nil || next.ID == old.ID {
		t.Fatal(next, err)
	}
	got, err := db.Metadata(context.Background(), "a")
	if err != nil || got.ID != next.ID {
		t.Fatal("old checkpoint binding survived recreate", got, err)
	}
	if err := db.UpdateMany(context.Background(), "a", []Point{{Timestamp: 20, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot(context.Background(), "a")
	if err != nil || len(snap.Archives[0].Points) != 1 || snap.Archives[0].Points[0].Value != 42 {
		t.Fatal(snap, err)
	}
}

func TestMetadataCheckpointPrepareWithReads(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var names []string
	for i := range 48 {
		name := fmt.Sprint("read.", i)
		names = append(names, name)
		if _, err := db.Create(context.Background(), MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
	}
	db.ForgetMetadataCacheForTest()
	var wg sync.WaitGroup
	wg.Go(func() {
		for _, name := range names {
			if _, err := db.Metadata(context.Background(), name); err != nil {
				t.Error(err)
			}
		}
	})
	if err := db.PrepareMetadataCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestMetadataCheckpointProgress(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	progress := new(PrewarmProgress)
	stats, err := db.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{Progress: progress})
	done, total := progress.Snapshot()
	if err != nil || !stats.Valid || stats.Validated+stats.Fenced != stats.Entries || done == 0 || done != total {
		t.Fatal(stats, done, total, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.LoadMetadataCheckpointAfter(ctx, PrewarmOptions{Progress: progress}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if done, total := progress.Snapshot(); done != 0 || total != 0 {
		t.Fatal("failed checkpoint progress leaked into catalog projection", done, total)
	}
}

func TestMetadataCheckpointRate(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	started := time.Now()
	stats, err := db.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{Rate: 100})
	if err != nil || !stats.Valid || time.Since(started) < 30*time.Millisecond {
		t.Fatal("checkpoint bypassed configured rate", stats, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := db.LoadMetadataCheckpointAfter(ctx, PrewarmOptions{ByteRate: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("byte pacer did not cancel", err)
	}
}

// Relayout uses the mutation lock without incrementing the metric revision.
// Such a mutation must stop a later load from reinstalling the old record.
func TestMetadataCheckpointRevisionNeutralMutation(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	unlock := db.lockMetricMutation("a")
	m, err := metadataFrom(db.db, "a")
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	revision := m.Revision
	m.AggregationMethod = Sum
	value, err := db.encodeCatalog(m)
	if err == nil {
		err = db.db.Set(catalogKey("a"), value, pebble.Sync)
	}
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err != nil || !stats.Valid {
		t.Fatal(stats, err)
	}
	if db.stateCache.has(db.metricStripe("a"), "a") {
		t.Fatal("checkpoint reintroduced a mutated name")
	}
	got, err := db.Metadata(context.Background(), "a")
	if err != nil || got.Revision != revision || got.AggregationMethod != Sum {
		t.Fatal("revision-neutral mutation was lost", got, err)
	}
}

func TestMetadataCheckpointParallelMatchesLazy(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(fmt.Sprint(lazy), func(t *testing.T) {
			dir := t.TempDir()
			opts := checkpointOptions()
			db, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 257 {
				name := fmt.Sprintf("metric.%04d", i)
				if _, err := db.Create(context.Background(), MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Average}); err != nil {
					t.Fatal(err)
				}
				if err := db.UpdateMany(context.Background(), name, []Point{{Timestamp: 19, Value: float64(i)}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			legacyCheckpoint(t, dir)
			opts.MetadataCacheCheckpointLazy = lazy
			db, err = Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			stats, err := db.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{Workers: 4, Batch: 32})
			if err != nil || !stats.Valid || stats.Entries != 257 {
				t.Fatal(stats, err)
			}
			if !lazy && stats.Validated != 257 || lazy && stats.Validated != 0 {
				t.Fatal(stats)
			}
			for i := range 257 {
				snapshot, err := db.Snapshot(context.Background(), fmt.Sprintf("metric.%04d", i))
				if err != nil || len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != float64(i) {
					t.Fatal(i, snapshot, err)
				}
			}
		})
	}
}

// Trusted checkpoints must not turn the first write into a revision lookup.
func TestMetadataCheckpointFenceNeedsNoReads(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reads := CountStoreReadsForTest()
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err != nil || !stats.Valid || stats.Fenced != 3 || stats.Validated != 0 {
		t.Fatal(stats, err)
	}
	for _, name := range []string{"a", "b", "c"} {
		if _, err := db.Metadata(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	if n := reads(); n != 0 {
		t.Fatal("fenced checkpoint performed revision I/O", n)
	}
}

func TestMetadataCheckpointCannotCertifyProvisionalOrPending(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			dir := t.TempDir()
			seedCheckpoint(t, dir)
			legacyCheckpoint(t, dir)
			opts := checkpointOptions()
			opts.MetadataCacheCheckpointLazy = true
			db, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if stats, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil || stats.Entries != 3 || stats.Validated != 0 || stats.Fenced != 0 {
				t.Fatal(stats, err)
			}
			validated, err := db.ValidateMetadataCheckpoint(context.Background(), func(yield func(string) bool) { yield("b") }, PrewarmOptions{})
			if err != nil || validated.Validated != 1 {
				t.Fatal(validated, err)
			}
			var unlock func()
			if pending {
				unlock = db.lockMetricMutation("b")
			}
			if err := db.PrepareMetadataCheckpoint(context.Background()); err != nil {
				t.Fatal(err)
			}
			if unlock != nil {
				unlock()
			}
			if err := db.Close(); err != nil || db.MetadataCheckpointError() != nil {
				t.Fatal(err, db.MetadataCheckpointError())
			}
			db, err = Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
			want := int64(1)
			if pending {
				want = 0
			}
			if err != nil || !stats.Valid || stats.Entries != want || stats.Fenced != want {
				t.Fatal("unproven record certified", stats, err)
			}
		})
	}
}

func TestMetadataCheckpointValidateNextAdmissionAndStop(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	legacyCheckpoint(t, dir)
	opts := checkpointOptions()
	opts.MetadataCacheCheckpointLazy = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	names := func(yield func(string) bool) { yield("b"); yield("a") }
	var calls atomic.Int64
	stats, err := db.ValidateMetadataCheckpoint(context.Background(), names, PrewarmOptions{Batch: 1, Wait: func(ctx context.Context) error {
		calls.Add(1)
		// No cache lock or database view may be held across admission.
		unlock := db.lockMetric("c")
		db.stateCache.has(db.metricStripe("c"), "c")
		unlock()
		return ctx.Err()
	}})
	if err != nil || stats.Validated != 2 || calls.Load() != 2 {
		t.Fatal(stats, calls.Load(), err)
	}
	// A repeated offer of authoritative names does not ask for admission.
	if _, err := db.ValidateMetadataCheckpoint(context.Background(), names, PrewarmOptions{Wait: func(context.Context) error { t.Fatal("unnecessary admission"); return nil }}); err != nil {
		t.Fatal(err)
	}
	// Reinstall one provisional record, then block admission until shutdown.
	if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := db.ValidateMetadataCheckpoint(context.Background(), nil, PrewarmOptions{Wait: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("validator did not enter admission")
	}
	db.StopBackground()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestMetadataCheckpointValidateNextConcurrent(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	legacyCheckpoint(t, dir)
	opts := checkpointOptions()
	opts.MetadataCacheCheckpointLazy = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for range 10 {
				if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
					t.Error(err)
				}
				if _, err := db.ValidateMetadataCheckpoint(context.Background(), nil, PrewarmOptions{Batch: 2}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	for _, name := range []string{"a", "b", "c"} {
		wg.Go(func() {
			for i := range 10 {
				if err := db.UpdateMany(context.Background(), name, []Point{{Timestamp: int64(i + 1), Value: float64(i)}}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	for _, name := range []string{"a", "b", "c"} {
		snap, err := db.Snapshot(context.Background(), name)
		if err != nil || len(snap.Archives[0].Points) != 10 {
			t.Fatal(name, snap, err)
		}
	}
}

func TestMetadataCheckpointReplacedAfterOpen(t *testing.T) {
	dir, other := t.TempDir(), t.TempDir()
	seedCheckpoint(t, dir)
	seedCheckpoint(t, other)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	data, err := os.ReadFile(filepath.Join(other, checkpointFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checkpointFile), data, 0600); err != nil {
		t.Fatal(err)
	}
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err == nil || stats.Valid || stats.Entries != 0 {
		t.Fatal("replaced file trusted", stats, err)
	}
}

func TestMetadataCheckpointStaleRevisionBreaksFence(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	opts := checkpointOptions()
	opts.MetadataCacheCheckpoint = false
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	m, err := metadataFrom(db.db, "a")
	if err != nil {
		t.Fatal(err)
	}
	m.Revision++
	if err := db.db.Set(revisionKey(m), revisionBytes(m), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err != nil || stats.Valid || stats.Entries != 0 {
		t.Fatal("stale revision accepted by fence", stats, err)
	}
	got, err := db.Metadata(context.Background(), "a")
	if err != nil || got.Revision != m.Revision {
		t.Fatal(got, err)
	}
}

func TestMetadataCheckpointValidateNextMutationDuringAdmission(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	legacyCheckpoint(t, dir)
	opts := checkpointOptions()
	opts.MetadataCacheCheckpointLazy = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	progress := new(PrewarmProgress)
	stats, err := db.ValidateMetadataCheckpoint(context.Background(), func(yield func(string) bool) { yield("a") }, PrewarmOptions{Progress: progress, Wait: func(context.Context) error {
		if !progress.CheckpointLoading() {
			t.Fatal("validation omitted from loading progress")
		}
		unlock := db.lockMetricMutation("a")
		defer unlock()
		m, err := metadataFrom(db.db, "a")
		if err != nil {
			return err
		}
		m.AggregationMethod = Sum // state change without a revision increment
		value, err := db.encodeCatalog(m)
		if err != nil {
			return err
		}
		return db.db.Set(catalogKey("a"), value, pebble.Sync)
	}})
	if err != nil || stats.Validated != 0 || progress.CheckpointLoading() {
		t.Fatal(stats, err)
	}
	m, err := db.Metadata(context.Background(), "a")
	if err != nil || m.AggregationMethod != Sum {
		t.Fatal("mutated state replaced with checkpoint", m, err)
	}
}

// Preparation can refresh the saved clean stamp while a load waits for input.
// The loader must keep using its immutable opening fence and reject that file.
func TestMetadataCheckpointPrepareWhileLoadWaits(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() {
		stats, err := db.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{StartAfter: ready})
		if stats.Valid || stats.Entries != 0 {
			t.Error("load accepted a later clean stamp", stats)
		}
		done <- err
	}()
	if err := db.PrepareMetadataCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(ready)
	if err := <-done; err == nil {
		t.Fatal("refreshed fence accepted")
	}
}

func TestMetadataCheckpointConcurrentProgress(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	legacyCheckpoint(t, dir)
	opts := checkpointOptions()
	opts.MetadataCacheCheckpointLazy = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	progress := new(PrewarmProgress)
	loadEntered, validateEntered := make(chan struct{}), make(chan struct{})
	loadRelease, validateRelease := make(chan struct{}), make(chan struct{})
	wait := func(entered, release chan struct{}) func(context.Context) error {
		return func(ctx context.Context) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	loadDone, validateDone := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := db.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{Progress: progress, Wait: wait(loadEntered, loadRelease)})
		loadDone <- err
	}()
	go func() {
		_, err := db.ValidateMetadataCheckpoint(context.Background(), nil, PrewarmOptions{Progress: progress, Wait: wait(validateEntered, validateRelease)})
		validateDone <- err
	}()
	for _, entered := range []chan struct{}{loadEntered, validateEntered} {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("checkpoint work did not start")
		}
	}
	close(loadRelease)
	if err := <-loadDone; err != nil {
		t.Fatal(err)
	}
	if !progress.CheckpointLoading() {
		t.Fatal("loader hid still-running validation")
	}
	close(validateRelease)
	if err := <-validateDone; err != nil {
		t.Fatal(err)
	}
	if progress.CheckpointLoading() {
		t.Fatal("checkpoint work remained active")
	}
}

// Warm-first writeout offers a cold window to ValidateMetadataCheckpoint and
// then prefetches it. Legacy provisional records count as cold until promoted;
// first use then needs no revision lookup, and stale records are refused.
func TestWriteoutWindowValidatesLegacyCheckpoint(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprint(stale), func(t *testing.T) {
			dir := t.TempDir()
			seedCheckpoint(t, dir)
			legacyCheckpoint(t, dir)
			opts := checkpointOptions()
			opts.MetadataCacheCheckpointLazy = true
			db, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if stats, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil || !stats.Valid || stats.Validated != 0 || stats.Fenced != 0 {
				t.Fatal(stats, err)
			}
			if db.MetadataCached("a") {
				t.Fatal("provisional record was scheduled as warm")
			}
			var authoritative Metadata
			if stale {
				// Bypass mutation invalidation to test revision validation alone.
				if authoritative, err = metadataFrom(db.db, "a"); err != nil {
					t.Fatal(err)
				}
				authoritative.Revision++
				if err := db.db.Set(revisionKey(authoritative), revisionBytes(authoritative), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			window := []string{"a", "b", "c"}
			stats, err := db.ValidateMetadataCheckpoint(context.Background(), slices.Values(window), PrewarmOptions{Batch: DefaultPrewarmBatch})
			want := int64(3)
			if stale {
				want = 2
			}
			if err != nil || stats.Validated != want {
				t.Fatal(stats, err)
			}
			db.PrewarmWriteout(context.Background(), window)
			stop := CountStoreReadsForTest()
			for _, name := range window {
				if !db.MetadataCached(name) {
					t.Fatal("window was not warmed", name)
				}
			}
			if reads := stop(); reads != 0 {
				t.Fatal("scheduling hint performed reads", reads)
			}
			if !stale {
				stop = CountStoreReadsForTest()
				for _, name := range window {
					if _, err := db.Metadata(context.Background(), name); err != nil {
						t.Fatal(err)
					}
				}
				if reads := stop(); reads != 0 {
					t.Fatal("first use repeated checkpoint validation", reads)
				}
				return
			}
			got, err := db.Metadata(context.Background(), "a")
			if err != nil || got.Revision != authoritative.Revision {
				t.Fatal("stale checkpoint was promoted", got, err)
			}
		})
	}
}
