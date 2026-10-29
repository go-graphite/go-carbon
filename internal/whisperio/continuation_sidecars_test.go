package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func seedSidecar(t testing.TB, db *store.Store, name string, now int64) {
	seedSidecarHistory(t, db, name, now, 20)
}
func seedSidecarHistory(t testing.TB, db *store.Store, name string, now int64, history int) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: max(60, history*2)}, {Step: 5, Count: max(120, history)}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
		t.Fatal(err)
	}
	var seed []store.Point
	for age := int64(history); age > 0; age-- {
		seed = append(seed, store.Point{Timestamp: now - age, Value: float64(age) + .1})
	}
	for _, batch := range [][]store.Point{seed, {{Timestamp: now - 10, Value: 42.25}}} {
		if err := db.UpdateMany(ctx, name, batch); err != nil {
			t.Fatal(err)
		}
	}
}
func TestSidecarInventoryAndFoldAll(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			ctx := context.Background()
			now := int64(1792432631)
			codec := &ContinuationCodec{}
			options := store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: codec, Now: func() time.Time { return time.Unix(now, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
			dir := filepath.Join(t.TempDir(), "shared")
			db, err := store.Open(dir, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			for _, name := range []string{"a", "b", "c"} {
				seedSidecar(t, db, name, now)
			}
			inventory := db.Sidecars()
			if !inventory.Ready || inventory.Count != 3 || inventory.Points == 0 || inventory.Oldest == 0 {
				t.Fatalf("inventory: %+v", inventory)
			}
			if db.CheckNoSidecars() == nil {
				t.Fatal("conversion check accepted pending sidecars")
			}
			// Replacing a sidecar slot changes neither occupancy nor oldest timestamp.
			if err := db.UpdateMany(ctx, "a", []store.Point{{Timestamp: now - 10, Value: 50.5}}); err != nil {
				t.Fatal(err)
			}
			if got := db.Sidecars(); got != inventory {
				t.Fatalf("replacement changed occupancy: %+v -> %+v", inventory, got)
			}
			scan, err := db.ScanSidecars(ctx, "", 2, true)
			if err != nil || len(scan.Sidecars) != 2 || scan.Done {
				t.Fatalf("scan: %+v %v", scan, err)
			}
			scan, err = db.ScanSidecars(ctx, scan.After, 2, true)
			if err != nil || !scan.Done {
				t.Fatalf("resume scan: %+v %v", scan, err)
			}
			if got := db.Sidecars(); got != inventory {
				t.Fatalf("scan changed inventory: %+v", got)
			}
			before, err := db.Snapshot(ctx, "a")
			if err != nil {
				t.Fatal(err)
			}
			dry, err := db.FoldSidecars(ctx, "", 2, true, 0)
			if err != nil || dry.Count != 2 || dry.Folded != 0 {
				t.Fatalf("dry run: %+v %v", dry, err)
			}
			if db.Sidecars() != inventory {
				t.Fatal("dry run mutated inventory")
			}
			m, err := db.Metadata(ctx, "a")
			if err != nil || m.Revision != before.Metadata.Revision {
				t.Fatal("dry run changed revision", err)
			}
			first, err := db.FoldSidecars(ctx, "", 1, false, 0)
			if err != nil || first.Folded != 1 || first.Done {
				t.Fatalf("first fold: %+v %v", first, err)
			}
			// Reopen between pages to prove that both inventory and cursor are durable.
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(dir, options)
			if err != nil {
				t.Fatal(err)
			}
			if db.Sidecars().Count != 2 || !db.Sidecars().Ready {
				t.Fatalf("inventory after reopen: %+v", db.Sidecars())
			}
			final, err := db.FoldSidecars(ctx, first.After, 10, false, 0)
			if err != nil || !final.Done || final.Folded != 2 {
				t.Fatalf("resume fold: %+v %v", final, err)
			}
			if err := db.CheckNoSidecars(); err != nil {
				t.Fatal(err)
			}
			after, err := db.Snapshot(ctx, "a")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(before, after); err != nil {
				t.Fatal("fold changed merge values:", err)
			}
			// Deletes update count, points and the oldest heap under the same lock.
			seedSidecar(t, db, "delete", now)
			if err := db.Delete(ctx, "delete"); err != nil {
				t.Fatal(err)
			}
			if db.Sidecars().Count != 0 || db.Sidecars().Points != 0 || db.Sidecars().Oldest != 0 {
				t.Fatal("delete left inventory", db.Sidecars())
			}
		})
	}
}

type blockingSidecarCodec struct {
	*ContinuationCodec
	entered, release chan struct{}
	fail             bool
	failRemove       bool
	once             sync.Once
}

type failingSidecarRemovalIO struct{ store.ContinuationIO }

func (f failingSidecarRemovalIO) Delete(key []byte) error {
	if bytes.Equal(key, continuationNameKey("main.wsp.ooo")) {
		return errFoldInjected
	}
	return f.ContinuationIO.Delete(key)
}

var errFoldInjected = errors.New("injected fold failure")

func (c *blockingSidecarCodec) FoldSidecar(ctx context.Context, kv store.ContinuationIO, clock func() time.Time) (string, error) {
	if c.entered != nil {
		c.once.Do(func() { close(c.entered) })
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-c.release:
		}
	}
	if c.failRemove {
		kv = failingSidecarRemovalIO{kv}
	}
	kind, err := c.ContinuationCodec.FoldSidecar(ctx, kv, clock)
	if err == nil && c.fail {
		return kind, errFoldInjected
	}
	return kind, err
}
func TestSidecarFoldAtomicWithLiveWrites(t *testing.T) {
	ctx := context.Background()
	now := int64(1792432631)
	codec := &blockingSidecarCodec{ContinuationCodec: &ContinuationCodec{}}
	db, err := store.Open(filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: func() time.Time { return time.Unix(now, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedSidecar(t, db, "metric", now)
	inventory := db.Sidecars()
	codec.fail = true
	if _, err := db.FoldSidecars(ctx, "", 1, false, 0); !errors.Is(err, errFoldInjected) {
		t.Fatal("missing injected failure", err)
	}
	if db.Sidecars() != inventory {
		t.Fatal("failed batch changed inventory")
	}
	codec.fail = false
	codec.failRemove = true
	if _, err := db.FoldSidecars(ctx, "", 1, false, 0); err == nil {
		t.Fatal("fold accepted a failed sidecar deletion")
	}
	if db.Sidecars() != inventory {
		t.Fatal("failed sidecar deletion changed inventory")
	}
	codec.failRemove = false
	codec.entered, codec.release = make(chan struct{}), make(chan struct{})
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() { _, err := db.FoldSidecars(cancelledCtx, "", 1, false, 0); cancelled <- err }()
	<-codec.entered
	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled fold committed", err)
	}
	if db.Sidecars() != inventory {
		t.Fatal("cancelled fold changed inventory")
	}
	codec.once = sync.Once{}
	codec.DirectLateWrites = true
	codec.entered, codec.release = make(chan struct{}), make(chan struct{})
	folded := make(chan error, 1)
	go func() { _, err := db.FoldSidecars(ctx, "", 1, false, 0); folded <- err }()
	<-codec.entered
	written := make(chan error, 1)
	go func() { written <- db.UpdateMany(ctx, "metric", []store.Point{{Timestamp: now - 10, Value: 99.5}}) }()
	close(codec.release)
	if err := <-folded; err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if err := db.CheckNoSidecars(); err != nil {
		t.Fatal(err)
	}
	got, err := db.Fetch(ctx, "metric", int(now-11), int(now-9))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i, v := range got.Values {
		if got.FromTime+i*got.Step == int(now-10) {
			found = true
			if v != 99.5 {
				t.Fatalf("new live value lost: %v", v)
			}
		}
	}
	if !found {
		t.Fatal("corrected interval absent")
	}
}

func TestConversionFoldsAggregationMethods(t *testing.T) {
	for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.First, store.Last, store.Min, store.Max} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			sc := oracleScenario{rets: []store.Retention{{Step: 1, Count: 120}, {Step: 5, Count: 120}}, method: method, xff: .5}
			src := newSidecarSource(t, t.TempDir(), sc, 16)
			now := src.clock.now
			var seed []store.Point
			for i := 60; i > 0; i-- {
				seed = append(seed, store.Point{Timestamp: int64(now - i), Value: float64(i) + .1})
			}
			src.update(now, seed, false)
			src.update(now+1, []store.Point{{Timestamp: int64(now - 30), Value: 99.25}}, false)
			if !src.hasSidecar() {
				t.Fatal("fixture has no sidecar")
			}
			// Conversion must fold even with direct late writes disabled.
			db := openImportFoldStore(t, filepath.Dir(src.path), &src.clock, true, &ContinuationCodec{})
			convertSource(t, db, "converted", src.path)
			if err := db.CheckNoSidecars(); err != nil {
				t.Fatal(err)
			}
			page, err := db.ScanSidecars(context.Background(), "", 10, true)
			if err != nil || len(page.Sidecars) != 0 {
				t.Fatalf("sidecars: %+v %v", page, err)
			}
		})
	}
}

func BenchmarkStoreOOOFoldAll(b *testing.B) {
	for _, history := range []int{20, 2000} {
		for _, rewrite := range []bool{false, true} {
			b.Run(fmt.Sprintf("history=%d/rewrite=%v", history, rewrite), func(b *testing.B) {
				const metrics = 64
				ctx := context.Background()
				now := int64(1792432631)
				db, err := store.Open(filepath.Join(b.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &ContinuationCodec{}, Now: func() time.Time { return time.Unix(now, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				var cpu int64
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					b.StopTimer()
					for i := 0; i < metrics; i++ {
						name := fmt.Sprintf("metric.%03d", i)
						if iteration > 0 {
							if err := db.Delete(ctx, name); err != nil {
								b.Fatal(err)
							}
						}
						seedSidecarHistory(b, db, name, now, history)
					}
					previous := inPlaceRollupPatches
					inPlaceRollupPatches = !rewrite
					var before, after syscall.Rusage
					if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					page, err := db.FoldSidecars(ctx, "", metrics, false, 0)
					b.StopTimer()
					if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
						b.Fatal(err)
					}
					inPlaceRollupPatches = previous
					cpu += after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
					if err != nil || page.Folded != metrics || page.Inventory.Count != 0 {
						b.Fatalf("pass: %+v %v", page, err)
					}
					for kind, n := range page.Results {
						b.ReportMetric(float64(n), kind+"/pass")
					}
				}
				b.ReportMetric(float64(cpu)/float64(b.N*metrics), "cpu-ns/sidecar")
				b.ReportMetric(metrics, "sidecars/pass")
			})
		}
	}
}

func TestConversionFoldsIngestedSidecar(t *testing.T) {
	ctx := context.Background()
	sc := oracleScenario{rets: []store.Retention{{Step: 1, Count: 120}, {Step: 5, Count: 120}}, method: store.Average, xff: .5}
	src := newSidecarSource(t, t.TempDir(), sc, 16)
	now := src.clock.now
	var seed []store.Point
	for age := 60; age > 0; age-- {
		seed = append(seed, store.Point{Timestamp: int64(now - age), Value: float64(age) + .1})
	}
	src.update(now, seed, false)
	src.update(now+1, []store.Point{{Timestamp: int64(now - 30), Value: 99.25}}, false)
	if !src.hasSidecar() {
		t.Fatal("fixture has no sidecar")
	}
	db, err := store.Open(filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &ContinuationCodec{DirectLateWrites: true}, IncrementalSource: filepath.Dir(src.path), IngestFileStages: true, IngestGroupBytes: 1, IngestLinger: time.Millisecond, Now: src.clock.time, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	convertSource(t, db, "converted", src.path)
	if db.Stats().IngestEntries != 1 {
		t.Fatal("conversion did not use SST ingestion")
	}
	if err := db.CheckNoSidecars(); err != nil {
		t.Fatal(err)
	}
	page, err := db.ScanSidecars(ctx, "", 10, true)
	if err != nil || len(page.Sidecars) != 0 {
		t.Fatalf("ingestion left sidecars: %+v %v", page, err)
	}
}

func TestConversionRejectsMixSidecar(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "mixed.wsp")
	retentions := whisper.MustParseRetentionDefs("1s:2m,5s:10m")
	w, err := whisper.CreateWithOptions(path, retentions, whisper.Mix, 0, &whisper.Options{Compressed: true, MixAggregationSpecs: []whisper.MixAggregationSpec{{Method: whisper.Average}, {Method: whisper.Sum}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	side, err := whisper.Create(whisper.OutOfOrderSidecarPath(path), retentions, whisper.Average, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := side.Close(); err != nil {
		t.Fatal(err)
	}
	// Mix is outside the source sidecar and shared-store policy contracts. A
	// conversion must retain the original pair instead of importing an unusable
	// sidecar or guessing which aggregation should own its coarse archives.
	if _, err := ReadSnapshotIn(context.Background(), "mixed", path, t.TempDir()); err == nil {
		t.Fatal("accepted unsupported Mix sidecar")
	}
	for _, p := range []string{path, whisper.OutOfOrderSidecarPath(path)} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal("failed conversion removed source", err)
		}
	}
}

func TestRestagingClearsLegacySidecarInventory(t *testing.T) {
	ctx := context.Background()
	sc := oracleScenario{rets: []store.Retention{{Step: 1, Count: 120}, {Step: 5, Count: 120}}, method: store.Average}
	src := newSidecarSource(t, t.TempDir(), sc, 16)
	now := src.clock.now
	var seed []store.Point
	for age := 60; age > 0; age-- {
		seed = append(seed, store.Point{Timestamp: int64(now - age), Value: float64(age) + .1})
	}
	src.update(now, seed, false)
	src.update(now+1, []store.Point{{Timestamp: int64(now - 30), Value: 99.25}}, false)
	if !src.hasSidecar() {
		t.Fatal("fixture has no sidecar")
	}
	codec := &ContinuationCodec{keepImportedSidecars: true}
	db := openImportFoldStore(t, filepath.Dir(src.path), &src.clock, true, codec)
	stage := func() (store.Snapshot, store.Metadata) {
		t.Helper()
		capture, err := ReadSnapshotIn(ctx, "metric", src.path, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		m, err := db.StageFileSnapshot(ctx, capture)
		if err != nil {
			t.Fatal(err)
		}
		return capture, m
	}
	stage()
	if db.Sidecars().Count != 1 {
		t.Fatal("legacy stage not inventoried")
	}
	// A retry can replace an old unowned stage after its source was compacted.
	src.update(now+1, nil, true)
	codec.keepImportedSidecars = false
	capture, m := stage()
	if _, err := db.PublishFileOwnership(ctx, capture, m); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckNoSidecars(); err != nil {
		t.Fatal(err)
	}
}
