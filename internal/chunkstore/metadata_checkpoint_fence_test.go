package chunkstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// A stop saves the checkpoint while reads stay up and stop-time writers still
// commit. The save used to fail with "store changed during/after metadata
// checkpoint", so the successor started with a cold metadata cache. It must
// now save, and the successor must never trust a record mutated after the
// save fence.
func TestMetadataCheckpointSavesWhileStoreChanges(t *testing.T) {
	dir := t.TempDir()
	names := make([]string, 300)
	for i := range names {
		names[i] = fmt.Sprintf("fence.metric.%03d", i)
	}
	during, concurrent, after := names[:20], names[20:40], names[40:50]
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, name := range names {
		if _, err := db.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, name, []Point{{Timestamp: 1, Value: 1}}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Metadata(ctx, name); err != nil {
			t.Fatal(err)
		}
	}

	// A writer that keeps committing for the whole save, like reads and
	// stop-time tails on a stopping host.
	stop := make(chan struct{})
	var writes atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for ts := int64(2); ; ts++ {
			for _, name := range concurrent {
				select {
				case <-stop:
					return
				default:
				}
				if err := db.UpdateMany(ctx, name, []Point{{Timestamp: 2 + ts%15, Value: float64(ts)}}); err != nil {
					t.Error(err)
					return
				}
				writes.Add(1)
			}
		}
	})
	// Deterministic writes inside the save, between its fence and its end.
	var once sync.Once
	options := PrewarmOptions{Workers: 2, Wait: func(context.Context) error {
		once.Do(func() {
			for _, name := range during {
				if err := db.UpdateMany(ctx, name, []Point{{Timestamp: 18, Value: 18}}); err != nil {
					t.Error(err)
				}
			}
		})
		return nil
	}}
	prepareErr := db.PrepareMetadataCheckpointWithOptions(ctx, options)
	close(stop)
	wg.Wait()
	if prepareErr != nil {
		t.Fatal("prepare failed with writes during the save:", prepareErr)
	}
	// Writes after the save and before close, like close-time bookkeeping.
	for _, name := range after {
		if err := db.UpdateMany(ctx, name, []Point{{Timestamp: 19, Value: 19}}); err != nil {
			t.Fatal(err)
		}
	}
	want := make(map[string]Metadata, len(names))
	for _, name := range names {
		m, err := db.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		want[name] = m
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.MetadataCheckpointError(); err != nil {
		t.Fatal("save failed with writes after the save:", err)
	}

	db, err = Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := db.LoadMetadataCheckpoint(ctx, nil)
	if err != nil || !stats.Valid {
		t.Fatal("successor did not load the checkpoint:", stats, err)
	}
	// Every mutated stripe is skipped; the rest load as authoritative.
	dirty := map[uint64]bool{}
	for _, name := range append(append(append([]string{}, during...), concurrent...), after...) {
		dirty[db.metricStripe(name)] = true
	}
	skippable := 0
	for _, name := range names {
		if dirty[db.metricStripe(name)] {
			skippable++
		}
	}
	if stats.SkippedDirty < int64(len(during)+len(after)) || stats.SkippedDirty > int64(skippable) {
		t.Fatalf("skipped %d records, want between %d and %d (stats %+v)", stats.SkippedDirty, len(during)+len(after), skippable, stats)
	}
	if stats.Entries < int64(len(names)-skippable) || stats.Entries != stats.Fenced {
		t.Fatalf("loaded %d records, want at least %d authoritative (stats %+v)", stats.Entries, len(names)-skippable, stats)
	}
	for _, name := range names {
		got, err := db.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		w := want[name]
		if got.ID != w.ID || got.Generation != w.Generation || got.Revision != w.Revision || !got.LastUpdate.Equal(w.LastUpdate) {
			t.Fatalf("%s: successor metadata %+v, want %+v", name, got, w)
		}
	}
	// The successor keeps writing on top of the restored state.
	for _, name := range append(append([]string{}, during...), names[200]) {
		if err := db.UpdateMany(ctx, name, []Point{{Timestamp: 20, Value: 20}}); err != nil {
			t.Fatal(err)
		}
		snap, err := db.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		points := snap.Archives[0].Points
		if len(points) == 0 || points[len(points)-1].Timestamp != 20 || points[0].Timestamp != 1 {
			t.Fatalf("%s: successor points %v", name, points)
		}
	}
	t.Logf("concurrent writes during the save: %d; loaded %d, skipped %d", writes.Load()+int64(len(during)), stats.Entries, stats.SkippedDirty)

	// A crash (no save) consumes the clean stamp: the old fence is useless.
	if err := db.CloseForExit(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, err = db.LoadMetadataCheckpoint(ctx, nil)
	if err != nil || stats.Valid {
		t.Fatal("stale fence accepted after an unsaved exit:", stats, err)
	}
}

// A fence names the base sequence of the very file it completes.
func TestMetadataCheckpointFenceRejectsOtherSave(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := db.Metadata(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := db.PrepareMetadataCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(ctx, "a", []Point{{Timestamp: 20, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	// Corrupt the base: the fence then completes no saved file.
	db.checkpoint.sequence++
	if err := db.Close(); err != nil || db.MetadataCheckpointError() != nil {
		t.Fatal(err, db.MetadataCheckpointError())
	}
	db, err = Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, err := db.LoadMetadataCheckpoint(ctx, nil)
	if err != nil || stats.Valid {
		t.Fatal(stats, err)
	}
	if _, err := db.Metadata(ctx, "a"); err != nil {
		t.Fatal(err)
	}
}
