package chunkstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

type batchSidecarCodec struct{ batchTestCodec }

func (batchSidecarCodec) Update(_ context.Context, io ContinuationIO, _ Metadata, input []Point, _ int, _ func() time.Time, _ *ContinuationMaintenance) error {
	for _, point := range input {
		if point.Value < 0 {
			return errors.New("injected prepare failure")
		}
		if point.Value == 0 {
			continue
		}
		summary := SidecarSummary{}
		if point.Timestamp >= 0 {
			summary = SidecarSummary{Present: true, Points: uint64(point.Value), Oldest: point.Timestamp}
		}
		if err := io.Set(SidecarSummaryKey, summary.Encode()); err != nil {
			return err
		}
	}
	return nil
}

func TestUpdateBatchSidecarInventory(t *testing.T) {
	ctx := context.Background()
	options := Options{fs: vfs.NewMem(), ExactBlocks: true, Continuation: batchSidecarCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20, MetadataCacheSize: 1 << 20}
	db, err := Open("store", options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	for _, name := range []string{"a", "b"} {
		createTestMetric(t, db, name)
	}
	update := func(name string, points float64) MetricUpdate {
		return MetricUpdate{Name: name, Points: []Point{{Timestamp: 100, Value: points}}}
	}
	if err := db.UpdateBatch(ctx, []MetricUpdate{update("a", 2), update("b", 3), update("a", 4), update("a", 0)}, nil); err != nil {
		t.Fatal(err)
	}
	want := db.Sidecars()
	if !want.Ready || want.Count != 2 || want.Points != 7 || want.Oldest != 100 {
		t.Fatal(want)
	}
	if err := db.UpdateBatch(ctx, []MetricUpdate{update("a", 9), update("b", -1)}, nil); err == nil {
		t.Fatal("prepare failure lost")
	}
	if got := db.Sidecars(); got != want {
		t.Fatalf("failed preparation changed inventory: %+v want %+v", got, want)
	}
	remove := MetricUpdate{Name: "a", Points: []Point{{Timestamp: -1, Value: 1}}}
	if err := db.UpdateBatch(ctx, []MetricUpdate{remove, update("b", 5)}, nil); err != nil {
		t.Fatal(err)
	}
	want = db.Sidecars()
	if want.Count != 1 || want.Points != 5 {
		t.Fatal(want)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open("store", options)
	if err != nil {
		t.Fatal(err)
	}
	if got := db.Sidecars(); got != want {
		t.Fatalf("reopened inventory: %+v want %+v", got, want)
	}
	if err := db.UpdateMany(ctx, "b", update("b", 6).Points); err != nil {
		t.Fatal(err)
	}
	if got := db.Sidecars(); got.Count != 1 || got.Points != 6 {
		t.Fatal(got)
	}
}

func TestCommitSidecarRecordsFailureIsAtomic(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	batch := db.db.NewBatch()
	defer batch.Close()
	want := db.Sidecars()
	failure := errors.New("injected commit failure")
	records := []SidecarRecord{{Name: "a", SidecarSummary: SidecarSummary{Present: true, Points: 2, Oldest: 100}}}
	if err := db.commitSidecarRecords(batch, records, func(*pebble.Batch) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if got := db.Sidecars(); got != want {
		t.Fatalf("failed commit published inventory: %+v", got)
	}
	if _, closer, err := db.db.Get(sidecarCatalogKey("a")); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			closer.Close()
		}
		t.Fatalf("failed commit persisted inventory: %v", err)
	}
}
