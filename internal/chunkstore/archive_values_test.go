package chunkstore

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestArchiveValuesCommitAtomicallyAndRemoveMergedSlots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "exact")
	db, now := openTestStore(t, dir, Options{ExactBlocks: true})
	defer func() { _ = db.Close() }()
	timestamp := int64(now/60*60 - 60)
	_, err := db.Create(ctx, MetricConfig{Name: "mirror", Retentions: []Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}, AggregationMethod: Sum})
	if err != nil {
		t.Fatal(err)
	}
	put := func(value float64) error {
		return db.UpdateManyWithArchiveValues(ctx, "mirror", []Point{{Timestamp: timestamp, Value: value}}, []ArchiveValue{{Archive: 1, Timestamp: timestamp, Value: -value, Present: true}})
	}
	if err := put(1); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	errs := make(chan error, 1)
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-stopped:
				return
			default:
			}
			snapshot, err := db.Snapshot(ctx, "mirror")
			if err == nil {
				a, b := snapshot.Archives[0].Points, snapshot.Archives[1].Points
				if len(a) != 1 || len(b) != 1 || a[0].Value != -b[0].Value {
					err = fmt.Errorf("partially published archive correction: %+v", snapshot)
				}
			}
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	for i := 2; i < 100; i++ {
		if err := put(float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	close(stopped)
	reader.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	// The slot has merge operands; an absent override must materialize it so
	// compaction cannot resurrect the old value from an earlier operand.
	if err := db.UpdateManyWithArchiveValues(ctx, "mirror", nil, []ArchiveValue{{Archive: 1, Timestamp: timestamp, Present: false}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.Snapshot(ctx, "mirror")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 1 || len(snapshot.Archives[1].Points) != 0 {
		t.Fatalf("removed virtual rollup resurrected: %+v", snapshot)
	}
	newer := timestamp + 1440*60
	if err := db.UpdateManyWithArchiveValues(ctx, "mirror", nil, []ArchiveValue{{Archive: 1, Timestamp: newer, Value: 3, Present: true}, {Archive: 1, Timestamp: timestamp, Present: false}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = db.Snapshot(ctx, "mirror")
	if err != nil {
		t.Fatal(err)
	}
	if p := snapshot.Archives[1].Points; len(p) != 1 || p[0].Timestamp != newer || p[0].Value != 3 {
		t.Fatalf("older absence removed newer owner: %+v", p)
	}
}
