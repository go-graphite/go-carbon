package chunkstore

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestCompactionBudgetChangesPreserveConcurrentWrites(t *testing.T) {
	t.Parallel()
	now := time.Unix(1800000000, 0)
	path := filepath.Join(t.TempDir(), "exact")
	opts := Options{fs: noSyncFS{FS: vfs.Default}, ExactBlocks: true, CacheSize: 32 << 20, MemTableSize: 4 << 20, CompactionWorkers: 1, L0CompactionConcurrency: 2, Now: func() time.Time { return now }}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	for i := range 8 {
		_, err = db.Create(ctx, MetricConfig{Name: fmt.Sprintf("budget.%d", i), Retentions: []Retention{{Step: 1, Count: 4096}}, AggregationMethod: Last})
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 200 {
				if e := db.UpdateMany(ctx, fmt.Sprintf("budget.%d", i), []Point{{Timestamp: now.Unix() - 201 + int64(j), Value: float64(i*1000 + j)}}); e != nil {
					t.Error(e)
					return
				}
			}
		})
	}
	for _, workers := range []int{4, 8, 2, 1, 8} {
		if err = db.SetCompactionWorkers(workers); err != nil {
			t.Fatal(err)
		}
		if got := db.Stats().CompactionWorkers; got != int64(workers) {
			t.Fatalf("compaction budget=%d, want %d", got, workers)
		}
		time.Sleep(time.Millisecond)
	}
	for _, invalid := range []int{0, -1, 65} {
		if err = db.SetCompactionWorkers(invalid); err == nil || db.Stats().CompactionWorkers != 8 {
			t.Fatal("invalid budget changed the running store")
		}
	}
	wg.Wait()
	if err = db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		snapshot, e := db.Snapshot(ctx, fmt.Sprintf("budget.%d", i))
		if e != nil || len(snapshot.Archives) != 1 || len(snapshot.Archives[0].Points) != 200 {
			t.Fatalf("reopened budget history differs: %v", e)
		}
		for j, point := range snapshot.Archives[0].Points {
			if point.Timestamp != now.Unix()-201+int64(j) || math.Float64bits(point.Value) != math.Float64bits(float64(i*1000+j)) {
				t.Fatal("resource change lost or changed a committed point")
			}
		}
	}
}

func TestTableReaderBudgetRejectsInvalidValuesBeforeCreatingStore(t *testing.T) {
	t.Parallel()
	for _, budget := range []int{-1, 1, 63, 1048577} {
		path := filepath.Join(t.TempDir(), "invalid")
		if db, err := Open(path, Options{MaxOpenFiles: budget}); err == nil {
			_ = db.Close()
			t.Fatalf("invalid reader budget %d accepted", budget)
		}
	}
}
