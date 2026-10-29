package chunkstore

import (
	"math"
	"sync"
	"testing"
)

func TestVisitedMetricsConcurrentAndSparseIDs(t *testing.T) {
	var empty *IDSet
	empty.Mark(1)
	if empty.Contains(1) {
		t.Fatal("nil bitmap accepted an ID")
	}
	var sparse IDSet
	for _, id := range []uint64{0, 1, 65535, 65536, 1 << 63, math.MaxUint64} {
		sparse.Mark(id)
		if sparse.Contains(id) != (id != 0) {
			t.Fatalf("sparse ID %d was recorded incorrectly", id)
		}
	}
	if len(sparse.pages) != 4 || sparse.Contains(2) {
		t.Fatalf("sparse allocation or false positive: %d pages", len(sparse.pages))
	}
	var visited IDSet
	var group sync.WaitGroup
	for worker := range 16 {
		group.Go(func() {
			for id := uint64(worker + 1); id <= 100000; id += 16 {
				visited.Mark(id)
			}
		})
	}
	group.Wait()
	for id := uint64(1); id <= 100000; id++ {
		if !visited.Contains(id) {
			t.Fatalf("concurrent marking lost ID %d", id)
		}
	}
	if visited.Contains(100001) {
		t.Fatal("unvisited metric accepted")
	}
	// A new process/walk cannot reuse any earlier conversion's membership.
	var restarted IDSet
	if restarted.Contains(1) {
		t.Fatal("membership survived a new walk")
	}
}
