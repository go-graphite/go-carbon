package downsize

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
)

type boundedFullSync struct {
	*fakeSync
	largest atomic.Int64
}

func (b *boundedFullSync) Fetch(ctx context.Context, source string, names []string, from, until int64) (map[string]syncSeries, error) {
	for old := b.largest.Load(); int64(len(names)) > old; old = b.largest.Load() {
		if b.largest.CompareAndSwap(old, int64(len(names))) {
			break
		}
	}
	return b.fakeSync.Fetch(ctx, source, names, from, until)
}

func TestSyncFullHistoryMemoryBudgetParity(t *testing.T) {
	f := newFakeSync()
	// All stores disagree, so every name takes the materialized fallback.
	// A week at one-minute resolution must not be fetched as one 500-name
	// response from each peer. Compare the complete ordered verdict to the
	// original point-only policy, including holes near a batch boundary.
	names := make([]string, 500)
	schema := []syncArchive{{Step: 60, Points: 7 * 1440}}
	schemas := make(map[string][]syncArchive)
	for i := range names {
		n := fmt.Sprintf("memory.%04d", i)
		names[i] = n
		schemas[n] = schema
		for _, source := range []string{syncT, syncD1, syncD2, syncP} {
			f.put(source, n, schema)
		}
		delete(f.store(syncT).points[n][60], syncTestNow-86400)
	}
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2, syncP}}
	b := &boundedFullSync{fakeSync: f}
	s := &syncer{o: o, b: b}
	for _, n := range names {
		s.masks.Store(n, uint64(7))
	}
	var want, got syncRow
	if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	b.largest.Store(0)
	if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered hole verdict changed: got %d, want %d", len(got.Holes), len(want.Holes))
	}
	if largest := b.largest.Load(); largest == 0 || largest > 64 {
		t.Fatalf("week-long fallback retained %d names in one peer response; want <=64", largest)
	}
}
