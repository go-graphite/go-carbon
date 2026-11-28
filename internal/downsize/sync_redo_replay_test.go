package downsize

import (
	"math/rand/v2"
	"reflect"
	"testing"
	"time"
)

// Batched replay must rebuild exactly the union that row-by-row redoMerge
// builds, including overlapping, adjacent, duplicate and unordered ranges.
func TestRedoGuardReplayMatchesIncrementalMerge(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	var rows []syncRow
	want := map[uint64][]syncRedoRange{}
	for range 3000 {
		var w syncRedoWork
		for range 1 + rng.IntN(3) {
			w.Keys = append(w.Keys, uint64(rng.IntN(4)))
		}
		for range 1 + rng.IntN(40) {
			first := rng.Int64N(redoReplayBatch / 2)
			w.IDs = append(w.IDs, syncRedoRange{first, first + rng.Int64N(3)})
		}
		for _, k := range w.Keys {
			for _, r := range w.IDs {
				want[k] = redoMerge(want[k], r)
			}
		}
		rows = append(rows, syncRow{Kind: "verified", Work: []syncRedoWork{w}})
	}
	g := newSyncRedoGuard(&syncJournal{rows: rows})
	if !reflect.DeepEqual(g.seen, want) {
		t.Fatal("batched replay differs from incremental merge")
	}
}

// A journal of many disjoint ranges replayed newest-first was quadratic.
func TestRedoGuardReplayManyDisjointRanges(t *testing.T) {
	t.Parallel()
	const n = 400_000
	rows := make([]syncRow, 0, n/100)
	for i := n/100 - 1; i >= 0; i-- {
		w := syncRedoWork{Keys: []uint64{7}}
		for j := 99; j >= 0; j-- {
			id := int64(2 * (i*100 + j))
			w.IDs = append(w.IDs, syncRedoRange{id, id})
		}
		rows = append(rows, syncRow{Kind: "verified", Work: []syncRedoWork{w}})
	}
	started := time.Now()
	g := newSyncRedoGuard(&syncJournal{rows: rows})
	if len(g.seen[7]) != n || g.seen[7][0].First != 0 || g.seen[7][n-1].Last != 2*(n-1) {
		t.Fatal("unexpected union", len(g.seen[7]))
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("replay of %d disjoint ranges took %s", n, elapsed)
	}
}
