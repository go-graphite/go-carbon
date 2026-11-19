package v2core

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// pendingRunBenchHead appends rows one-point applications over ids IDs to a
// head, publishing a view every pub rows, and returns the last view.
// raw publishes without runs (the cost before them).
func pendingRunBenchHead(rows, ids, pub int, raw bool) PendingView {
	rng := rand.New(rand.NewPCG(1, 2))
	find := func(uint64) *ApplicationPolicy { return nil }
	h := &pendingHead{}
	var c pendingCompaction
	var v PendingView
	batch := make([]coreBatch, 0, 256)
	order := uint64(1)
	for i := 0; i < rows; {
		batch = batch[:0]
		for k := 0; k < 256 && i < rows; k, i = k+1, i+1 {
			id := uint64(rng.IntN(ids)) + 1
			batch = append(batch, coreBatch{ID: id, Order: order, Clock: 1, Points: []row{{ID: id, Time: 1_700_000_000 + int64(i), Bits: uint64(i), Seq: order}}})
			order++
		}
		h.add(batch, find)
		if i%pub == 0 || i == rows {
			if raw {
				v = h.rawView(find)
			} else {
				h.view(find)
				c.start(h)
				c.wait(h)
				v = h.view(find)
			}
		}
	}
	return v
}

// BenchmarkPendingRunPublish is the owner's append-and-publish cost per
// pending row (262,144 rows in 64 publications over 1,048,576 IDs) with one
// ID-sorted run per publication including joined background merges (runs),
// and without (raw). Joining includes the complete work rather than leaving
// outstanding merges outside the timer. The difference is the runs' cost.
func BenchmarkPendingRunPublish(b *testing.B) {
	const rows, ids, pub = 1 << 18, 1 << 20, 1 << 12
	for _, raw := range []bool{false, true} {
		name := "runs"
		if raw {
			name = "raw"
		}
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				pendingRunBenchHead(rows, ids, pub, raw)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/rows, "ns/row")
		})
	}
}

// BenchmarkPendingRunFind is a one-ID read of the same head: binary search of
// its runs (Find) against the linear row scan it replaces.
func BenchmarkPendingRunFind(b *testing.B) {
	const rows, ids, pub = 1 << 18, 1 << 20, 1 << 12
	v := pendingRunBenchHead(rows, ids, pub, false)
	none := func(*coreBatch) (bool, error) { return false, nil }
	b.Run("runs", func(b *testing.B) {
		b.ReportMetric(float64(len(v.runs)), "runs")
		for i := 0; b.Loop(); i++ {
			if _, err := v.Find([]uint64{uint64(i%ids) + 1}, none); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("linear", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			want, n := uint64(i%ids)+1, 0
			v.IDs(0, v.Len(), func(_ int, id uint64, _ *coreBatch) bool {
				if id == want {
					n++
				}
				return true
			})
			_ = n
		}
	})
}

// TestPendingRunSortByID checks the run sort against a full key sort for keys
// in position order, over narrow and wide ID ranges (radix bytes skipped and
// used) and sizes on both sides of the small-run cutoff.
func TestPendingRunSortByID(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, n := range []int{0, 1, 255, 256, 5000, 100000} {
		for _, span := range []uint64{1, 300, 1 << 20, 1<<32 - 1} {
			keys := make([]uint64, n)
			for pos := range keys {
				keys[pos] = rng.Uint64N(span)<<32 | uint64(pos)
			}
			want := slices.Sorted(slices.Values(keys))
			if got := sortByID(keys); !slices.Equal(got, want) {
				t.Fatalf("n %d span %d: radix order differs from key order", n, span)
			}
		}
	}
}
