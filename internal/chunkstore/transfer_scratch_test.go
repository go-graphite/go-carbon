package chunkstore

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// The original allocating slot map is intentionally independent of the pool.
func referenceRetainSlots(points []Point, r Retention) []Point {
	seen := make(map[int64]struct{})
	start := len(points)
	for i := len(points) - 1; i >= 0; i-- {
		slot := points[i].Timestamp / int64(r.Step) % int64(r.Count)
		if _, ok := seen[slot]; ok {
			continue
		}
		seen[slot] = struct{}{}
		start--
		points[start] = points[i]
	}
	return points[start:]
}

func TestTransferScratchDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(732, 91))
	var scratch []Point
	for trial := range 1024 {
		r := Retention{Step: 1 + rng.IntN(60), Count: 1 + rng.IntN(513)}
		makePoints := func() []Point {
			p := make([]Point, rng.IntN(1024))
			for i := range p {
				p[i] = Point{Timestamp: int64(1 + rng.IntN(100000)), Value: math.Float64frombits(rng.Uint64())}
			}
			slices.SortStableFunc(p, func(a, b Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
			return p
		}
		a, b := makePoints(), makePoints()
		want, err := fillArchive(a, b, r.Step)
		if err != nil {
			t.Fatal(err)
		}
		want = referenceRetainSlots(want, r)
		got, _, _, err := nativeArchiveUnionInto(Archive{r, slices.Clone(a)}, Archive{r, slices.Clone(b)}, scratch, false)
		if err != nil {
			t.Fatal(err)
		}
		scratch = got[:0]
		got = retainTransferSlots(got, r)
		if len(want) != len(got) {
			t.Fatalf("trial %d count differs", trial)
		}
		for i, p := range want {
			if p.Timestamp != got[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(got[i].Value) {
				t.Fatalf("trial %d point %d changed", trial, i)
			}
		}
		// Classification is still required by native combine, even though
		// ordinary imports no longer compute and discard it.
		a, _, _ = nativeNormalizeArchive(a, r.Step)
		b, _, _ = nativeNormalizeArchive(b, r.Step)
		seen := make(map[int64]bool)
		for _, p := range a {
			seen[p.Timestamp/int64(r.Step)%int64(r.Count)] = true
		}
		intersects := false
		for _, p := range b {
			intersects = intersects || seen[p.Timestamp/int64(r.Step)%int64(r.Count)]
		}
		if nativeSlotsIntersect(a, b, r) != intersects {
			t.Fatalf("trial %d slot classification changed", trial)
		}
	}
}

func BenchmarkTransferSlotRetention(b *testing.B) {
	r := Retention{Step: 60, Count: 50400}
	input := make([]Point, 40000)
	for i := range input {
		input[i] = Point{Timestamp: int64(100000 + i*180), Value: float64(i)}
	}
	for _, old := range []bool{true, false} {
		name := "pooled"
		if old {
			name = "allocating"
		}
		b.Run(name, func(b *testing.B) {
			points := make([]Point, len(input))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				copy(points, input)
				if old {
					referenceRetainSlots(points, r)
				} else {
					retainTransferSlots(points, r)
				}
			}
		})
	}
}
