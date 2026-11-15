package v2core

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestCoreBucketSortMatchesOriginalBytes(t *testing.T) {
	rng := rand.New(rand.NewPCG(41, 19))
	var scratch, work []row
	for trial := range 48 {
		rows := make([]row, 8192)
		for i := range rows {
			id := rng.Uint64()
			if trial%4 == 0 {
				id %= 64
			}
			rows[i] = row{id, 1800000000 + int64(i%30), rng.Uint64(), uint64(i)}
			if trial%4 == 1 {
				rows[i].Time = rng.Int64()
			}
			if trial%4 == 2 {
				rows[i].Time = -3600 + int64(i%30)
			}
		}
		if trial%4 == 3 {
			// Equal sort keys with unequal IEEE payloads must retain the old
			// unstable comparator's exact choice, including NaN and signed zero.
			for i := range 2000 {
				rows[i] = row{17, 1800000000, []uint64{0, 1 << 63, 0x7ff8000000000001, math.MaxUint64}[i%4], 1}
			}
		}
		if trial == 1 {
			rows = append(rows, row{Time: math.MinInt64}, row{Time: math.MaxInt64})
		}
		original := slices.Clone(rows)
		want := slices.Clone(rows)
		sortCoreRows(want, 3600)
		scratch = sortCoreRowsBucketed(rows, 3600, scratch)
		if !slices.Equal(rows, want) {
			t.Fatalf("bucket trial %d differs", trial)
		}
		copy(rows, original)
		scratch, work = sortCoreRowsRadix(rows, 3600, scratch, work)
		if !slices.Equal(rows, want) {
			t.Fatalf("trial %d differs", trial)
		}
	}
}

func BenchmarkCoreSortCandidates(b *testing.B) {
	rng := rand.New(rand.NewPCG(64, 96))
	source := make([]row, 1<<20)
	for i := range source {
		source[i] = row{rng.Uint64N(16000000), 1800000000, rng.Uint64(), uint64(i)}
	}
	for _, algorithm := range []string{"comparison", "bucketed", "radix"} {
		b.Run(algorithm, func(b *testing.B) {
			rows, scratch, work := make([]row, len(source)), make([]row, len(source)), make([]row, len(source))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				copy(rows, source)
				if algorithm == "radix" {
					scratch, work = sortCoreRowsRadix(rows, 3600, scratch, work)
				} else if algorithm == "bucketed" {
					scratch = sortCoreRowsBucketed(rows, 3600, scratch)
				} else {
					sortCoreRows(rows, 3600)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(source)), "ns/point")
		})
	}
}
