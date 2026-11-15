package sealrollup

import (
	"fmt"
	"math"
	"testing"
)

// Low-density, incrementally advancing owner-local sealed views. Includes policy
// validation/fingerprinting, immutable planning and publication into owner state.
// No SST I/O: use BenchmarkAtomicSeal for the full atomic seal adapter.
func BenchmarkSealPlanDensity(b *testing.B) {
	for _, density := range []int{1, 4, 16} {
		for _, archives := range []int{1, 4} {
			b.Run(fmt.Sprintf("density%d/archives%d", density, archives), func(b *testing.B) {
				c := config("1s:1d,1m:30d,1h:1y,1d:10y", Average, 0)
				c.Archives = c.Archives[:archives]
				b.ReportAllocs()
				b.ResetTimer()
				start := cpuNanos()
				b.RunParallel(func(pb *testing.PB) {
					state := map[key]Sample{}
					prior := Frontier{}
					points := make([]Sample, density)
					order := uint64(0)
					for pb.Next() {
						order++
						now := int64(1794700800) + int64(order)*int64(density)
						for i := range points {
							points[i] = Sample{Time: now - int64(density) + int64(i), Bits: math.Float64bits(float64((order + uint64(i)) % 97)), Sequence: uint64(i + 1)}
						}
						p, err := Plan(c, prior, view(state), []Batch{{Order: order, Now: now, Points: points}})
						if err != nil {
							panic(err)
						}
						for _, r := range p.Records {
							state[key{r.Archive, r.Slot}] = r.Sample
						}
						prior = p.Next
					}
				})
				cpu := cpuNanos() - start
				b.StopTimer()
				b.ReportMetric(float64(cpu)/float64(b.N*density), "cpu-ns/point")
				b.ReportMetric(float64(b.N*density)/b.Elapsed().Seconds(), "points/s")
			})
		}
	}
}

// Prepared sparse path on the same advancing input. Each parallel caller owns
// its index; CPU timing excludes I/O and does not qualify the complete engine.
func BenchmarkSealPlanSparse(b *testing.B) {
	for _, density := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("density%d", density), func(b *testing.B) {
			c := config("1s:1d,1m:30d,1h:1y,1d:10y", Average, 0)
			prepared, err := Prepare(c)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			start := cpuNanos()
			b.RunParallel(func(pb *testing.PB) {
				state := map[key]Sample{}
				prior := Frontier{}
				points := make([]Sample, density)
				order := uint64(0)
				var workspace Workspace
				index := NewSlots(c.Archives)
				ranges := index.Range
				for pb.Next() {
					order++
					now := int64(1794700800) + int64(order)*int64(density)
					for i := range points {
						points[i] = Sample{Time: now - int64(density) + int64(i), Bits: math.Float64bits(float64((order + uint64(i)) % 97)), Sequence: uint64(i + 1)}
					}
					p, err := prepared.PlanWithWorkspace(prior, view(state), ranges, []Batch{{Order: order, Now: now, Points: points}}, &workspace)
					if err != nil {
						panic(err)
					}
					for _, r := range p.Records {
						index.Install(r.Delta)
						state[key{r.Archive, r.Slot}] = r.Sample
					}
					prior = p.Next
				}
			})
			cpu := cpuNanos() - start
			b.StopTimer()
			b.ReportMetric(float64(cpu)/float64(b.N*density), "cpu-ns/point")
			b.ReportMetric(float64(b.N*density)/b.Elapsed().Seconds(), "points/s")
		})
	}
}
