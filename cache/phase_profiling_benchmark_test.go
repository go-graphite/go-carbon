//go:build unix

package cache

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
)

// The same synthetic Graphite series mix as BenchmarkAddPointParallelSeries:
// counters, gauges, floats, constants, and rare late/duplicate timestamps.
// Producers share 512 names; bounded 128-point writeouts keep memory small.
func BenchmarkIngestProfiles(b *testing.B) {
	for _, cfg := range []struct{ fraction, block int }{{0, 0}, {1, 0}, {5, 0}, {100, 0}, {0, 1000000}, {100, 1000000}} {
		b.Run(fmt.Sprintf("mutex=%d/block=%d", cfg.fraction, cfg.block), func(b *testing.B) {
			previous := runtime.SetMutexProfileFraction(cfg.fraction)
			runtime.SetBlockProfileRate(cfg.block)
			defer runtime.SetMutexProfileFraction(previous)
			defer runtime.SetBlockProfileRate(0)
			c := New()
			c.SetMaxSize(0)
			var workers atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			cpu := cpuTime()
			b.RunParallel(func(pb *testing.PB) {
				id := workers.Add(1)
				names, gens := seriesSet(512, 0)
				keys := make([][]byte, len(names))
				counts := make([]uint8, len(names))
				for i, name := range names {
					keys[i] = []byte(name)
				}
				for i := int(id * 17); pb.Next(); i++ {
					m := i % len(names)
					value, ts := gens[m].next()
					c.AddPoint(keys[m], value, ts)
					if counts[m]++; counts[m] == 128 {
						counts[m] = 0
						if p, ok := c.PopNotConfirmed(names[m]); ok {
							c.Confirm(p)
						}
					}
				}
			})
			elapsedCPU := cpuTime() - cpu
			b.StopTimer()
			b.ReportMetric(float64(elapsedCPU.Nanoseconds())/float64(b.N), "cpu-ns/point")
		})
	}
}
