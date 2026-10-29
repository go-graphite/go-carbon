package cache

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// A single hot series serializes producers on its cache shard. Independent
// series measure the same intake with those locks spread across shards.
// Keep at most 256 points per producer between pops so memory stays bounded.
func BenchmarkIntakeShardContention(b *testing.B) {
	for _, hot := range []bool{true, false} {
		name := "spread"
		if hot {
			name = "hot"
		}
		b.Run(name, func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			var worker atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := worker.Add(1)
				if hot {
					id = 0
				}
				metric := fmt.Sprintf("phase.contention.%d", id)
				key := []byte(metric)
				n := 0
				for pb.Next() {
					c.AddPoint(key, float64(n%17), int64(n))
					n++
					if n%256 == 0 {
						c.Pop(metric)
					}
				}
			})
		})
	}
}
