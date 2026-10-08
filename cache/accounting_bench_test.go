package cache

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// BenchmarkCacheAccounting exercises the shared size counter and settings
// reads without retaining an ever-growing backlog or allocating per point.
func BenchmarkCacheAccounting(b *testing.B) {
	c := New()
	c.SetMaxSize(2_000_000_000)
	var worker atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		metric := fmt.Sprintf("accounting.worker.%d", worker.Add(1))
		p := points.OnePoint(metric, 1, 1)
		for pb.Next() {
			c.Add(p)
			if _, ok := c.Pop(metric); !ok {
				b.Error("accepted point missing from cache")
				return
			}
		}
	})
	if got := c.Size(); got != 0 {
		b.Fatalf("cache size after drain = %d, want 0", got)
	}
}
