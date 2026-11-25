package cache

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/points"
)

func BenchmarkStreamAdmissionResident(b *testing.B) {
	for _, count := range []int{4096, 262144} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			c := New()
			c.SetMaxSize(0)
			c.SetThrottleWithHash(func(p *points.Points, _ bool, h uint64, state *any) bool {
				n, _ := (*state).(uint64)
				*state = n + 1
				return h&255 == 0 && n%17 == 16
			})
			series := make([]*StreamSeries, count)
			for i := range series {
				name := fmt.Appendf(nil, "example.application.region.zone.worker.%06d.requests.count", i)
				series[i] = c.ResolveStreamName(name).(*StreamSeries)
				c.addStreamSeries(series[i], 1, 1)
			}
			// Warm every hint after construction has finished rehashing.
			for _, s := range series {
				c.addStreamSeries(s, 1, 1)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			b.ReportAllocs()
			b.ResetTimer()
			cpu := compactCPUTime()
			var state uint64 = 1
			for i := range b.N {
				state = state*6364136223846793005 + 1
				s := series[(state>>32)%uint64(count)]
				c.addStreamSeries(s, float64(i%29), int64(i/count))
			}
			cpu = compactCPUTime() - cpu
			b.StopTimer()
			var final runtime.MemStats
			runtime.ReadMemStats(&final)
			b.ReportMetric(float64(final.TotalAlloc-after.TotalAlloc)/float64(b.N), "B/point")
			b.ReportMetric(float64(final.Mallocs-after.Mallocs)/float64(b.N), "allocs/point")
			b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/point")
			b.ReportMetric(float64(unsafe.Sizeof(item{})), "item-B")
			b.ReportMetric(float64(unsafe.Sizeof(StreamSeries{})), "identity-B")
			b.ReportMetric(float64(retained)/float64(count), "resident-B/name")
			runtime.KeepAlive(c)
		})
	}
}
