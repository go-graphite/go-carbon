package helper

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkMetricQueuePending5M(b *testing.B) {
	const count = 5000000
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("burst.group%04d.metric%06d", i/1000, i%1000)
	}
	for _, mode := range []string{"channel", "pending"} {
		b.Run(mode, func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				b.StartTimer()
				if mode == "channel" {
					ch := make(chan string, 393216)
					dropped := 0
					for _, name := range names {
						select {
						case ch <- name:
						default:
							dropped++
						}
					}
					b.StopTimer()
					runtime.GC()
					runtime.ReadMemStats(&after)
					runtime.KeepAlive(ch)
					b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(len(ch)), "B/pending")
					b.ReportMetric(float64(dropped), "dropped")
				} else {
					q := NewMetricQueue()
					for _, name := range names {
						q.Add(name)
					}
					b.StopTimer()
					runtime.GC()
					runtime.ReadMemStats(&after)
					runtime.KeepAlive(q)
					if q.Len() != count {
						b.Fatal(q.Len())
					}
					b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/count, "B/pending")
					b.ReportMetric(0, "dropped")
				}
			}
		})
	}
	runtime.KeepAlive(names)
}
