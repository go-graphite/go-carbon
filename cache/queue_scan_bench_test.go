package cache

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// Opt in to a large resident cache; normal test runs allocate no fixture.
func BenchmarkDeferredQueueScan(b *testing.B) {
	n, _ := strconv.Atoi(os.Getenv("INGEST_BENCH_METRICS"))
	if n <= 0 {
		b.Skip("set INGEST_BENCH_METRICS")
	}
	c := New()
	c.SetMaxSize(0)
	c.SetWriteoutBatching(960, 960*time.Second)
	c.SetWriteoutSpread(.2)
	readyEvery, _ := strconv.Atoi(os.Getenv("QUEUE_BENCH_READY_EVERY"))
	ready := 0
	data := make([]points.Point, 959)
	for i := range data {
		data[i] = points.Point{Value: 1, Timestamp: 1790000000}
	}
	for i := range n {
		name := fmt.Appendf(nil, "services.namespace-%04d.host-%08d.component.requests.value", i%1000, i)
		c.AddPoint(name, 1, 1790000000)
		if readyEvery > 0 && i%readyEvery == 0 {
			c.AddPoints(name, data)
			ready++
		}
	}
	now := time.Now()
	c.makeQueueAt(now) // Warm reusable scratch before measuring scans.
	cold := os.Getenv("QUEUE_BENCH_COLD") == "1"
	var summaryBytes int
	for _, shard := range c.data {
		if d := shard.items.deadlines; d != nil {
			summaryBytes += int(reflect.TypeFor[queueDeadlines]().Size()) + cap(d.next)*int(reflect.TypeFor[time.Time]().Size())
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if cold {
			for _, shard := range c.data {
				if d := shard.items.deadlines; d != nil {
					d.settings = nil
				}
			}
		}
		q, next := c.makeQueueAt(now)
		if len(q) != ready || (ready < n && next.IsZero()) {
			b.Fatal("expected deferred work")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(summaryBytes)/float64(n), "summary-B/metric")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/metric")
}
