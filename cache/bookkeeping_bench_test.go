package cache

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/points"
)

// Opt in with CACHE_BOOKKEEPING_METRICS=5000000. The same benchmark is built
// against the old and new implementations; it exercises real insertion and
// pop/confirmation, not an accounting-only model. All point bytes remain in
// the cache. The last ten points per metric model one undrained log window.
func BenchmarkCacheBookkeepingHeap(b *testing.B) {
	n, _ := strconv.Atoi(os.Getenv("CACHE_BOOKKEEPING_METRICS"))
	if n == 0 {
		b.Skip("set CACHE_BOOKKEEPING_METRICS=5000000 for 500M cached points")
	}
	const perMetric, staged = 100, 10
	for range b.N {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		c := New()
		c.SetMaxSize(0)
		c.EnableInputLog()
		data := make([]points.Point, perMetric)
		for i := range data {
			data[i] = points.Point{Value: 42, Timestamp: 1700000000 + int64(i)*60}
		}
		prefix := strings.Repeat("namespace.component.", 8)
		nameOf := func(i int) string { return fmt.Sprintf("%smetric.%07d", prefix, i) }
		started := time.Now()
		for i := range n {
			c.AddRecovered(&points.Points{Metric: nameOf(i), Data: data[:perMetric-staged]})
		}
		restore := time.Since(started)
		started = time.Now()
		for _, sh := range c.data {
			for name := range sh.items.all {
				c.AddPoints([]byte(name), data[perMetric-staged:])
			}
		}
		ingest := time.Since(started)
		if c.Size() != int64(n)*perMetric || c.LogDropped() != 0 {
			b.Fatalf("size=%d dropped=%d", c.Size(), c.LogDropped())
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		var names, pointBuffers, slots int64
		for _, sh := range c.data {
			slots += int64(cap(sh.items.slots)) * int64(unsafe.Sizeof(itemSlot{}))
			for _, it := range sh.items.all {
				names += int64(len(it.Metric))
				pointBuffers += int64(cap(it.compact.buf))
			}
		}
		heap := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		// These names are exactly 174 bytes, rounded to 176 by the allocator.
		// Point buffers in this fixture are exactly 32 bytes (a size class).
		if len(nameOf(0)) != 174 || pointBuffers != int64(n)*32 {
			b.Fatalf("update payload accounting: name=%d point buffers=%d", len(nameOf(0)), pointBuffers)
		}
		bookkeeping := heap - int64(n)*176 - pointBuffers
		b.ReportMetric(float64(heap)/float64(n), "heap-B/metric")
		b.ReportMetric(float64(bookkeeping)/float64(n), "book-B/metric")
		b.ReportMetric(float64(bookkeeping)/float64(n*perMetric), "book-B/point")
		b.ReportMetric(float64(unsafe.Sizeof(item{})), "item-B")
		b.ReportMetric(float64(slots)/float64(n), "slots-B/metric")
		b.ReportMetric(float64(names)/float64(n), "name-B/metric")
		b.ReportMetric(float64(pointBuffers)/float64(n*perMetric), "encoded-B/point")
		b.ReportMetric(float64(restore.Nanoseconds())/float64(n*(perMetric-staged)), "restore-ns/point")
		b.ReportMetric(float64(ingest.Nanoseconds())/float64(n*staged), "ingest-ns/point")
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(n*perMetric), "fill-alloc-B/point")
		b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(n*perMetric), "fill-allocs/point")
		// Pop in bounded batches without invalidating the iterator being used.
		var popTime time.Duration
		var popped int
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		alloc, mallocs := mem.TotalAlloc, mem.Mallocs
		for _, sh := range c.data {
			var batch []string
			for name := range sh.items.all {
				batch = append(batch, name)
			}
			started = time.Now()
			for _, name := range batch {
				p, ok := c.PopNotConfirmed(name)
				if !ok || len(p.Data) != perMetric {
					b.Fatal("incomplete pop")
				}
				c.Confirm(p)
				popped += len(p.Data)
			}
			popTime += time.Since(started)
		}
		runtime.ReadMemStats(&mem)
		if popped != n*perMetric || c.LogLowWater() != 0 || !c.IsEmpty() {
			b.Fatal("incomplete drain")
		}
		b.ReportMetric(float64(popTime.Nanoseconds())/float64(popped), "pop-ns/point")
		b.ReportMetric(float64(mem.TotalAlloc-alloc)/float64(popped), "pop-alloc-B/point")
		b.ReportMetric(float64(mem.Mallocs-mallocs)/float64(popped), "pop-allocs/point")
		runtime.KeepAlive(c)
	}
}
