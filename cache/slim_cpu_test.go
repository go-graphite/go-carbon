package cache

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in phase costs include the real cache and real collect/encode/compress/
// write/sync driver. Process CPU excludes descheduling and fsync waits.
func BenchmarkBookkeepingCPU(b *testing.B)       { bookkeepingCPU(b, false) }
func BenchmarkBookkeepingCPUSingle(b *testing.B) { bookkeepingCPU(b, true) }
func bookkeepingCPU(b *testing.B, single bool) {
	const rounds, n, staged = 12, 100, 10
	metrics, _ := strconv.Atoi(os.Getenv("CACHE_CPU_METRICS"))
	if metrics == 0 {
		metrics = 200000
	}
	data := compactWorkload("mix", n)
	names := make([]string, metrics)
	for i := range names {
		names[i] = fmt.Sprintf("namespace.component.instance.%08d.counter.value", i)
	}
	for range b.N {
		c := New()
		c.SetMaxSize(0)
		if os.Getenv("CACHE_CPU_THROTTLE") != "" {
			c.SetThrottleWithState(func(_ *points.Points, _ bool, state *any) bool {
				if *state == nil {
					*state = uint64(1)
				}
				return false
			})
		}
		l, _ := startInputLog(b, b.TempDir(), c, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 64 << 20, Workers: 4})
		var ingest, logCPU, popCPU int64
		var ingestAllocations, ingestBytes uint64
		for cycle := 0; cycle < rounds; cycle++ {
			for i, name := range names {
				c.AddRecovered(&points.Points{Metric: name, Data: data[i%len(data)][:n-staged]})
			}
			runtime.GC()
			var memBefore, memAfter runtime.MemStats
			runtime.ReadMemStats(&memBefore)
			before := compactCPUTime()
			for i, name := range names {
				tail := data[i%len(data)][n-staged:]
				if single {
					for _, p := range tail {
						c.AddPoint([]byte(name), p.Value, p.Timestamp)
					}
				} else {
					c.AddPoints([]byte(name), tail)
				}
			}
			a := compactCPUTime()
			runtime.ReadMemStats(&memAfter)
			logStart := compactCPUTime()
			syncLog(b, l)
			z := compactCPUTime()
			for _, name := range names {
				p, ok := c.PopNotConfirmed(name)
				if !ok || len(p.Data) != n {
					b.Fatal("pop lost points")
				}
				c.Confirm(p)
			}
			end := compactCPUTime()
			if cycle >= 2 {
				ingest += a - before
				logCPU += z - logStart
				ingestAllocations += memAfter.Mallocs - memBefore.Mallocs
				ingestBytes += memAfter.TotalAlloc - memBefore.TotalAlloc
				popCPU += end - z
			}
		}
		if c.LogDropped() != 0 || !c.IsEmpty() {
			b.Fatal("incomplete lifecycle")
		}
		l.Close()
		b.ReportMetric(float64(ingestAllocations)/float64((rounds-2)*metrics*staged), "ingest-allocs/point")
		b.ReportMetric(float64(ingestBytes)/float64((rounds-2)*metrics*staged), "ingest-alloc-B/point")
		b.ReportMetric(float64(ingest)/float64((rounds-2)*metrics*staged), "ingest-cpu-ns/point")
		b.ReportMetric(float64(logCPU)/float64((rounds-2)*metrics*staged), "log-cpu-ns/point")
		b.ReportMetric(float64(popCPU)/float64((rounds-2)*metrics*n), "pop-cpu-ns/point")
	}
}
