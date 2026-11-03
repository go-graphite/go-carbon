//go:build linux || darwin

package cache

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// This bounded lifecycle measures every admitted point through the actual cache,
// log collect/encode/compress/write/sync, and pop/confirm. No recovered fill is
// excluded from the denominator. Process CPU includes GC and log workers.
// The workload is synthetic: 100k series, 100 points/series, ten log windows.
// Resolve identities once, retain them across pops, as streaming clients do.
func BenchmarkStackPipeline(b *testing.B) {
	metrics, _ := strconv.Atoi(os.Getenv("CACHE_STACK_METRICS"))
	if metrics == 0 {
		metrics = 100000
	}
	mode := os.Getenv("CACHE_STACK_MODE")
	if mode == "" {
		mode = "batch"
	}
	const n, window, rounds = 100, 10, 5
	data := compactWorkload("mix", n)
	names := make([]string, metrics)
	for i := range names {
		names[i] = fmt.Sprintf("namespace.component.instance.%08d.counter.value", i) + strings.Repeat(".segment", 16)
	}
	for range b.N {
		c := New()
		c.SetMaxSize(0)
		c.SetThrottleWithState(func(_ *points.Points, _ bool, state *any) bool {
			if *state == nil {
				*state = uint64(1)
			}
			return false
		})
		identities := make([]*StreamSeries, metrics)
		if mode == "stream" {
			for i, name := range names {
				identities[i] = c.ResolveStreamName([]byte(name)).(*StreamSeries)
			}
		}
		l, _ := startInputLog(b, b.TempDir(), c, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 8 << 20, Workers: 4})
		var ingest, logCPU, popCPU int64
		var allocations, allocated uint64
		for cycle := 0; cycle < rounds; cycle++ {
			runtime.GC()
			var beforeMem, afterMem runtime.MemStats
			runtime.ReadMemStats(&beforeMem)
			var in, lg int64
			for offset := 0; offset < n; offset += window {
				before := stackCPUTime()
				for i, name := range names {
					tail := data[i%len(data)][offset : offset+window]
					switch mode {
					case "stream":
						for _, p := range tail {
							c.addStreamSeries(identities[i], p.Value, p.Timestamp)
						}
					case "single":
						for _, p := range tail {
							c.AddPoint([]byte(name), p.Value, p.Timestamp)
						}
					default:
						c.AddPoints([]byte(name), tail)
					}
				}
				mid := stackCPUTime()
				syncLog(b, l)
				after := stackCPUTime()
				in += mid - before
				lg += after - mid
			}
			before := stackCPUTime()
			for i, name := range names {
				p, ok := c.PopNotConfirmed(name)
				expected := data[i%len(data)]
				if !ok || len(p.Data) != n || p.Data[n-1] != expected[n-1] {
					b.Fatal("pop lost points")
				}
				c.Confirm(p)
			}
			pc := stackCPUTime() - before
			runtime.ReadMemStats(&afterMem)
			if cycle > 0 {
				ingest += in
				logCPU += lg
				popCPU += pc
				allocated += afterMem.TotalAlloc - beforeMem.TotalAlloc
				allocations += afterMem.Mallocs - beforeMem.Mallocs
			}
		}
		if c.LogDropped() != 0 || !c.IsEmpty() {
			b.Fatal("incomplete lifecycle")
		}
		if err := l.Close(); err != nil {
			b.Fatal(err)
		}
		denom := float64((rounds - 1) * metrics * n)
		b.ReportMetric(float64(ingest)/denom, "ingest-cpu-ns/point")
		b.ReportMetric(float64(logCPU)/denom, "log-cpu-ns/point")
		b.ReportMetric(float64(popCPU)/denom, "pop-cpu-ns/point")
		b.ReportMetric(float64(ingest+logCPU+popCPU)/denom, "total-cpu-ns/point")
		b.ReportMetric(float64(allocated)/denom, "allocated-B/point")
		b.ReportMetric(float64(allocations)/denom, "allocs/point")
	}
}

func stackCPUTime() int64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	return (int64(u.Utime.Sec)+int64(u.Stime.Sec))*1e9 + (int64(u.Utime.Usec)+int64(u.Stime.Usec))*1e3
}

// Whole live heap includes names, items, staging, map slack and point buffers.
// Measure a normal one-point or a ten-point staging window with 100 cached
// points per series. No disk fixture is needed for this retained-state check.
func BenchmarkStackHeap(b *testing.B) {
	for _, staged := range []int{1, 10} {
		b.Run(fmt.Sprintf("staged=%d", staged), func(b *testing.B) {
			const metrics, n = 100000, 100
			data := compactWorkload("mix", n)
			for range b.N {
				runtime.GC()
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				c := New()
				c.SetMaxSize(0)
				c.EnableInputLog()
				for i := 0; i < metrics; i++ {
					name := fmt.Sprintf("namespace.component.instance.%08d.counter.value", i) + strings.Repeat(".segment", 16)
					row := data[i%len(data)]
					c.AddRecovered(&points.Points{Metric: name, Data: row[:n-staged]})
					c.AddPoints([]byte(name), row[n-staged:])
				}
				if c.Size() != metrics*n || c.LogDropped() != 0 {
					b.Fatal("incomplete fill")
				}
				runtime.GC()
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(metrics*n), "heap-B/point")
				runtime.KeepAlive(c)
			}
		})
	}
}
