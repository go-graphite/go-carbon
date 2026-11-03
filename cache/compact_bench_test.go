package cache

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"testing"
	"unsafe"

	testseries "github.com/go-graphite/go-carbon/internal/testutil/series"
	"github.com/go-graphite/go-carbon/points"
)

// compactWorkload reuses the four shapes in receiver/stream/codec's
// syntheticValue: counters, hundredth-rounded gauges, stable gauges and
// full-mantissa floats. The mix is 50/20/20/10 (70% integer series).
// Half the counters use newSeries' variable increments rather than +100.
// Timestamp anomalies come from newSeries, at steps of 1 or 60 seconds.
func compactWorkload(kind string, n int) [][]points.Point {
	count := 100
	if kind != "mix" && kind != "float30" {
		count = 10
	}
	data := make([][]points.Point, count)
	for id := range data {
		g := newSeries(uint64(id))
		g.step = 60
		if id%2 == 0 {
			g.step = 1
		}
		g.kind, g.value = counterSeries, float64(id*1000000)
		shape := kind
		if shape == "mix" || shape == "float30" {
			switch {
			case id < 50:
				shape = "counter"
			case id < 70:
				shape = "stable"
			case id < 90 && kind != "float30":
				shape = "rounded"
			default:
				shape = "float"
			}
		}
		data[id] = make([]points.Point, n)
		for epoch := range data[id] {
			variable, ts := g.next()
			var v float64
			switch shape {
			case "counter":
				v = testseries.Value(0, id, epoch)
				if id%2 == 0 {
					v = variable
				}
			case "rounded":
				v = testseries.Value(1, id, epoch)
			case "stable":
				v = testseries.Value(2, id, epoch)
			case "float":
				v = testseries.Value(3, id, epoch)
			default:
				panic(shape)
			}
			data[id][epoch] = points.Point{Value: v, Timestamp: ts}
		}
	}
	return data
}

func BenchmarkCompactWorkload(b *testing.B) {
	for _, n := range []int{100, 4096} {
		for _, kind := range []string{"mix", "counter", "stable", "rounded", "float"} {
			data := compactWorkload(kind, n)
			encoded := make([]compactPoints, len(data))
			var used, retained, maxCapacity int
			for i := range data {
				encoded[i].appendPoints(data[i])
				used += encoded[i].encodedBytes()
				retained += cap(encoded[i].buf)
				maxCapacity = max(maxCapacity, cap(encoded[i].buf))
			}
			for _, mode := range []string{"append", "fresh", "decode", "read"} {
				b.Run(fmt.Sprintf("%s/n=%d/%s", kind, n, mode), func(b *testing.B) {
					b.ReportAllocs()
					out := make([]points.Point, 0, n)
					c := compactPoints{buf: make([]byte, 0, maxCapacity)}
					b.ResetTimer()
					cpuStart := compactCPUTime()
					for i := 0; i < b.N; i++ {
						j := i % len(data)
						switch mode {
						case "append":
							c = compactPoints{buf: c.buf[:0]}
							c.appendPoints(data[j])
						case "fresh":
							c = compactPoints{}
							c.appendPoints(data[j])
						case "decode":
							out = encoded[j].appendDecoded(out[:0])
						case "read":
							out = encoded[j].appendDecoded(nil)
						}
					}
					b.StopTimer()
					cpuNanos := compactCPUTime() - cpuStart
					runtime.KeepAlive(c)
					runtime.KeepAlive(out)
					total := float64(len(data) * n)
					b.ReportMetric(float64(used)/total, "encoded-B/point")
					b.ReportMetric(float64(retained)/total, "capacity-B/point")
					b.ReportMetric(float64(retained+len(data)*int(unsafe.Sizeof(compactPoints{})))/total, "state+capacity-B/point")
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
					if cpuNanos > 0 {
						b.ReportMetric(float64(cpuNanos)/float64(b.N*n), "cpu-ns/point")
					}
				})
			}
		}
	}
}

// Includes allocator rounding, and reports the whole cache separately from
// the point buffers. Set CACHE_COMPACT_MEMORY=1 for an offline measurement.
func TestCompactWorkloadHeap(t *testing.T) {
	if os.Getenv("CACHE_COMPACT_MEMORY") == "" {
		t.Skip("set CACHE_COMPACT_MEMORY=1")
	}
	const metrics = 100000
	for _, kind := range []string{"mix", "float30"} {
		data := compactWorkload(kind, 209)
		for _, variable := range []bool{false, true} {
			lengths := make([]int, metrics)
			names := make([][]byte, metrics)
			total := 0
			for i := range lengths {
				lengths[i] = 100
				if variable {
					lengths[i] = 1 + (i*73)%209
				}
				total += lengths[i]
				names[i] = []byte(fmt.Sprintf("example.metric.%06d", i))
			}
			enc := make([]compactPoints, metrics)
			before := heapInUse()
			for i := range enc {
				enc[i].appendPoints(data[i%len(data)][:lengths[i]])
			}
			after := heapInUse()
			capacity, encoded := 0, 0
			for i := range enc {
				capacity += cap(enc[i].buf)
				encoded += enc[i].encodedBytes()
			}
			t.Logf("kind=%s variable=%v points=%d encoded=%.4f capacity=%.4f buffer-heap=%.4f state+buffer-heap=%.4f B/point",
				kind, variable, total, float64(encoded)/float64(total), float64(capacity)/float64(total), float64(after-before)/float64(total), float64(after-before+uint64(metrics)*uint64(unsafe.Sizeof(compactPoints{})))/float64(total))
			if kind == "mix" && float64(after-before+8*metrics)/float64(total) > 1.90 {
				t.Fatal("point buffers plus the entire partial word exceed 1.90 B/point")
			}
			runtime.KeepAlive(enc)
			before = heapInUse()
			c := New()
			c.SetMaxSize(0)
			for round := 0; round < 209; round++ {
				for i, name := range names {
					if round < lengths[i] {
						p := data[i%len(data)][round]
						c.AddPoint(name, p.Value, p.Timestamp)
					}
				}
			}
			after = heapInUse()
			if c.Size() != int64(total) {
				t.Fatal("cache count differs")
			}
			t.Logf("kind=%s variable=%v whole-cache-heap=%.4f B/point estimated=%.4f B/point", kind, variable, float64(after-before)/float64(total), float64(c.EstimatedBytes())/float64(total))
			runtime.KeepAlive(c)
			runtime.KeepAlive(names)
			runtime.KeepAlive(data)
			runtime.KeepAlive(lengths)
		}
	}
}

// One operation is the lifetime of one metric at the actual cache boundary,
// including single-point ingestion, decoding at writeout and confirmation.
func BenchmarkCompactCacheCycle(b *testing.B) {
	for _, n := range []int{100, 4096} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			data := compactWorkload("mix", n)
			names := make([][]byte, 1000)
			for i := range names {
				names[i] = []byte(fmt.Sprintf("example.metric.%06d", i))
			}
			cycle := func(i int) {
				name := names[i%len(names)]
				for _, p := range data[i%len(data)] {
					c.AddPoint(name, p.Value, p.Timestamp)
				}
				p, ok := c.PopNotConfirmed(string(name))
				if !ok || len(p.Data) != n {
					b.Fatal("writeout differs")
				}
				c.Confirm(p)
			}
			// Seed the lower-quartile hint a running process learns. Relearning
			// it from millions of writeouts at each calibration wastes CPU.
			lengths := make([]int, len(data))
			for i, p := range data {
				enc := encodePoints(p)
				lengths[i] = len(enc.buf)
			}
			slices.Sort(lengths)
			for _, shard := range c.data {
				shard.bytesQuarters = 4 * min(maxBytesHint, max(minBytesHint, lengths[len(lengths)/4]))
			}
			for i := 0; i < 1000; i++ {
				cycle(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			cpuStart := compactCPUTime()
			for i := 0; i < b.N; i++ {
				cycle(i)
			}
			b.StopTimer()
			cpuNanos := compactCPUTime() - cpuStart
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
			if cpuNanos > 0 {
				b.ReportMetric(float64(cpuNanos)/float64(b.N*n), "cpu-ns/point")
			}
		})
	}
}

func BenchmarkCompactDumpBoundary(b *testing.B) {
	const metrics = 1000
	for _, n := range []int{100, 4096} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			data := compactWorkload("mix", n)
			for i := 0; i < metrics; i++ {
				c.Add(&points.Points{Metric: fmt.Sprintf("example.metric.%06d", i), Data: data[i%len(data)]})
			}
			var bytes int
			visit := func(_ string, data []byte, _ int) error { bytes += len(data); return nil }
			b.ReportAllocs()
			b.ResetTimer()
			cpuStart := compactCPUTime()
			for i := 0; i < b.N; i++ {
				if err := c.DumpShardsCompact(0, ShardCount, visit); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			cpuNanos := compactCPUTime() - cpuStart
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n*metrics), "ns/point")
			if cpuNanos > 0 {
				b.ReportMetric(float64(cpuNanos)/float64(b.N*n*metrics), "cpu-ns/point")
			}
			runtime.KeepAlive(bytes)
		})
	}
}
