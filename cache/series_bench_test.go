package cache

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// Value kinds of synthetic Graphite series.
const (
	constantSeries = iota // the same value every time, often 0 or 1
	counterSeries         // an integer that grows by small steps
	gaugeSeries           // a small integer that often repeats
	floatSeries           // a rate or mean: an arbitrary float
	seriesKinds
)

// series generates one metric's points: mostly regular steps, with rare
// jitter, duplicates and out-of-order timestamps.
type series struct {
	rng   *rand.Rand
	kind  int
	step  int64
	ts    int64
	value float64
}

func newSeries(seed uint64) *series {
	rng := rand.New(rand.NewPCG(seed, 0x5eed))
	g := &series{rng: rng, ts: 1_760_000_000 + rng.Int64N(60)}
	// A quarter constant, a quarter counters, a fifth gauges, the rest floats.
	switch n := rng.IntN(20); {
	case n < 5:
		g.kind, g.value = constantSeries, float64(rng.IntN(2))
	case n < 10:
		g.kind, g.value = counterSeries, float64(rng.Int64N(1<<40))
	case n < 14:
		g.kind, g.value = gaugeSeries, float64(rng.IntN(100))
	default:
		g.kind, g.value = floatSeries, rng.Float64()*1000
	}
	switch n := rng.IntN(10); {
	case n < 1:
		g.step = 1
	case n < 4:
		g.step = 10
	default:
		g.step = 60
	}
	return g
}

func (g *series) next() (float64, int64) {
	g.ts += g.step
	ts := g.ts
	switch n := g.rng.IntN(1000); {
	case n < 2:
		ts -= 3 * g.step // out of order
	case n < 4:
		ts -= g.step // usually a duplicate of the previous timestamp
	case n < 24:
		ts += int64(g.rng.IntN(3)) - 1 // jitter
	}
	switch g.kind {
	case counterSeries:
		g.value += float64(g.rng.IntN(100))
	case gaugeSeries:
		if g.rng.IntN(4) == 0 {
			g.value += float64(g.rng.IntN(5) - 2)
		}
	case floatSeries:
		g.value = g.rng.Float64() * 1000
	}
	return g.value, ts
}

func seriesName(i int) string {
	return fmt.Sprintf("apps.service-%03d.host-%05d.requests.%s.count", i%997, i, strconv.Itoa(i%7))
}

// heapInUse returns live heap bytes after collecting garbage.
func heapInUse() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func envInts(t *testing.T, name, fallback string) []int {
	value := os.Getenv(name)
	if value == "" {
		value = fallback
	}
	var out []int
	for _, field := range strings.Split(value, ",") {
		n, err := strconv.Atoi(field)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out = append(out, n)
	}
	return out
}

// TestCacheMemoryPerPoint reports live heap per cached point. It is a manual
// measurement: CACHE_MEMORY_REPORT=1 go test -run CacheMemoryPerPoint ./cache/
// Points arrive time-major, as from live clients. With steady=1 each metric
// stops at a random length, as metrics do between writeouts. With steady=2
// it holds a random part of what arrives in points/metric seconds, as with a
// writeout-max-delay of that many seconds: a 60-second series holds at most
// 8 of 480 points.
func TestCacheMemoryPerPoint(t *testing.T) {
	if os.Getenv("CACHE_MEMORY_REPORT") == "" {
		t.Skip("set CACHE_MEMORY_REPORT=1")
	}
	metrics := envInts(t, "CACHE_MEMORY_METRICS", "1000000")[0]
	for _, perMetric := range envInts(t, "CACHE_MEMORY_POINTS", "50,160,480,960") {
		for _, steady := range envInts(t, "CACHE_MEMORY_STEADY", "0,1") {
			names := make([][]byte, metrics)
			gens := make([]*series, metrics)
			lengths := make([]int, metrics)
			rng := rand.New(rand.NewPCG(uint64(perMetric), 1))
			for i := range names {
				names[i] = []byte(seriesName(i))
				gens[i] = newSeries(uint64(i))
				switch lengths[i] = perMetric; steady {
				case 1:
					lengths[i] = 1 + rng.IntN(perMetric)
				case 2:
					lengths[i] = 1 + rng.IntN(max(perMetric/int(gens[i].step), 1))
				}
			}
			before := heapInUse()
			c := New()
			c.SetMaxSize(0)
			total := 0
			for round := 0; round < perMetric; round++ {
				for i, name := range names {
					if round < lengths[i] {
						value, ts := gens[i].next()
						c.AddPoint(name, value, ts)
						total++
					}
				}
			}
			after := heapInUse()
			if got := c.Size(); got != int64(total) {
				t.Fatalf("size %d, want %d", got, total)
			}
			used := float64(after - before)
			t.Logf("metrics=%d points/metric=%d steady=%d points=%d heap=%.2f GB bytes/point=%.2f bytes/metric=%.0f",
				metrics, perMetric, steady, total, used/1e9, used/float64(total), used/float64(metrics))
			runtime.KeepAlive(c)
			// The inputs were live before the fill, so they stay live after.
			runtime.KeepAlive(names)
			runtime.KeepAlive(gens)
			runtime.KeepAlive(lengths)
		}
	}
}

// fillSeries adds perMetric points to each of metrics series.
func fillSeries(c *Cache, names []string, gens []*series, perMetric int) {
	for round := 0; round < perMetric; round++ {
		for i, name := range names {
			value, ts := gens[i].next()
			c.AddPoint([]byte(name), value, ts)
		}
	}
}

func seriesSet(n, seed int) ([]string, []*series) {
	names := make([]string, n)
	gens := make([]*series, n)
	for i := range names {
		names[i] = seriesName(seed*n + i)
		gens[i] = newSeries(uint64(seed*n + i))
	}
	return names, gens
}

// BenchmarkAddPointParallelSeries writes synthetic series from every
// goroutine. Each metric is popped and confirmed once it holds writeout
// points, as the persister would, so about a tenth of a percent of calls
// start a new item.
func BenchmarkAddPointParallelSeries(b *testing.B) {
	c := New()
	c.SetMaxSize(0)
	b.ReportAllocs()
	addPointParallel(b, c)
}

var resident struct {
	once  sync.Once
	cache *Cache
}

// BenchmarkAddPointResident adds the same way to a cache that also holds a
// million metrics of 160 points, so collections run against a production-like
// live heap. It reports collector CPU time per operation.
func BenchmarkAddPointResident(b *testing.B) {
	resident.once.Do(func() {
		resident.cache = New()
		resident.cache.SetMaxSize(0)
		names, gens := seriesSet(1_000_000, 1000)
		fillSeries(resident.cache, names, gens, 160)
	})
	runtime.GC()
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/gc/cycles/total:gc-cycles"}}
	metrics.Read(samples)
	gcSeconds, cycles := samples[0].Value.Float64(), samples[1].Value.Uint64()
	b.ReportAllocs()
	b.ResetTimer()
	addPointParallel(b, resident.cache)
	b.StopTimer()
	metrics.Read(samples)
	b.ReportMetric((samples[0].Value.Float64()-gcSeconds)*1e9/float64(b.N), "gc-ns/op")
	b.ReportMetric(float64(samples[1].Value.Uint64()-cycles)*1e6/float64(b.N), "gcs/Mop")
}

func addPointParallel(b *testing.B, c *Cache) {
	const writeout = 960
	var worker atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		names, gens := seriesSet(4096, int(worker.Add(1)))
		keys := make([][]byte, len(names))
		for i, name := range names {
			keys[i] = []byte(name)
		}
		counts := make([]int, len(names))
		for i := 0; pb.Next(); i++ {
			m := i % len(names)
			value, ts := gens[m].next()
			c.AddPoint(keys[m], value, ts)
			if counts[m]++; counts[m] == writeout {
				counts[m] = 0
				if p, ok := c.PopNotConfirmed(names[m]); ok {
					c.Confirm(p)
				}
			}
		}
	})
}

// BenchmarkPopSeries measures one PopNotConfirmed and Confirm of a metric
// holding the given number of points.
func BenchmarkPopSeries(b *testing.B) {
	for _, perMetric := range []int{50, 160, 480, 960} {
		b.Run(fmt.Sprintf("points=%d", perMetric), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			names, gens := seriesSet(8192, 0)
			templates := make([][]points.Point, len(names))
			for i := range templates {
				for j := 0; j < perMetric; j++ {
					value, ts := gens[i].next()
					templates[i] = append(templates[i], points.Point{Value: value, Timestamp: ts})
				}
			}
			refill := func() {
				for i, name := range names {
					c.Add(&points.Points{Metric: name, Data: slices.Clone(templates[i])})
				}
			}
			refill()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := i % len(names)
				if m == 0 && i > 0 {
					b.StopTimer()
					refill()
					b.StartTimer()
				}
				p, ok := c.PopNotConfirmed(names[m])
				if !ok || len(p.Data) != perMetric {
					b.Fatalf("popped %d points", len(p.Data))
				}
				c.Confirm(p)
			}
		})
	}
}

// BenchmarkGetSeries reads cached metrics from every goroutine.
func BenchmarkGetSeries(b *testing.B) {
	for _, perMetric := range []int{50, 160, 480, 960} {
		b.Run(fmt.Sprintf("points=%d", perMetric), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			names, gens := seriesSet(8192, 0)
			fillSeries(c, names, gens, perMetric)
			var worker atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for i := int(worker.Add(1)) * 7919; pb.Next(); i++ {
					if got := c.Get(names[i%len(names)]); len(got) != perMetric {
						b.Errorf("got %d points", len(got))
						return
					}
				}
			})
		})
	}
}

var benchSink float64

// BenchmarkSeriesNext is the generator's own cost, to subtract from the
// AddPoint benchmark.
func BenchmarkSeriesNext(b *testing.B) {
	g := newSeries(1)
	for i := 0; i < b.N; i++ {
		v, ts := g.next()
		benchSink += v + float64(ts)
	}
	if math.IsNaN(benchSink) {
		b.Fatal("NaN")
	}
}
