package cache

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

var ingestFixture struct {
	sync.Once
	cache *Cache
	names []string
}

// Opt in: INGEST_BENCH_METRICS=20000000. Each lookup copies a name into a
// reused input buffer, as a receiver does, rather than comparing interned keys.
func largeIngestCache(b *testing.B) (*Cache, []string, int) {
	n, _ := strconv.Atoi(os.Getenv("INGEST_BENCH_METRICS"))
	if n == 0 {
		b.Skip("set INGEST_BENCH_METRICS (20000000 for the production-size map)")
	}
	ingestFixture.Do(func() {
		c := New()
		c.SetMaxSize(0)
		ingestFixture.cache = c
		ingestFixture.names = make([]string, n)
		for i := range n {
			name := fmt.Sprintf("services.namespace-%04d.host-%08d.component.requests.region.zone.%s.value", i%1000, i, strings.Repeat("x", 8+i%71))
			ingestFixture.names[i] = name
			c.Add(points.OnePoint(name, 1, 1_790_000_000))
		}
		runtime.GC()
	})
	return ingestFixture.cache, ingestFixture.names, n
}

func BenchmarkAddPointLarge(b *testing.B) {
	c, names, n := largeIngestCache(b)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	b.ReportAllocs()
	b.ResetTimer()
	var buf [160]byte
	state := uint64(1)
	for i := 0; i < b.N; i++ {
		state = state*6364136223846793005 + 1
		name := names[(state>>32)%uint64(len(names))]
		copy(buf[:], name)
		c.AddPoint(buf[:len(name)], 1, 1_790_000_060+int64(i/n))
	}
	b.StopTimer()
	b.ReportMetric(float64(mem.HeapAlloc)/float64(n), "heap-B/metric")
	b.ReportMetric(float64(reflect.TypeFor[item]().Size()), "item-B")
}

func BenchmarkWriteoutDeadline(b *testing.B) {
	c := New()
	c.SetMaxSize(0)
	c.SetWriteoutBatching(960, 960*time.Second)
	c.SetWriteoutSpread(.2)
	const n = 100000
	for i := range n {
		c.AddPoint([]byte(fmt.Sprintf("deadline.metric.%d", i)), 1, 1)
	}
	var items []*item
	for _, shard := range c.data {
		for _, it := range shard.items.all {
			items = append(items, it)
		}
	}
	s := c.settings.Load().(*cacheSettings)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := items[i%n]
		if writeoutDeadline(it, s).IsZero() {
			b.Fatal("missing deadline")
		}
	}
}

// Keep the original pointer-map implementation beside the new one, with
// identical inputs, to isolate the lookup from queue building and sharding.
func BenchmarkDeadlineLookup(b *testing.B) {
	const n = 100000
	s := &cacheSettings{writeoutMinPoints: 960, writeoutMaxDelay: 960 * time.Second, writeoutSpread: .2}
	items := make([]item, n)
	first := make(map[*points.Points]time.Time, n)
	for i := range items {
		items[i].firstArrival = time.Unix(1790000000+int64(i), 0)
		items[i].spread = uint16(i)
		first[&items[i].Points] = items[i].firstArrival
	}
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("map=%t", old), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				it := &items[i%n]
				var deadline time.Time
				if old {
					at := first[&it.Points]
					if it.count() < s.minPointsFor(it) && !at.IsZero() {
						deadline = at.Add(s.delayFor(it))
					}
				} else {
					deadline = writeoutDeadline(it, s)
				}
				if deadline.IsZero() {
					b.Fatal("missing deadline")
				}
			}
		})
	}
}

func BenchmarkCachedNotification(b *testing.B) {
	c := New()
	c.SetMaxSize(0)
	c.SetBloomSize(50000000)
	c.SetNewMetricsChan(make(chan string, 1))
	c.SetMetricExists(func(string) bool { return true })
	names := make([][]byte, 16384)
	for i := range names {
		names[i] = []byte(fmt.Sprintf("services.namespace.host-%08d.component.requests.region.zone.long-metric-name.value", i))
		c.AddPoint(names[i], 1, 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AddPoint(names[i%len(names)], 1, 1)
	}
}

func BenchmarkAddPointsLarge(b *testing.B) {
	c, names, _ := largeIngestCache(b)
	out := func(name []byte, data []points.Point) {
		for _, p := range data {
			c.AddPoint(name, p.Value, p.Timestamp)
		}
	}
	if target, ok := any(c).(interface{ AddPoints([]byte, []points.Point) }); ok {
		out = target.AddPoints
	}
	for _, n := range []int{1, 4, 16, 32} {
		b.Run(fmt.Sprintf("points=%d", n), func(b *testing.B) {
			data := make([]points.Point, n)
			for i := range data {
				data[i] = points.Point{Value: 1, Timestamp: 1790000060}
			}
			var buf [160]byte
			state := uint64(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				state = state*6364136223846793005 + 1
				name := names[(state>>32)%uint64(len(names))]
				copy(buf[:], name)
				out(buf[:len(name)], data)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
		})
	}
}
