package cache

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/lomik/go-carbon/points"
)

func TestCache(t *testing.T) {

	c := New(nil)

	c.Add(points.OnePoint("hello.world", 42, 10))

	if c.Size() != 1 {
		t.FailNow()
	}

	c.Add(points.OnePoint("hello.world", 15, 12))

	if c.Size() != 2 {
		t.FailNow()
	}

	q := c.WriteoutQueue()
	values := q.Get(nil)

	if values.Metric != "hello.world" {
		t.FailNow()
	}

	if len(values.Data) != 2 {
		t.FailNow()
	}

	if c.Size() != 0 {
		t.FailNow()
	}
}

var cache *Cache

func createCacheAndPopulate(metricsCount int, maxPointsPerMetric int) *Cache {
	if cache != nil {
		return cache
	}
	cache = New(nil)

	for i := 0; i < metricsCount; i++ {
		m := fmt.Sprintf("%d.metric.name.for.bench.test.%d", rand.Intn(metricsCount), i)

		p := points.OnePoint(m, 0, int64(i))
		for j := 0; j < rand.Intn(maxPointsPerMetric)+1; j++ {
			p.Add(0, int64(i+j))
		}
		cache.Add(p)
	}
	cache.makeQueue() // warmup
	return cache
}

var gp *points.Points

func benchmarkStrategy(b *testing.B, strategy string) {
	cache := createCacheAndPopulate(1000*1000, 100)
	if err := cache.SetWriteStrategy(strategy); err != nil {
		b.Errorf("Can't set strategy %s", strategy)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.makeQueue()
	}
}

func BenchmarkUpdateQueueMax(b *testing.B)  { benchmarkStrategy(b, "max") }
func BenchmarkUpdateQueueSort(b *testing.B) { benchmarkStrategy(b, "sort") }
func BenchmarkUpdateQueueNoop(b *testing.B) { benchmarkStrategy(b, "noop") }
