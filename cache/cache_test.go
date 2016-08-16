package cache

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	c := New()
	assert := assert.New(t)

	c.Add(points.OnePoint("hello.world", 42, 10))
	assert.Equal(1, int(c.Size()), "Size of cache didn't go up when point was added")

	c.Add(points.OnePoint("hello.world", 15, 12))
	assert.Equal(2, int(c.Size()), "Size of cache didn't go up to 2 when point was added")

	values, ok := c.GetMetric("hello.world")
	assert.True(ok, "Can't get metric which we just added to cache")
	assert.Equal("hello.world", values.Metric, "Retrieved metric name is not one we added")
	assert.Equal(2, len(values.Data), "Retrieved metric doesn't have expected number of datapoints")
}

var cache *Cache

func createCacheAndPopulate(metricsCount int, maxPointsPerMetric int) *Cache {
	if cache != nil {
		return cache
	}
	cache = New()
	for i := 0; i < metricsCount; i++ {
		m := fmt.Sprintf("%d.metric.name.for.bench.test.%d", rand.Intn(metricsCount), i)

		p := points.OnePoint(m, 0, int64(i))
		for j := 0; j < rand.Intn(maxPointsPerMetric)+1; j++ {
			p.Add(0, int64(i+j))
		}
		cache.Add(p)
	}
	cache.updateQueue() // warmup
	return cache
}

var gp points.Points

func benchmarkStrategy(b *testing.B, strategy string) {
	cache := createCacheAndPopulate(1000*1000, 100)
	if err := cache.SetWriteStrategy(strategy); err != nil {
		b.Errorf("Can't set strategy %s", strategy)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.updateQueue()
	}
}

func BenchmarkUpdateQueueMax(b *testing.B)  { benchmarkStrategy(b, "max") }
func BenchmarkUpdateQueueSort(b *testing.B) { benchmarkStrategy(b, "sort") }
func BenchmarkUpdateQueueNoop(b *testing.B) { benchmarkStrategy(b, "noop") }
