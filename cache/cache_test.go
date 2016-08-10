package cache

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
)

func TestCache(t *testing.T) {

	c := New()

	c.Add(points.OnePoint("hello.world", 42, 10))

	if c.Size() != 1 {
		t.FailNow()
	}

	c.Add(points.OnePoint("hello.world", 15, 12))

	if c.Size() != 2 {
		t.FailNow()
	}

	values := c.Pop()

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

func TestCacheCheckpoint(t *testing.T) {
	t.SkipNow() // @TODO
	cache := New()
	cache.Start()
	cache.SetOutputChanSize(0)

	defer cache.Stop()

	startTime := time.Now().Unix() - 60*60

	sizes := []int{1, 15, 42, 56, 22, 90, 1}

	for index, value := range sizes {
		metricName := fmt.Sprintf("metric%d", index)

		for i := value; i > 0; i-- {
			cache.In() <- points.OnePoint(metricName, float64(i), startTime+int64(i))
		}

	}

	time.Sleep(100 * time.Millisecond)
	cache.doCheckpoint()

	d := <-cache.Out()
	if d.Metric != "metric0" {
		t.Fatal("wrong metric received")
	}

	systemMetrics := []string{
		"carbon.cache.inputLenAfterCheckpoint",
		"carbon.cache.inputLenBeforeCheckpoint",
		"carbon.cache.checkpointTime",
		"carbon.cache.overflow",
		"carbon.cache.queries",
		"carbon.cache.metrics",
		"carbon.cache.size",
	}

	for _, metricName := range systemMetrics {
		d = <-cache.Out()
		if d.Metric != metricName {
			t.Fatalf("%#v != %#v", d.Metric, metricName)
		}
	}

	result := sizes[1:]
	sort.Sort(sort.Reverse(sort.IntSlice(result)))

	for _, size := range result {
		d = <-cache.Out()
		if len(d.Data) != size {
			t.Fatalf("wrong metric received. Waiting metric with %d points, received with %d", size, len(d.Data))
		}
	}
}

var cache *Cache

func createCacheAndPopulate(metricsCount int, maxPointsPerMetric int) *Cache {
	if cache != nil {
		return cache
	}
	cache = New()
	cache.SetOutputChanSize(0)

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

var gp *points.Points

func benchmarkStrategy(b *testing.B, strategy string) {
	cache := createCacheAndPopulate(1000*1000, 100)
	if err := cache.SetWriteStrategy(strategy); err != nil {
		b.Errorf("Can't set strategy %s", strategy)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.updateQueue()
                for {
                    p := cache.getNext()
                    gp = p  // assign to package level var to avoid compiler optimizing us out
                    if p == nil { break }
                }
	}
}

func BenchmarkUpdateQueueMax(b *testing.B)  { benchmarkStrategy(b, "max") }
func BenchmarkUpdateQueueSort(b *testing.B) { benchmarkStrategy(b, "sort") }
func BenchmarkUpdateQueueNoop(b *testing.B) { benchmarkStrategy(b, "noop") }
