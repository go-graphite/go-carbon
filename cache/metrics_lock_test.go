package cache

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// Count the owned objects independently of both published and pending totals.
// Exercise growth, batched inserts, eviction, failed writes and confirmation,
// with and without the optional byte budget and input log.
func TestCacheMetricsMatchLockedCensus(t *testing.T) {
	for _, logged := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "logged"}[logged], func(t *testing.T) {
			c := New()
			c.SetMaxSize(1 << 20) // Small slack exercises positive/negative publication.
			if logged {
				c.SetMaxBytes(1 << 40)
				c.EnableInputLog()
			}
			check := func() {
				t.Helper()
				var size, published, memory, pointBytes int64
				var metrics, flight int32
				for i, shard := range c.data {
					shard.mu.RLock()
					var count int64
					for _, it := range shard.items.all {
						count += int64(it.count())
					}
					accounted := shard.sizePublished + shard.sizeDelta
					size += count
					published += shard.sizePublished
					metrics += int32(shard.items.len())
					flight += int32(shard.notConfirmedUsed)
					memory += shard.memoryBytes()
					pointBytes += shard.items.pointBytes
					shard.mu.RUnlock()
					if accounted != count {
						t.Fatalf("shard %d accounting %d, census %d", i, accounted, count)
					}
				}
				if c.approxSize() != published || c.Size() != size || c.Len() != metrics || c.NotConfirmedLength() != flight || c.EstimatedBytes() != memory+c.queueMemory() || c.PointBytes() != pointBytes {
					t.Fatalf("published %d/%d, points %d/%d, metrics %d/%d, in-flight %d/%d, memory %d/%d, point bytes %d/%d",
						c.approxSize(), published, c.Size(), size, c.Len(), metrics, c.NotConfirmedLength(), flight, c.EstimatedBytes(), memory+c.queueMemory(), c.PointBytes(), pointBytes)
				}
			}
			check()
			pointBytesOperations(c, 71, 300, check)
			c.AddRecovered(&points.Points{Metric: "oversized", Data: make([]points.Point, 10_000)})
			check()
			c.Pop("oversized")
			check()
		})
	}
}

func TestCacheMetricsDoNotWaitForShard(t *testing.T) {
	c := New()
	c.Add(points.OnePoint("metrics.flight", 1, 1))
	c.PopNotConfirmed("metrics.flight")
	c.Add(points.OnePoint("metrics.queued", 2, 2))
	c.ResolveStreamName([]byte("metrics.queued"))

	for _, test := range []struct {
		name string
		read func() any
	}{
		{"Size", func() any { return c.Size() }},
		{"Len", func() any { return c.Len() }},
		{"NotConfirmedLength", func() any { return c.NotConfirmedLength() }},
		{"EstimatedBytes", func() any { return c.EstimatedBytes() }},
		{"PointBytes", func() any { return c.PointBytes() }},
		{"StreamSeriesIndexBytes", func() any { return c.StreamSeriesIndexBytes() }},
		{"Stat", func() any {
			values := make(map[string]float64)
			c.Stat(func(name string, value float64) { values[name] = value })
			return values
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := test.read()
			shard := c.GetShard("metrics.queued")
			shard.mu.Lock()
			done := make(chan any, 1)
			go func() { done <- test.read() }()
			select {
			case got := <-done:
				shard.mu.Unlock()
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			case <-time.After(time.Second):
				shard.mu.Unlock()
				<-done
				t.Fatal("cache metrics waited for a shard write lock")
			}
		})
	}
}
