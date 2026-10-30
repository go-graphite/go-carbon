package cache

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// Compare incremental accounting to the owned objects through growth, map
// rehash, failed writes, live arrivals while in flight, and confirmation.
func TestCacheMemoryAccounting(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.SetMaxBytes(1 << 40)
	c.EnableInputLog()
	check := func() {
		t.Helper()
		var exact, tracked, published int64
		for _, sh := range c.data {
			var items int64
			for _, it := range sh.items.all {
				items += it.memoryBytes()
			}
			if items != sh.items.bytes {
				t.Fatalf("items account for %d, want %d", sh.items.bytes, items)
			}
			exact += sh.memoryBytes()
			tracked += sh.byteSize
			published += sh.byteSize - sh.byteDelta
		}
		if exact != tracked {
			t.Fatalf("tracked bytes %d, want %d", tracked, exact)
		}
		if got := c.stat.bytes.Load(); got != published {
			t.Fatalf("published bytes %d, want %d", got, published)
		}
		if got := c.EstimatedBytes(); got != exact+c.queueMemory() {
			t.Fatalf("estimated %d, want %d", got, exact+c.queueMemory())
		}
	}
	for i := range 256 {
		name := fmt.Sprintf("memory.%d.%s", i, strings.Repeat("long.name.", i%16))
		for j := range 20 {
			c.AddPoint([]byte(name), float64(i+j), int64(j))
		}
		p, ok := c.PopNotConfirmed(name)
		if !ok {
			t.Fatal("missing batch")
		}
		c.AddPoint([]byte(name), 3, 100)
		c.Requeue(p)
		check()
		p, _ = c.PopNotConfirmed(name)
		check()
		c.Confirm(p)
		c.Confirm(p)
		check()
	}
	c.DisableInputLog()
	check()
}

func TestCacheByteLimit(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	if err := c.SetWriteoutBatching(1000, time.Hour); err != nil {
		t.Fatal(err)
	}
	base := c.EstimatedBytes()
	c.SetMaxBytes(uint64(base + 4000))
	name := strings.Repeat("long.metric.", 512)
	c.AddPoint([]byte(name), 1, 100)
	if !c.Has(name) || !c.WriteoutReady(name) {
		t.Fatal("byte pressure did not bypass batching")
	}
	if c.HasRecoveryCapacity(1) {
		t.Fatal("recovery ignored byte pressure")
	}
	c.AddPoint([]byte(name), 2, 101)
	c.Add(points.OnePoint("another", 1, 100))
	if c.Size() != 1 {
		t.Fatal("byte limit did not stop live admission")
	}
	p, ok := c.PopNotConfirmed(name)
	if !ok {
		t.Fatal("pressured batch could not drain")
	}
	if c.HasRecoveryCapacity(1) {
		t.Fatal("recovery ignored in-flight bytes")
	}
	c.Confirm(p)
	if !c.HasRecoveryCapacity(1) {
		t.Fatal("empty recovery could not progress")
	}
	c.SetMaxBytes(0)
	c.Add(points.OnePoint("another", 1, 100))
	if !c.Has("another") {
		t.Fatal("disabling byte limit did not resume admission")
	}
}

func TestCacheByteAccountingConcurrent(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.SetMaxBytes(16 << 20)
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			name := fmt.Sprintf("concurrent.%d.%s", worker, strings.Repeat("name.", 64))
			for i := range 128 {
				c.AddPoint([]byte(name), float64(i), int64(i))
				p, ok := c.PopNotConfirmed(name)
				if !ok {
					t.Error("missing batch below byte limit")
					return
				}
				c.Confirm(p)
			}
		})
	}
	wg.Wait()
	accounted := c.stat.bytes.Load()
	for _, sh := range c.data {
		accounted += sh.byteDelta
	}
	if got := c.EstimatedBytes(); got != accounted {
		t.Fatalf("concurrent estimate %d, accounted %d", got, accounted)
	}
}

func TestDeferredQueueRetainsOnlyReadyScratch(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	if err := c.SetWriteoutBatching(2, time.Hour); err != nil {
		t.Fatal(err)
	}
	for i := range 4096 {
		c.AddPoint([]byte(fmt.Sprintf("deferred.%d", i)), 1, 100)
	}
	if q, _ := c.makeQueueAt(time.Now()); q != nil {
		t.Fatal("deferred points queued early")
	}
	if scratch := c.queueScratch.Load(); scratch != nil && cap(*scratch) > 1024 {
		t.Fatalf("scratch reserved for %d deferred metrics", cap(*scratch))
	}
}

func BenchmarkDeferredQueue(b *testing.B) {
	for _, ready := range []int{0, 1000} {
		b.Run(fmt.Sprintf("ready=%d", ready), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			if err := c.SetWriteoutBatching(2, time.Hour); err != nil {
				b.Fatal(err)
			}
			for i := range 100000 {
				name := []byte(fmt.Sprintf("queue.memory.%06d", i))
				c.AddPoint(name, 1, 100)
				if i < ready {
					c.AddPoint(name, 2, 101)
				}
			}
			c.makeQueueAt(time.Now())
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				c.makeQueueAt(time.Now())
			}
			b.StopTimer()
			if scratch := c.queueScratch.Load(); scratch != nil {
				b.ReportMetric(float64(cap(*scratch))*24/100000, "scratch-B/metric")
			}
		})
	}
}
