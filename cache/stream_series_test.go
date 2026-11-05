package cache

import (
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"
	"weak"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

func TestStreamSeriesSharesCacheNameAndDoesNotPinBatch(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("example.series")
	c.AddPoint(name, 1, 1)
	a := c.ResolveStreamName(name).(*StreamSeries)
	b := c.ResolveStreamName(append([]byte(nil), name...)).(*StreamSeries)
	if a != b {
		t.Fatal("definitions did not share a series identity")
	}
	sh := c.data[a.hash&(shardCount-1)]
	sh.mu.Lock()
	it := sh.items.getBytesHashed(a.hash, name)
	if unsafe.StringData(a.name) != unsafe.StringData(it.Metric) {
		t.Fatal("series copied a resident cache name")
	}
	old := weak.Make(it)
	it = nil
	sh.mu.Unlock()
	var slot any = a
	c.AddStreamPoint(name, 2, 2, &slot)
	if len(c.Get(string(name))) != 2 {
		t.Fatal("shared hint lost a point")
	}
	c.Pop(string(name))
	for range 10 {
		runtime.GC()
		if old.Value() == nil {
			break
		}
	}
	if old.Value() != nil {
		t.Fatal("wire series retained a popped cache batch")
	}
	c.AddStreamPoint(name, 3, 3, &slot)
	if got := c.Get(string(name)); len(got) != 1 || got[0].Value != 3 {
		t.Fatal("series did not re-resolve a removed cache entry")
	}
	runtime.KeepAlive(a)
	runtime.KeepAlive(slot)
}

func TestStreamGenerationExhaustionUsesOrdinaryLookup(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("example.exhausted")
	series := c.ResolveStreamName(name).(*StreamSeries)
	sh := c.data[series.hash&(shardCount-1)]
	sh.mu.Lock()
	sh.streamGeneration = ^uint64(0)
	sh.mu.Unlock()
	var slot any = series
	c.AddStreamPoint(name, 1, 1, &slot)
	if series.hint.Load() != 0 {
		t.Fatal("zero generation was published as a live hint")
	}
	c.Pop(string(name))
	c.AddStreamPoint(name, 2, 2, &slot)
	if got := c.Get(string(name)); len(got) != 1 || got[0].Value != 2 {
		t.Fatal("exhausted generations did not fall back safely")
	}
}

// A resident identity must remain resolvable while ingestion owns the cache
// lock. This is also a lock-order/liveness check for the independent index.
func TestStreamSeriesHitDoesNotTakeCacheLock(t *testing.T) {
	c := New()
	name := []byte("stream.lock.isolation")
	s := c.ResolveStreamName(name).(*StreamSeries)
	sh := c.data[s.hash&(shardCount-1)]
	sh.mu.Lock()
	done := make(chan any, 1)
	go func() { done <- c.ResolveStreamName(name) }()
	select {
	case got := <-done:
		sh.mu.Unlock()
		if got != s {
			t.Fatal("resident identity changed")
		}
	case <-time.After(time.Second):
		sh.mu.Unlock()
		<-done
		t.Fatal("identity hit waited for the cache shard lock")
	}
}

func TestStreamSeriesFullIndexCollisionAndGrowth(t *testing.T) {
	c := New()
	var held []*StreamSeries
	for i := range 50000 {
		name := fmt.Appendf(nil, "stream.growth.%d", i)
		s := c.ResolveStreamName(name).(*StreamSeries)
		held = append(held, s)
		if c.ResolveStreamName(name) != s {
			t.Fatal("growth lost a live identity")
		}
	}
	name := []byte("stream.full.index")
	hash := xxh3.Hash(name)
	sh := c.data[hash&(shardCount-1)]
	// Fill the table with nonmatching hashes, including dead referents. Neither
	// an empty slot nor liveness-based reclamation is required for termination.
	sh.streamSeries = make([]seriesSlot, maxStreamSeriesSlots)
	dead := weak.Make(&StreamSeries{name: "dead"})
	for i := range sh.streamSeries {
		sh.streamSeries[i] = seriesSlot{hash ^ 1, dead}
	}
	home := int(hash>>shardBits) & (maxStreamSeriesSlots - 1)
	wrong := &StreamSeries{name: "hash.collision", cache: c, hash: hash}
	right := &StreamSeries{name: string(name), cache: c, hash: hash}
	sh.streamSeries[home] = seriesSlot{hash, weak.Make(wrong)}
	sh.streamSeries[(home+1)&(maxStreamSeriesSlots-1)] = seriesSlot{hash, weak.Make(right)}
	if c.ResolveStreamName(name) != right {
		t.Fatal("hash collision aliased a name")
	}
	for i, inserted := 0, 0; inserted < 100; i++ {
		n := fmt.Appendf(nil, "stream.full.miss.%d", i)
		if xxh3.Hash(n)&(shardCount-1) != hash&(shardCount-1) {
			continue
		}
		inserted++
		slot := c.ResolveStreamName(n)
		c.AddStreamPoint(n, float64(i), int64(i), &slot)
		if got := c.Get(string(n)); len(got) != 1 || got[0].Value != float64(i) {
			t.Fatal("replacement lost a point")
		}
	}
	runtime.KeepAlive(held)
	runtime.KeepAlive(wrong)
	runtime.KeepAlive(right)
}

func TestStreamSeriesRejectedFallbackRefreshesHint(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		c := New()
		c.SetMaxSize(0)
		c.SetThrottleWithState(func(p *points.Points, _ bool, state *any) bool {
			n, _ := (*state).(int)
			*state = n + 1
			return !overflow && p.Data[0].Value < 0
		})
		s := c.ResolveStreamName([]byte("stream.rejected.hint")).(*StreamSeries)
		c.addStreamSeries(s, 1, 1)
		s.hint.Store(0) // Eviction/rehash can invalidate a previously warm hint.
		if overflow {
			c.SetMaxBytes(1)
		}
		c.addStreamSeries(s, -1, 2)
		if s.hint.Load() == 0 {
			t.Fatal("rejected existing item did not refresh its hint")
		}
		c.addStreamSeries(s, -2, 3)
		sh := c.data[s.hash&(shardCount-1)]
		it := sh.items.get(s.name)
		// The ordinary fallback uses scratch quota state, and the warm path uses
		// item state directly, including on rejection. Preserve both behaviors.
		if it.throttleState() != 2 || it.count() != 1 {
			t.Fatalf("quota state %v, points %d", it.throttleState(), it.count())
		}
	}
}
