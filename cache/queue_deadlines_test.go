package cache

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// The full scan is the oracle, including the earliest deferred wake-up.
func checkQueueDeadlines(t *testing.T, c *Cache, now time.Time) {
	t.Helper()
	s := c.settings.Load().(*cacheSettings)
	batch := c.batchWrites(s)
	want := make(map[string]bool)
	var deadline time.Time
	for _, shard := range c.data {
		shard.mu.Lock()
		for name, it := range shard.items.all {
			at := writeoutDeadline(it, s)
			if batch && now.Before(at) {
				if deadline.IsZero() || at.Before(deadline) {
					deadline = at
				}
			} else {
				want[name] = true
			}
		}
		shard.mu.Unlock()
	}
	q, gotDeadline := c.makeQueueAt(now)
	if !gotDeadline.Equal(deadline) {
		t.Fatalf("wake-up = %v, full scan %v", gotDeadline, deadline)
	}
	for q != nil && len(q) > 0 {
		name := <-q
		if !want[name] {
			t.Fatalf("unexpected or duplicate metric %q", name)
		}
		delete(want, name)
	}
	if len(want) != 0 {
		t.Fatalf("missed %d ready metrics", len(want))
	}
}

func TestQueueDeadlinesMatchFullScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		c.SetMaxSize(0)
		c.SetWriteoutBatching(16, time.Second)
		c.SetWriteoutSpread(.2)
		c.SetWriteoutStartupSpread(true)
		rng := rand.New(rand.NewPCG(3, 4))
		for round := range 80 {
			for range 40 {
				name := fmt.Sprintf("queue.metric.%d", rng.IntN(300))
				switch rng.IntN(5) {
				case 0:
					c.AddRestored(points.OnePoint(name, 1, 1))
				case 1:
					if p, ok := c.PopNotConfirmed(name); ok {
						c.AddPoint([]byte(name), 2, 2)
						c.Requeue(p)
					}
				case 2:
					c.Pop(name)
				default:
					for range 1 + rng.IntN(20) {
						c.AddPoint([]byte(name), 1, 1)
					}
				}
			}
			if round%10 == 0 {
				c.SetWriteoutBatching(8+round, time.Second+time.Duration(round)*time.Millisecond)
				c.SetWriteoutStartupSpread(round%20 == 0)
				c.SetWriteoutSparseSpread(round%20 != 0)
				c.SetMaxSize(uint64([]int{0, 500}[round/10%2]))
			}
			checkQueueDeadlines(t, c, time.Now())
			checkQueueDeadlines(t, c, time.Now()) // Reuse the summaries.
			time.Sleep(50 * time.Millisecond)
		}
	})
}

func TestQueueDeadlinesProbeAcrossRegionAndRehash(t *testing.T) {
	var m itemMap
	s := &cacheSettings{writeoutMinPoints: 2, writeoutMaxDelay: time.Hour}
	now := time.Now()
	// Force a probe across the 256-slot boundary using real hashes, so ready
	// invalidates the item's actual region rather than its initial bucket.
	m.slots = make([]itemSlot, 1024)
	var colliding []*item
	for i := 0; len(colliding) < 3; i++ {
		name := fmt.Sprintf("collision.%d", i)
		if (xxh3.HashString(name)>>shardBits)&1023 != 255 {
			continue
		}
		it := newItem(name, 0)
		it.firstArrival = now
		it.compact.append(1, 1)
		m.set(name, it)
		colliding = append(colliding, it)
	}
	var got int
	yield := func(*item) { got++ }
	m.readyAt(now, s, yield)
	colliding[1].compact.append(2, 2)
	m.invalidateItemDeadline(colliding[1])
	m.readyAt(now, s, yield)
	if got != 1 {
		t.Fatalf("threshold crossing across region boundary: got %d", got)
	}
	m.remove(colliding[1].Metric)
	for i := range 600 {
		m.set(fmt.Sprintf("growth.%d", i), newItem(fmt.Sprintf("growth.%d", i), 0))
	}
	got = 0
	m.readyAt(now, s, yield)
	if got != 600 {
		t.Fatalf("rehash lost newly ready items: got %d", got)
	}
}

func TestQueueDeadlinesWakeOnThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		c.SetWriteoutBatching(8, time.Hour)
		c.AddPoint([]byte("wake.metric"), 1, 1)
		abort := make(chan bool)
		defer close(abort)
		got := make(chan string, 1)
		go func() { got <- c.WriteoutQueue().Get(abort) }()
		synctest.Wait() // The first scan records a future region deadline.
		start := time.Now()
		for i := range 7 {
			c.AddPoint([]byte("wake.metric"), float64(i), int64(i))
		}
		if name := <-got; name != "wake.metric" || time.Since(start) > time.Second {
			t.Fatalf("threshold wake-up: %q after %v", name, time.Since(start))
		}
		if p, ok := c.PopNotConfirmed("wake.metric"); !ok || len(p.Data) != 8 {
			t.Fatal("threshold write lost points")
		} else {
			c.Confirm(p)
		}
	})
}

func TestQueueDeadlinesArrivalWithPreviousSettings(t *testing.T) {
	c := New()
	previous := c.settings.Load().(*cacheSettings)
	c.SetWriteoutBatching(2, time.Hour)
	c.AddPoint([]byte("reload.metric"), 1, 1)
	checkQueueDeadlines(t, c, time.Now())
	shard := c.GetShard("reload.metric")
	shard.mu.Lock()
	// Model AddPoint loading settings before the reload, then waiting for
	// the shard lock while the new settings' scan records its deadline.
	c.addPointLocked(shard, previous, shard.items.get("reload.metric"), 2, 2)
	shard.mu.Unlock()
	checkQueueDeadlines(t, c, time.Now())
}

func TestQueueDeadlinesSparseGrowth(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.SetWriteoutBatching(8, time.Hour)
	c.SetWriteoutSparseSpread(true)
	c.AddPoint([]byte("sparse.metric"), 1, 1)
	shard := c.GetShard("sparse.metric")
	it := shard.items.get("sparse.metric")
	it.sparse, it.slot = true, 1<<30 // An earlier sparse deadline.
	checkQueueDeadlines(t, c, time.Now())
	c.AddPoint([]byte("sparse.metric"), 2, 2)
	c.AddPoint([]byte("sparse.metric"), 3, 3)
	checkQueueDeadlines(t, c, time.Now())
}
