package cache

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// Rehash during a walk must neither lose existing names nor inspect a detached
// item's compact data: Pop decodes detached items without the shard lock.
func TestQueueRegionRehash(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			shard := &Shard{}
			settings := &cacheSettings{writeoutScanYield: true, writeoutMinPoints: 2, writeoutMaxDelay: time.Hour}
			// Force table order, keeping all target names outside the first region.
			shard.items.slots = make([]itemSlot, 1024)
			shard.items.control = make([]byte, 1024)
			var names []string
			for i := 0; len(names) < 400; i++ {
				name := fmt.Sprintf("walk.%d", i)
				hash := xxh3.HashString(name)
				if (hash>>shardBits)&1023 < 256 {
					continue
				}
				it := newItem(name, 0)
				it.compact.append(1, 1)
				shard.items.setHashed(hash, it)
				names = append(names, name)
			}
			// One ready item in the first region triggers the concurrent mutation model.
			first := newItem("first", 0)
			first.compact.append(1, 1)
			shard.items.setHashed(0, first)
			removed := names[len(names)-1]
			want := map[string]bool{"first": true}
			for _, name := range names[:len(names)-1] {
				want[name] = true
			}

			var scan queueScan
			q, _, more := scan.appendRegion(shard, time.Now(), settings, batch, Noop, nil)
			if !more {
				t.Fatal("fixture fits one region")
			}
			shard.mu.Lock()
			shard.items.remove(removed)
			for i := range 700 {
				name := fmt.Sprintf("new.%d", i)
				v := newItem(name, 0)
				v.firstArrival = time.Now()
				v.compact.append(1, 1)
				shard.items.set(name, v)
			}
			shard.mu.Unlock()
			for more {
				q, _, more = scan.appendRegion(shard, time.Now(), settings, batch, Noop, q)
			}
			seen := map[string]bool{}
			for _, it := range q {
				if seen[it.metric] {
					t.Fatalf("duplicate %q", it.metric)
				}
				seen[it.metric] = true
				if it.metric == removed {
					t.Fatal("queued removed item")
				}
				delete(want, it.metric)
			}

			if next := shard.items.readyAt(time.Now(), settings, func(*item) {}); batch && next.IsZero() {
				t.Fatal("lost deferred wakeup after rehash")
			}
			if len(want) != 0 {
				t.Fatalf("missed %d original names", len(want))
			}
		})
	}
}

func TestQueueConcurrentRestoreLiveWriteout(t *testing.T) {
	const metrics, rounds, workers = 2048, 8, 16
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	if err := c.SetWriteoutBatching(2, time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	// Concentrate on a real shard so concurrent writers force multi-region
	// walks and rehashes without constructing a multi-million-metric test cache.
	names := make([]string, 0, metrics)
	for candidate := 0; len(names) < metrics; candidate++ {
		name := fmt.Sprintf("concurrent.restore.%d", candidate)
		if xxh3.HashString(name)&(shardCount-1) == 0 {
			names = append(names, name)
		}
	}
	// Per-source unique values prove exact point conservation, including failed
	// writes returned to the cache. Each metric has one live writer.
	written := make(map[string][]points.Point)
	abort := make(chan bool)
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for {
			name := c.WriteoutQueue().Get(abort)
			if name == "" {
				return
			}
			if p, ok := c.PopNotConfirmed(name); ok {
				if len(p.Data)%3 == 0 {
					c.Requeue(p)
				} else {
					written[name] = append(written[name], p.Data...)
					c.Confirm(p)
				}
			}
		}
	}()
	var producers sync.WaitGroup
	for w := range workers {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for round := range rounds {
				for i := w; i < metrics; i += workers {
					c.AddPoint([]byte(names[i]), float64(round), int64(100+round))
				}
			}
		}()
	}
	producers.Add(1)
	go func() {
		defer producers.Done()
		for i, name := range names {
			c.AddRestored(&points.Points{Metric: name, Data: []points.Point{{Value: float64(i), Timestamp: 1}, {Value: float64(i), Timestamp: 2}}})
		}
	}()
	producers.Wait()
	close(abort)
	writer.Wait()
	for _, name := range names {
		if p, ok := c.PopNotConfirmed(name); ok {
			written[name] = append(written[name], p.Data...)
			c.Confirm(p)
		}
	}
	for i, name := range names {
		counts := map[points.Point]int{}
		lastLive := int64(99)
		for _, p := range written[name] {
			counts[p]++
			if p.Timestamp >= 100 {
				if p.Timestamp <= lastLive {
					t.Fatalf("live order changed for %s: %v", name, written[name])
				}
				lastLive = p.Timestamp
			}
		}
		want := map[points.Point]int{{Value: float64(i), Timestamp: 1}: 1, {Value: float64(i), Timestamp: 2}: 1}
		for round := range rounds {
			want[points.Point{Value: float64(round), Timestamp: int64(100 + round)}]++
		}
		if !reflect.DeepEqual(counts, want) {
			t.Fatalf("%s points=%v want=%v", name, counts, want)
		}
	}
	if c.Size() != 0 || c.NotConfirmedLength() != 0 {
		t.Fatal("points left after final drain")
	}
}

func TestQueueScanYieldEscape(t *testing.T) {
	c := New()
	if !c.settings.Load().(*cacheSettings).writeoutScanYield {
		t.Fatal("scan yielding must default on")
	}
	shard := c.data[0]
	for i := range 400 {
		name := fmt.Sprintf("escape.%d", i)
		shard.items.set(name, newItem(name, 0))
	}
	for _, on := range []bool{true, false, true} {
		c.SetWriteoutScanYield(on)
		var scan queueScan
		q, _, more := scan.appendRegion(shard, time.Now(), c.settings.Load().(*cacheSettings), false, Noop, nil)
		if more != on {
			t.Fatalf("yield=%t more=%t", on, more)
		}
		for more {
			q, _, more = scan.appendRegion(shard, time.Now(), c.settings.Load().(*cacheSettings), false, Noop, q)
		}
		if len(q) != 400 {
			t.Fatalf("yield=%t queued %d metrics", on, len(q))
		}
	}
}

func TestQueueRegionsCoalesceDeferred(t *testing.T) {
	shard := &Shard{}
	now := time.Now()
	s := &cacheSettings{writeoutScanYield: true, writeoutMinPoints: 2, writeoutMaxDelay: time.Hour}
	for i := range 400 {
		name := fmt.Sprintf("deferred.%d", i)
		it := newItem(name, 0)
		it.firstArrival = now
		it.compact.append(1, 1)
		shard.items.set(name, it)
	}
	want := shard.items.readyAt(now, s, func(*item) { t.Fatal("fresh point ready") })
	var scan queueScan
	q, next, more := scan.appendRegion(shard, now, s, true, Noop, nil)
	if more || len(q) != 0 || !next.Equal(want) {
		t.Fatalf("deferred scan: more=%t queued=%d next=%s want=%s", more, len(q), next, want)
	}
	// A threshold crossing in a cached region must interrupt the summary skip.
	it := shard.items.get("deferred.300")
	it.compact.append(2, 2)
	shard.items.invalidateItemDeadline(it)
	scan = queueScan{}
	q, next, more = scan.appendRegion(shard, now, s, true, Noop, nil)
	for more {
		var at time.Time
		q, at, more = scan.appendRegion(shard, now, s, true, Noop, q)
		if !at.IsZero() && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}
	if len(q) != 1 || q[0].metric != it.Metric || !next.Equal(want) {
		t.Fatalf("threshold scan: queued=%v next=%s want=%s", q, next, want)
	}
}
