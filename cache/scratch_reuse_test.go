package cache

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// referenceQueue is the scan of the queue builder before buffer reuse: every
// eligible item exactly once.
func referenceQueue(c *Cache, now time.Time) []string {
	s := c.settings.Load().(*cacheSettings)
	batch := c.batchWrites(s)
	var names []string
	for _, shard := range c.data {
		shard.mu.Lock()
		for _, p := range shard.items.all {
			if batch && now.Before(writeoutDeadline(p, s)) {
				continue
			}
			names = append(names, p.Metric)
		}
		shard.mu.Unlock()
	}
	slices.Sort(names)
	return names
}

func drainQueue(q chan string) []string {
	var names []string
	for len(q) > 0 {
		names = append(names, <-q)
	}
	return names
}

func cachedItem(c *Cache, name string) *item {
	shard := c.GetShard(name)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.items.get(name)
}

// requireCleanScratch fails if the retained buffer pins any metric name.
func requireCleanScratch(t *testing.T, c *Cache) {
	t.Helper()
	if scratch := c.queueScratch.Load(); scratch != nil {
		for i, item := range (*scratch)[:cap(*scratch)] {
			if item != (queueItem{}) {
				t.Fatalf("scratch entry %d retained %+v", i, item)
			}
		}
	}
}

func scratchArray(c *Cache) *queueItem {
	scratch := c.queueScratch.Load()
	if scratch == nil || cap(*scratch) == 0 {
		return nil
	}
	return &(*scratch)[:1][0]
}

func TestMakeQueueReusesScratchWithoutStaleEntries(t *testing.T) {
	t.Parallel()
	const universe = 3000
	var names [universe]string
	for i := range names {
		names[i] = fmt.Sprintf("scratch.metric.%04d", i)
	}
	name := func(i int) string { return names[i] }
	for _, tc := range []struct {
		strategy string
		batching bool
	}{{"max", false}, {"sorted", false}, {"noop", false}, {"lexical", false}, {"noop", true}} {
		t.Run(fmt.Sprintf("%s/batching=%v", tc.strategy, tc.batching), func(t *testing.T) {
			t.Parallel()
			c := New()
			c.SetMaxSize(1 << 30)
			if err := c.SetWriteStrategy(tc.strategy); err != nil {
				t.Fatal(err)
			}
			if tc.batching {
				// Items below three points stay deferred, so the scan fills
				// only part of the reserved queue.
				if err := c.SetWriteoutBatching(3, time.Hour); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < universe; i++ {
				for j := 0; j <= i%5; j++ {
					c.AddPoint([]byte(name(i)), float64(j), int64(1000+i%7+j))
				}
			}

			// Rebuild under concurrent AddPoint, Pop and in-flight writes.
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for w := 0; w < 4; w++ {
				wg.Add(1)
				go func(seed uint64) {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(seed, 1))
					buf := make([]byte, 0, 32)
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						buf = append(buf[:0], name(rng.IntN(universe))...)
						c.AddPoint(buf, float64(i), int64(1000+i%11))
					}
				}(uint64(w))
			}
			for w := 0; w < 2; w++ {
				wg.Add(1)
				go func(seed uint64) {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(seed, 2))
					for {
						select {
						case <-stop:
							return
						default:
						}
						metric := name(rng.IntN(universe))
						if rng.IntN(2) == 0 {
							c.Pop(metric)
						} else if p, ok := c.PopNotConfirmed(metric); ok {
							c.Confirm(p)
						}
					}
				}(uint64(10 + w))
			}
			seen := make(map[string]bool, universe)
			for round := 0; round < 40; round++ {
				q, _ := c.makeQueueAt(time.Now())
				clear(seen)
				for len(q) > 0 {
					metric := <-q
					var i int
					if _, err := fmt.Sscanf(metric, "scratch.metric.%04d", &i); err != nil || i < 0 || i >= universe || metric != name(i) || seen[metric] {
						t.Fatalf("round %d: stale, foreign or duplicate entry %q", round, metric)
					}
					seen[metric] = true
				}
			}
			close(stop)
			wg.Wait()
			requireCleanScratch(t, c)

			// Quiescent rebuilds must match the reference scan exactly.
			check := func(stage string) {
				t.Helper()
				now := time.Now()
				want := referenceQueue(c, now)
				q, _ := c.makeQueueAt(now)
				ordered := drainQueue(q)
				for i := 1; i < len(ordered); i++ {
					prev, cur := ordered[i-1], ordered[i]
					var bad bool
					switch tc.strategy {
					case "max":
						bad = cachedItem(c, prev).count() < cachedItem(c, cur).count()
					case "sorted":
						bad = c.Get(prev)[0].Timestamp > c.Get(cur)[0].Timestamp
					case "lexical":
						bad = prev > cur
					}
					if bad {
						t.Fatalf("%s: %s order violated at %d: %q before %q", stage, tc.strategy, i, prev, cur)
					}
				}
				got := slices.Clone(ordered)
				slices.Sort(got)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: queue has %d entries, reference %d", stage, len(got), len(want))
				}
				requireCleanScratch(t, c)
			}
			check("after churn")

			// Leave a known population for the shrink checks: concurrent pops
			// may otherwise remove every metric that the final drain keeps.
			for i := 0; i < universe; i++ {
				c.AddPoint([]byte(name(i)), 1, 1000)
			}

			// Start from an exact-size buffer; shrinking by a tenth reuses it.
			c.queueScratch.Store(nil)
			check("fresh scratch")
			before := scratchArray(c)
			if before == nil {
				t.Fatal("no scratch retained")
			}
			for i := 0; i < universe; i += 10 {
				c.Pop(name(i))
			}
			if capacity, n := cap(*c.queueScratch.Load()), len(referenceQueue(c, time.Now())); capacity < n || capacity > max(1024, 2*n) {
				t.Fatalf("precondition: scratch cap %d for %d metrics", capacity, n)
			}
			check("after shrinking")
			if scratchArray(c) != before {
				t.Fatal("scratch was not reused for a moderately smaller cache")
			}

			// Shrink far below half: a smaller buffer replaces it.
			before = scratchArray(c)
			for i := 0; i < universe-20; i++ {
				c.Pop(name(i))
			}
			check("after draining")
			if scratchArray(c) == before {
				t.Fatal("scratch more than twice the cache was kept")
			}

			// An empty cache keeps the buffer for later and builds no queue.
			for i := 0; i < universe; i++ {
				c.Pop(name(i))
			}
			before = scratchArray(c)
			if q, _ := c.makeQueueAt(time.Now()); q != nil {
				t.Fatalf("empty cache built a queue of %d", len(q))
			}
			if scratchArray(c) != before {
				t.Fatal("empty rebuild replaced the scratch")
			}
		})
	}
}

func TestAddPointReservesPoppedLengthWithoutChangingAccounting(t *testing.T) {
	c := New()
	name := []byte("hint.metric")
	shard := c.GetShard(string(name))

	c.AddPoint(name, 1, 1)
	if it := cachedItem(c, string(name)); it.count() != 1 || cap(it.compact.buf) != minBytesHint {
		t.Fatalf("untrained item count %d cap %d", it.count(), cap(it.compact.buf))
	}
	c.Pop(string(name))

	// cycle returns the compact length of n points.
	cycle := func(n int) int {
		for j := 0; j < n; j++ {
			c.AddPoint(name, float64(j), int64(j))
		}
		size := len(cachedItem(c, string(name)).compact.buf)
		p, ok := c.PopNotConfirmed(string(name))
		if !ok || len(p.Data) != n {
			t.Fatalf("popped %v, want %d points", p, n)
		}
		c.Confirm(p)
		return size
	}
	var size int
	for i := 0; i < 1000; i++ {
		size = cycle(20)
	}
	if shard.bytesHint() != size || size <= minBytesHint {
		t.Fatalf("hint %d after %d-byte writeouts", shard.bytesHint(), size)
	}

	c.AddPoint(name, 0, 100)
	it := cachedItem(c, string(name))
	first := &it.compact.buf[:1][0]
	if it.count() != 1 || cap(it.compact.buf) != size || c.Size() != 1 || c.Len() != 1 {
		t.Fatalf("count %d cap %d size %d metrics %d", it.count(), cap(it.compact.buf), c.Size(), c.Len())
	}
	for j := 1; j < 10; j++ {
		c.AddPoint(name, float64(j), int64(100+j))
	}
	if &it.compact.buf[:1][0] != first || c.Size() != 10 {
		t.Fatalf("regrew within the hint, or size %d", c.Size())
	}
	if got := c.Get(string(name)); len(got) != 10 || got[9] != (points.Point{Value: 9, Timestamp: 109}) {
		t.Fatalf("got %v", got)
	}
	if p, _ := c.Pop(string(name)); len(p.Data) != 10 || c.Size() != 0 || c.Len() != 0 {
		t.Fatalf("popped %d points, size %d, metrics %d", len(p.Data), c.Size(), c.Len())
	}

	for i := 0; i < 2000; i++ {
		cycle(200)
	}
	if shard.bytesHint() != maxBytesHint {
		t.Fatalf("hint %d not bounded by %d", shard.bytesHint(), maxBytesHint)
	}
	for i := 0; i < 2000; i++ {
		cycle(1)
	}
	if shard.bytesHint() != minBytesHint {
		t.Fatalf("hint %d after 1-point writeouts", shard.bytesHint())
	}
	// Items shorter than the hint pull it down faster than longer ones push
	// it up, so a mostly short mix keeps one-point items at the minimum.
	for i := 0; i < 400; i++ {
		if i%10 < 7 {
			cycle(1)
		} else {
			cycle(8)
		}
		if shard.bytesHint() > minBytesHint {
			t.Fatalf("hint %d for a 70%% one-point mix", shard.bytesHint())
		}
	}

	// The limit counts points, not reserved capacity.
	admit := func(trained bool) (int64, int32, uint32) {
		c := New()
		c.SetMaxSize(10)
		if trained {
			for _, shard := range c.data {
				shard.bytesQuarters = maxBytesHint << 2
			}
		}
		for i := 0; i < 20; i++ {
			c.AddPoint([]byte(fmt.Sprintf("limit.%d", i)), 1, 1)
		}
		return c.Size(), c.Len(), c.stat.overflowCnt
	}
	sizeA, lenA, overflowA := admit(false)
	sizeB, lenB, overflowB := admit(true)
	if sizeA != sizeB || lenA != lenB || overflowA != overflowB || sizeA != 11 {
		t.Fatalf("untrained size %d metrics %d overflow %d; trained %d %d %d", sizeA, lenA, overflowA, sizeB, lenB, overflowB)
	}
}

// A batch with spare capacity is in flight; Get must not append into it.
func TestGetDoesNotWriteIntoInFlightBatch(t *testing.T) {
	c := New()
	name := "inflight.metric"
	c.AddPoint([]byte(name), 1, 1)
	c.AddPoint([]byte(name), 2, 2)
	inFlight, _ := c.PopNotConfirmed(name)
	// Popping decodes an exact slice; the persister owns it and may extend it.
	inFlight.Data = slices.Grow(inFlight.Data, 8)
	c.AddPoint([]byte(name), 3, 3)
	first := c.Get(name)
	c.Pop(name)
	c.AddPoint([]byte(name), 4, 4)
	second := c.Get(name)
	if first[2].Value != 3 || second[2].Value != 4 {
		t.Fatalf("first %v second %v", first, second)
	}
	if len(inFlight.Data) != 2 || inFlight.Data[:3][2] != (points.Point{}) {
		t.Fatalf("in-flight batch modified: %v", inFlight.Data[:cap(inFlight.Data)])
	}
}
