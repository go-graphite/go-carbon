package cache

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// Keep the old pointer-map model as an independent oracle. Drive the real
// cache through replacements, duplicate confirmation, relog and log toggles.
func TestBookkeepingGenerationOracle(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	sh := c.GetShard("generation.metric")
	index := slices.Index(c.data, sh)
	var names []string
	for i := 0; len(names) < 32; i++ {
		name := fmt.Sprintf("generation.metric.%d", i)
		if c.GetShard(name) == sh {
			names = append(names, name)
		}
	}
	want := map[*points.Points]uint64{}
	var flight []*points.Points
	gen, enabled := uint64(0), true
	rng := rand.New(rand.NewPCG(31, 73))
	check := func() {
		t.Helper()
		got := map[*points.Points]uint64{}
		counts := map[uint64]int32{}
		for _, it := range sh.items.all {
			if enabled && it.has(itemLogTracked) {
				got[it.identity()] = it.logFirst
				counts[it.logFirst]++
			}
			if _, ok := sh.log.first[it.identity()]; ok {
				t.Fatal("queued item retained in in-flight map")
			}
		}
		for p, g := range sh.log.first {
			got[p] = g.gen
			counts[g.gen]++
		}
		if !reflect.DeepEqual(got, want) || len(counts) != len(sh.log.items) {
			t.Fatalf("generation ownership differs: got %v want %v counts %v", got, want, sh.log.items)
		}
		low := gen
		for g, n := range counts {
			low = min(low, g)
			if sh.log.items[g] != n {
				t.Fatalf("generation %d count=%d, want %d", g, sh.log.items[g], n)
			}
		}
		if got := sh.log.lowWater(); got != low {
			t.Fatalf("low water=%d, want %d", got, low)
		}
	}
	for step := range 10000 {
		name := names[rng.IntN(len(names))]
		switch rng.IntN(12) {
		case 0:
			if it := sh.items.get(name); it != nil {
				delete(want, it.identity())
			}
			c.Pop(name)
		case 1, 2:
			if p, ok := c.PopNotConfirmed(name); ok {
				flight = append(flight, p)
			}
		case 3, 4:
			if len(flight) > 0 {
				i := rng.IntN(len(flight))
				p := flight[i]
				flight = append(flight[:i], flight[i+1:]...)
				if step%2 == 0 {
					c.Confirm(p)
					c.Confirm(p)
					delete(want, p)
				} else {
					first, tracked := want[p]
					delete(want, p)
					if old := sh.items.get(p.Metric); old != nil {
						if g, ok := want[old.identity()]; ok && (!tracked || g < first) {
							first, tracked = g, true
						}
						delete(want, old.identity())
					}
					c.Requeue(p)
					if enabled {
						if !tracked {
							first = gen
						}
						want[sh.items.get(p.Metric).identity()] = first
					}
					c.Requeue(p) // An old identity must not merge twice.
				}
			}
		case 5:
			gen++
			c.TakeLogBatch(index, gen, inputlog.Batch{})
		case 6:
			if enabled {
				cutoff := gen / 2
				sh.log.relog(cutoff, math.MaxInt, &sh.items)
				for p, g := range want {
					if g < cutoff {
						want[p] = gen
					}
				}
			}
		case 7:
			c.DisableInputLog()
			clear(want)
			gen, enabled = 0, false
		case 8:
			c.EnableInputLog()
			if !enabled {
				gen = 0
				for _, it := range sh.items.all {
					want[it.identity()] = 0
				}
				for _, p := range flight {
					want[p] = 0
				}
			}
			enabled = true
		default:
			old := sh.items.get(name)
			c.AddPoint([]byte(name), float64(step), int64(step))
			if enabled && old == nil {
				want[sh.items.get(name).identity()] = gen
			}
		}
		check()
	}
}

func TestBookkeepingLogDrainLimitsAndLoss(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	name := "bounded.metric"
	sh := c.GetShard(name)
	index := slices.Index(c.data, sh)
	for i := range maxShardLogEntries + 7 {
		c.AddPoint([]byte(name), math.Float64frombits(uint64(i)), int64(i)-17)
	}
	if c.LogDropped() != 7 || c.Size() != maxShardLogEntries+7 {
		t.Fatalf("cached=%d, lost=%d", c.Size(), c.LogDropped())
	}
	if sh.log.restoreSpace(1) == nil {
		t.Fatal("restore bypassed its half-buffer admission limit")
	}
	wait := sh.log.waitDrained
	batch := c.TakeLogBatch(index, 9, inputlog.Batch{})
	select {
	case <-wait:
	default:
		t.Fatal("drain did not release restore admission")
	}
	if batch.Len() != maxShardLogEntries {
		t.Fatal("column lengths or live drain limit changed")
	}
	// Popping and reusing the name must not alter the already drained batch.
	c.Pop(name)
	c.AddPoint([]byte(name), 42, math.MinInt64)
	for i, p := range batch.Entries(nil) {
		if p.Metric() != name || math.Float64bits(p.Value) != uint64(i) || p.Timestamp != int64(i)-17 {
			t.Fatalf("drained point %d changed", i)
		}
	}
	if sh.log.entries.Len() != 1 || c.LogDropped() != 7 || sh.log.restoreSpace(maxShardRestoredLogEntries-1) != nil || sh.log.restoreSpace(maxShardRestoredLogEntries) == nil {
		t.Fatal("drain changed loss accounting or restore limit")
	}
}

func TestBookkeepingRelogBudget(t *testing.T) {
	var m itemMap
	l := shardLog{first: map[*points.Points]logFlight{}, items: map[uint64]int32{}}
	// Exact collisions fix the iteration order without depending on name hashes.
	for i, n := range []int{7, 3, 2} {
		it := newItem(fmt.Sprintf("budget.%d", i), 0)
		for j := range n {
			it.compact.append(float64(j), int64(j))
		}
		m.setHashed(0, it)
		l.trackItem(it)
	}
	l.gen = 8
	if got := l.relog(4, 5, &m); got != 7 {
		t.Fatalf("first oversized batch did not make progress: %d", got)
	}
	if l.items[0] != 2 || l.items[8] != 1 || l.lowWater() != 0 {
		t.Fatal("skipped batches lost their old generation")
	}
	if got := l.relog(4, 5, &m); got != 5 || l.lowWater() != 8 || l.entries.Len() != 12 {
		t.Fatalf("remaining batches did not fit budget: %d", got)
	}
}

func TestBookkeepingConcurrentReadersAndDrainer(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	const name, count = "concurrent.bookkeeping", 10000
	index := slices.Index(c.data, c.GetShard(name))
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				for _, p := range c.Get(name) {
					if p.Value != float64(p.Timestamp) {
						t.Error("reader observed changed point")
						return
					}
				}
			}
		})
	}
	var recorded int
	wg.Go(func() {
		var spare inputlog.Batch
		for gen := uint64(1); ; gen++ {
			batch := c.TakeLogBatch(index, gen, spare)
			for _, p := range batch.Entries(nil) {
				if p.Metric() != name || p.Value != float64(p.Timestamp) {
					t.Error("drainer observed changed point")
					return
				}
			}
			recorded += batch.Len()
			clear(batch.Refs)
			spare = batch
			select {
			case <-done:
				batch = c.TakeLogBatch(index, gen+1, inputlog.Batch{})
				recorded += batch.Len()
				return
			default:
			}
		}
	})
	for i := range count {
		c.AddPoint([]byte(name), float64(i), int64(i))
		if i%13 == 12 {
			p, _ := c.PopNotConfirmed(name)
			if i%2 == 0 {
				c.Requeue(p)
			} else {
				c.Confirm(p)
			}
		}
	}
	close(done)
	wg.Wait()
	if recorded != count || c.LogDropped() != 0 {
		t.Fatalf("recorded=%d, dropped=%d", recorded, c.LogDropped())
	}
}

func TestItemDeferredIdentityAccounting(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.AddPoint([]byte("optional.state"), 42, 1)
	sh := c.GetShard("optional.state")
	it := sh.items.get("optional.state")
	if it.pending != nil {
		t.Fatal("ordinary item allocated a recovery identity")
	}
	before := sh.items.bytes
	if sh.setItemThrottle(it, 1) || sh.items.bytes != before || it.throttleState() != 1 {
		t.Fatal("inline quota state changed accounting")
	}
	sh.setItemThrottle(it, nil)
	identity := sh.itemIdentity(it)
	if sh.itemIdentity(it) != identity || sh.items.bytes != before+it.extraMemory() {
		t.Fatal("identity changed or was charged twice")
	}
	p, ok := c.PopNotConfirmed(it.Metric)
	if !ok || p != identity || len(p.Data) != 1 || p.Data[0].Value != 42 {
		t.Fatal("pop changed recovery identity or data")
	}
	c.Requeue(p)
	if got := c.Get(it.Metric); len(got) != 1 || got[0].Value != 42 {
		t.Fatal("retry lost points")
	}
	p, _ = c.PopNotConfirmed(it.Metric)
	c.Confirm(p)
	if !c.IsEmpty() {
		t.Fatal("confirmation retained a batch")
	}
}

// The state slot passed to a configured hook must not escape on every hot
// point. The side object is initialized once and the shard scratch is cleared.
func TestStatefulThrottleHotPathDoesNotAllocate(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.SetThrottleWithState(func(_ *points.Points, _ bool, state *any) bool {
		if *state == nil {
			*state = uint64(1)
		}
		return false
	})
	name := []byte("stateful.no-escape")
	c.AddPoint(name, 42, 1)
	sh := c.GetShard(string(name))
	it := sh.items.get(string(name))
	before := cap(it.compact.buf)
	it.compact.reserve(8192)
	sh.items.bytes += int64(cap(it.compact.buf) - before)
	sh.items.pointBytes += int64(cap(it.compact.buf) - before)
	allocations := testing.AllocsPerRun(1000, func() { c.AddPoint(name, 42, 1) })
	if allocations != 0 {
		t.Fatalf("stateful point allocated %.3f objects", allocations)
	}
	if sh.throttleScratch != nil || it.throttleState() != uint64(1) {
		t.Fatal("scratch retained state or item lost it")
	}
}

func TestBookkeepingDrainReleasesHighWater(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	c.SetWriteoutBatching(960, 960*1e9)
	sh := c.GetShard("drain.capacity")
	index := slices.Index(c.data, sh)
	var names []string
	for i := 0; len(names) < 200; i++ {
		name := fmt.Sprintf("drain.capacity.%d", i)
		if c.GetShard(name) == sh {
			names = append(names, name)
		}
	}
	for i, name := range names {
		c.TakeLogBatch(index, uint64(i+1), inputlog.Batch{})
		c.AddPoint([]byte(name), float64(i), int64(i))
	}
	var flight []*points.Points
	for _, name := range names {
		p, ok := c.PopNotConfirmed(name)
		if !ok {
			t.Fatal("missing pop")
		}
		flight = append(flight, p)
	}
	if sh.flightPeak < 200 || len(sh.log.items) != 200 || len(sh.firstArrival) != 200 {
		t.Fatal("fixture did not reach the tracking peak")
	}
	for _, p := range flight {
		c.Confirm(p)
	}
	if sh.flightPeak != 0 || sh.log.generationPeak != 0 || cap(sh.notConfirmed) > 64 || sh.firstArrival != nil || len(sh.log.items) != 0 || sh.log.first == nil || len(sh.log.first) != 0 {
		t.Fatal("drained tracking retained burst capacity or disabled logging")
	}
	taken := c.TakeLogBatch(index, 1000, inputlog.Batch{})
	if len(sh.items.slots) != 0 || len(sh.items.control) != 0 || sh.items.bytes != 0 {
		t.Fatal("empty item map retained its high-water table")
	}
	c.AddPoint([]byte(names[0]), 123, 456)
	if got := c.Get(names[0]); len(got) != 1 || got[0].Value != 123 || sh.log.lowWater() != 1000 {
		t.Fatal("new generation after drain changed data or tracking")
	}
	if taken.Len() != 1 || taken.Entries(nil)[0].Metric() != names[len(names)-1] {
		t.Fatal("drain mutated the consumer's accepted point")
	}
}
