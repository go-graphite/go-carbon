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
			if enabled && it.logTracked {
				got[&it.Points] = it.logFirst
				counts[it.logFirst]++
			}
			if _, ok := sh.log.first[&it.Points]; ok {
				t.Fatal("queued item retained in in-flight map")
			}
		}
		for p, g := range sh.log.first {
			got[p] = g
			counts[g]++
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
				delete(want, &it.Points)
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
						if g, ok := want[&old.Points]; ok && (!tracked || g < first) {
							first, tracked = g, true
						}
						delete(want, &old.Points)
					}
					c.Requeue(p)
					if enabled {
						if !tracked {
							first = gen
						}
						want[&sh.items.get(p.Metric).Points] = first
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
					want[&it.Points] = 0
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
				want[&sh.items.get(name).Points] = gen
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
	l := shardLog{first: map[*points.Points]uint64{}, items: map[uint64]int32{}}
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
