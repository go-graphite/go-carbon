package cache

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

func TestAddPointBatchMatchesSequential(t *testing.T) {
	for _, limit := range []uint64{0, 1, 7, 91} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			old, next := New(), New()
			var calls [2][]string
			for k, c := range []*Cache{old, next} {
				c.SetMaxSize(limit)
				c.EnableInputLog()
				c.SetBloomSize(512)
				c.SetNewMetricsChan(make(chan string, 1))
				c.SetMetricExists(func(string) bool { return true })
				c.SetThrottleWithHash(func(p *points.Points, exists bool, hash uint64, state *any) bool {
					n, _ := (*state).(uint64)
					*state = n + 1
					calls[k] = append(calls[k], fmt.Sprintf("%s/%t/%d/%d", p.Metric, exists, hash, n))
					return p.Data[0].Value < 0
				})
			}
			var names [][]byte
			var data []points.Point
			for i := range 211 {
				name := fmt.Sprintf("shared.namespace.%d", (i+i/7)%19)
				if i%37 == 0 {
					name = strings.Repeat("long.", 1000) + name
				}
				names = append(names, []byte(name))
				value := float64(i%17 - 2)
				if i%13 == 0 {
					value = math.Copysign(0, -1)
				}
				if i%29 == 0 {
					value = math.Float64frombits(0x7ff8000000000042)
				}
				data = append(data, points.Point{Value: value, Timestamp: int64(i%11) - 5})
			}
			for round := range 3 {
				// Warm-hit and new-item paths, with removals between rounds.
				for i, p := range data {
					old.AddPoint(names[i], p.Value, p.Timestamp)
				}
				next.AddPointBatch(names, data)
				if !reflect.DeepEqual(calls[0], calls[1]) {
					t.Fatal("quota order or state changed")
				}
				compareBatchCaches(t, old, next, names)
				for i, name := range names {
					if i%7 == round {
						old.Pop(string(name))
						next.Pop(string(name))
					}
				}
			}
			var ownedNames [][]byte
			for _, name := range names {
				ownedNames = append(ownedNames, bytes.Clone(name))
				clear(name)
			}
			compareBatchCaches(t, old, next, ownedNames)
			if old.Size() != next.Size() {
				t.Fatal("borrowed buffer changed counts")
			}
		})
	}
}

func compareBatchCaches(t *testing.T, a, b *Cache, names [][]byte) {
	t.Helper()
	if a.Size() != b.Size() || a.PointBytes() != b.PointBytes() || a.EstimatedBytes() != b.EstimatedBytes() || a.stat.overflowCnt != b.stat.overflowCnt {
		t.Fatal("cache accounting changed")
	}
	for _, name := range names {
		x, y := a.Get(string(name)), b.Get(string(name))
		if len(x) != len(y) {
			t.Fatal("point count changed")
		}
		for i := range x {
			if x[i].Timestamp != y[i].Timestamp || math.Float64bits(x[i].Value) != math.Float64bits(y[i].Value) {
				t.Fatal("arrival order or float bits changed")
			}
		}
	}
	for sh := range shardCount {
		xBatch, yBatch := a.TakeLogBatch(sh, 1, inputlog.Batch{}), b.TakeLogBatch(sh, 1, inputlog.Batch{})
		x, y := xBatch.Entries(nil), yBatch.Entries(nil)
		if len(x) != len(y) {
			t.Fatal("log count changed")
		}
		for i := range x {
			if x[i].Metric() != y[i].Metric() || x[i].Timestamp != y[i].Timestamp || math.Float64bits(x[i].Value) != math.Float64bits(y[i].Value) {
				t.Fatal("input-log order changed")
			}
		}
	}
}

func TestAddPointBatchValidatesReplacementAndGrowth(t *testing.T) {
	for _, action := range []string{"remove", "replace", "grow", "divert", "tags"} {
		t.Run(action, func(t *testing.T) {
			old, next := New(), New()
			var first, second string
			for i := 0; second == ""; i++ {
				n := fmt.Sprintf("validation.%d", i)
				if first == "" {
					first = n
				} else if old.GetShard(n) != old.GetShard(first) {
					second = n
				}
			}
			names := [][]byte{[]byte(first), []byte(second)}
			data := []points.Point{{Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 1}}
			var logs [2]bytes.Buffer
			for k, c := range []*Cache{old, next} {
				c.SetMaxSize(0)
				c.AddPoint(names[0], 1, 0)
				c.AddPoint(names[1], 1, 0)
				c.SetThrottleWithHash(func(p *points.Points, _ bool, _ uint64, _ *any) bool {
					if p.Metric == first {
						switch action {
						case "remove":
							c.Pop(second)
						case "replace":
							p, _ := c.PopNotConfirmed(second)
							c.Confirm(p)
							c.AddPoint(names[1], 19, 19)
						case "grow":
							sh := c.GetShard(second)
							for i := range 200 {
								metric := fmt.Sprintf("growth.%d", i)
								sh.mu.Lock()
								sh.items.setHashed(uint64(i)<<shardBits, newItem(metric, 0))
								sh.mu.Unlock()
							}
						case "divert":
							c.DivertToXlog(&logs[k])
						case "tags":
							c.SetTagsEnabled(true)
						}
					}
					return false
				})
			}
			for i, p := range data {
				old.AddPoint(names[i], p.Value, p.Timestamp)
			}
			next.AddPointBatch(names, data)
			compareBatchCaches(t, old, next, names)
			if logs[0].String() != logs[1].String() {
				t.Fatal("diversion changed")
			}
		})
	}
}

func TestAddPointBatchFullHashCollision(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte(strings.Repeat("prefix.", 40) + "a")
	other := string(name[:len(name)-1]) + "b"
	h := xxh3.Hash(name)
	sh := c.GetShard(string(name))
	wrong, right := newItem(other, 0), newItem(string(name), 0)
	wrong.compact.append(1, 1)
	right.compact.append(2, 2)
	sh.items.setHashed(h, wrong)
	sh.items.setHashed(h, right)
	c.addSizeLocked(sh, 2, 0)
	c.AddPointBatch([][]byte{name}, []points.Point{{Value: 3, Timestamp: 3}})
	if wrong.count() != 1 || right.count() != 2 || right.compact.appendDecoded(nil)[1].Value != 3 {
		t.Fatal("full hash collision aliased metrics")
	}
}

func TestAddPointBatchConcurrentLifecycle(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	names := [][]byte{[]byte("concurrent.a"), []byte("concurrent.b"), []byte("concurrent.c")}
	for _, n := range names {
		c.AddPoint(n, 1, 1)
	}
	var wg, workers sync.WaitGroup
	done := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			for _, n := range names {
				if p, ok := c.PopNotConfirmed(string(n)); ok {
					c.Confirm(p)
				}
			}
		}
	})
	for worker := range 8 {
		workers.Go(func() {
			data := make([]points.Point, len(names))
			for i := range 500 {
				for j := range data {
					data[j] = points.Point{Value: float64(worker), Timestamp: int64(i)}
				}
				c.AddPointBatch(names, data)
			}
		})
	}
	workers.Wait()
	close(done)
	wg.Wait()
	if c.LogDropped() != 0 {
		t.Fatal("unexpected log loss")
	}
	for _, n := range names {
		if p, ok := c.PopNotConfirmed(string(n)); ok {
			c.Confirm(p)
		}
	}
	if !c.IsEmpty() {
		t.Fatal("lifecycle left queued points")
	}
	if slices.ContainsFunc(names, func(n []byte) bool { return len(n) == 0 }) {
		t.Fatal("mutated borrowed names")
	}
}

func TestAddPointBatchWarmLongNamesDoNotAllocate(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	names := [][]byte{[]byte(strings.Repeat("namespace.", 30) + "one"), []byte(strings.Repeat("namespace.", 30) + "two")}
	data := []points.Point{{Value: 1, Timestamp: 1}, {Value: 1, Timestamp: 1}}
	c.SetThrottleWithHash(func(_ *points.Points, _ bool, _ uint64, state *any) bool { *state = uint64(1); return false })
	for _, name := range names {
		c.AddPoint(name, 1, 1)
		sh := c.GetShard(string(name))
		it := sh.items.get(string(name))
		capacity := cap(it.compact.buf)
		it.compact.reserve(8192)
		sh.items.bytes += int64(cap(it.compact.buf) - capacity)
		sh.items.pointBytes += int64(cap(it.compact.buf) - capacity)
	}
	if allocations := testing.AllocsPerRun(1000, func() { c.AddPointBatch(names, data) }); allocations != 0 {
		t.Fatalf("warm lookup batch allocated %g objects", allocations)
	}
}

func TestAddPointBatchPanicReleasesShard(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	names := [][]byte{[]byte("panic.first"), []byte("panic.second")}
	data := []points.Point{{Value: 1, Timestamp: 2}, {Value: 2, Timestamp: 1}}
	for _, n := range names {
		c.AddPoint(n, 1, 1)
	}
	c.SetThrottleWithHash(func(_ *points.Points, _ bool, _ uint64, _ *any) bool { panic("quota callback") })
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("missing callback panic")
			}
		}()
		c.AddPointBatch(names, data)
	}()
	c.SetThrottleWithHash(nil)
	done := make(chan struct{})
	go func() { c.AddPoint(names[0], 2, 2); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("panic retained the shard lock")
	}
}
