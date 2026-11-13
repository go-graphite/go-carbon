package cache

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// Compare the real state, including compact bytes, memory accounting, arrival
// times, sparse history, quota state and input-log entries/tracking. Fake time
// makes sequential oracle calls observe exactly the same arrival times.
func comparePointRefCaches(t *testing.T, want, got *Cache) {
	t.Helper()
	for i, a := range want.data {
		b := got.data[i]
		// Stream generations are hints, not cache data. Compare fields by
		// reference so the input log's atomic state is never copied.
		semantic := func(it *item) []any {
			return []any{it.Metric, it.pending, &it.compact, &it.StreamRef, it.spread, it.has(itemSparse), it.has(itemStartup), it.slot, it.firstArrival, it.throttleState(), it.indexGeneration, it.logFirst, it.has(itemLogTracked), it.has(itemLogHistory)}
		}
		am, bm := map[string][]any{}, map[string][]any{}
		for name, it := range a.items.all {
			am[name] = semantic(it)
		}
		for name, it := range b.items.all {
			bm[name] = semantic(it)
		}
		if !reflect.DeepEqual(am, bm) {
			t.Fatalf("shard %d queued items differ", i)
		}
		if !reflect.DeepEqual(a.notConfirmed, b.notConfirmed) || a.notConfirmedUsed != b.notConfirmedUsed {
			t.Fatalf("shard %d in-flight items differ", i)
		}
		if a.sizeDelta != b.sizeDelta || a.byteSize != b.byteSize || a.byteDelta != b.byteDelta || a.inFlightBytes != b.inFlightBytes || a.items.bytes != b.items.bytes || a.bytesQuarters != b.bytesQuarters || !reflect.DeepEqual(a.sparseLast, b.sparseLast) || !reflect.DeepEqual(a.adds, b.adds) {
			t.Fatalf("shard %d accounting/history differ", i)
		}
		arrivals := func(sh *Shard) map[string]time.Time {
			out := map[string]time.Time{}
			for p, at := range sh.firstArrival {
				out[p.Metric] = at
			}
			return out
		}
		if !reflect.DeepEqual(arrivals(a), arrivals(b)) {
			t.Fatal("in-flight arrivals differ")
		}
		if a.log.gen != b.log.gen || a.log.dropped != b.log.dropped || !reflect.DeepEqual(a.log.items, b.log.items) || a.log.entries.Len() != b.log.entries.Len() {
			t.Fatal("input-log accounting differs")
		}
		otherEntries := b.log.entries.Entries(nil)
		for j, x := range a.log.entries.Entries(nil) {
			y := otherEntries[j]
			if x.Metric() != y.Metric() || math.Float64bits(x.Value) != math.Float64bits(y.Value) || x.Timestamp != y.Timestamp {
				t.Fatal("input-log point differs")
			}
		}
		tracked := func(sh *Shard) map[string][]uint64 {
			out := map[string][]uint64{}
			for name, it := range sh.items.all {
				if it.has(itemLogTracked) {
					out[name] = append(out[name], it.logFirst)
				}
			}
			for p, gen := range sh.log.first {
				out[p.Metric] = append(out[p.Metric], gen.gen)
			}
			for _, gens := range out {
				slices.Sort(gens)
			}
			return out
		}
		if !reflect.DeepEqual(tracked(a), tracked(b)) {
			t.Fatal("input-log tracking differs")
		}
	}
	stats := func(c *Cache) map[string]float64 {
		out := map[string]float64{}
		c.Stat(func(name string, value float64) { out[name] = value })
		delete(out, "streamSeriesIndexBytes")
		return out
	}
	if !reflect.DeepEqual(stats(want), stats(got)) || want.EstimatedBytes() != got.EstimatedBytes() {
		t.Fatal("cache stats differ")
	}
	checkPointBytes(t, want, true)
	checkPointBytes(t, got, true)
	if a, b := want.pending.Load(), got.pending.Load(); a != nil && (b == nil || a.remaining.Load() != b.remaining.Load()) {
		t.Fatal("pending recovery counts differ")
	}
}

func TestPointRefMatchesAddPoint(t *testing.T)     { testPointRefMatchesAddPoint(t, false) }
func TestStreamSeriesMatchesAddPoint(t *testing.T) { testPointRefMatchesAddPoint(t, true) }
func testPointRefMatchesAddPoint(t *testing.T, series bool) {
	for _, mode := range []string{"plain", "bloom", "throttle", "stateful", "hashed", "recovery", "tags"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cs := [2]*Cache{New(), New()}
				channels := [2]chan string{make(chan string, 1), make(chan string, 1)}
				var calls [2][]string
				generation := 1
				for k, c := range cs {
					c.SetMaxSize(0)
					c.SetMaxBytes(1 << 30)
					c.InitCacheScanAdds()
					c.SetWriteoutBatching(4, time.Minute)
					c.SetWriteoutSpread(.2)
					c.SetWriteoutSparseSpread(true)
					c.SetWriteoutStartupSpread(true)
					c.EnableInputLog()
					c.SetNewMetricsChan(channels[k])
					if mode != "plain" {
						c.SetBloomSize(100)
					}
					if mode == "tags" {
						c.SetTagsEnabled(true)
					}
					if mode == "recovery" {
						if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
							t.Fatal(err)
						}
					}
					hook := func(p *points.Points, exists bool) bool {
						calls[k] = append(calls[k], fmt.Sprintf("%s/%t/%x/%d", p.Metric, exists, math.Float64bits(p.Data[0].Value), p.Data[0].Timestamp))
						return p.Data[0].Value < 0
					}
					if mode == "throttle" {
						c.SetThrottle(hook)
					}
					if mode == "stateful" || mode == "hashed" {
						c.SetThrottleWithState(func(p *points.Points, exists bool, state *any) bool {
							*state = generation // Model a namespace-route reload.
							return hook(p, exists)
						})
						if mode == "hashed" && k == 1 {
							c.SetThrottleWithHash(func(p *points.Points, exists bool, hash uint64, state *any) bool {
								if hash != xxh3.HashString(p.Metric) {
									t.Fatal("cache passed a different name hash")
								}
								*state = generation
								return hook(p, exists)
							})
						}
					}
				}
				refs := map[string]*PointRef{}
				seriesSlots := map[string]any{}
				check := func() {
					t.Helper()
					comparePointRefCaches(t, cs[0], cs[1])
					if !reflect.DeepEqual(calls[0], calls[1]) {
						t.Fatal("quota calls differ")
					}
					for len(channels[0]) > 0 {
						if len(channels[1]) == 0 || <-channels[0] != <-channels[1] {
							t.Fatal("notifications differ")
						}
					}
					if len(channels[1]) != 0 {
						t.Fatal("extra notification")
					}
				}
				add := func(name string, value float64, ts int64) {
					t.Helper()
					if refs[name] == nil {
						refs[name] = &PointRef{}
					}
					buf := []byte(name)
					cs[0].AddPoint(buf, value, ts)
					if series {
						slot := seriesSlots[name]
						if slot == nil {
							slot = cs[1].ResolveStreamName(buf)
						}
						cs[1].AddStreamPoint(buf, value, ts, &slot)
						seriesSlots[name] = slot
					} else {
						cs[1].AddPointRef(refs[name], buf, value, ts)
					}
					for i := range buf {
						buf[i] = 'X'
					}
					check()
				}
				name := "metric"
				if mode == "tags" {
					name = "metric;z=last;a=first"
				}
				for i := range 6 {
					add(name, float64(i), int64(i))
				}
				add(name, -1, -1)
				add(name, math.Float64frombits(0x8000000000000000), -2)
				key := name
				if mode == "tags" {
					key = "metric;a=first;z=last"
					add("metric;invalid", 1, 1)
				}
				for _, c := range cs {
					c.Pop(key)
				}
				add(name, 10, 10) // The old item is no longer cached.
				var flight [2]*points.Points
				for k, c := range cs {
					flight[k], _ = c.PopNotConfirmed(key)
				}
				add(name, 11, 11)
				for k, c := range cs {
					c.Requeue(flight[k])
				}
				add(name, 12, 12) // Requeue replaced a live item.
				generation++
				for k, c := range cs {
					c.SetBloomSize(100)
					channels[k] <- "full"
				}
				add(name, 13, 13) // Full notification channel leaves the item retryable.
				add(name, 14, 14)
				for _, c := range cs {
					c.SetWriteoutBatching(2, 2*time.Minute)
				}
				add(name, 15, 15)
				for _, c := range cs {
					checkQueueDeadlines(t, c, time.Now())
				}
				check()
				for _, c := range cs {
					c.SetMaxSize(1)
				}
				add(name, 16, 16) // Overflow on a warm ref.
				add("overflow.miss", 16, 16)
				for _, c := range cs {
					c.SetMaxSize(0)
					c.SetMaxBytes(1)
				}
				add(name, 17, 17) // Byte-budget overflow.
				for _, c := range cs {
					c.SetMaxBytes(1 << 30)
				}
				if mode == "recovery" {
					for _, c := range cs {
						if err := c.RecoverPending(nil, 0); err != nil {
							t.Fatal(err)
						}
					}
					add("other", 18, 18)
				}
				for _, c := range cs {
					p, ok := c.PopNotConfirmed(key)
					if ok {
						c.Confirm(p)
					}
				}
				add(name, 19, 19)
				var dump, xlog [2]bytes.Buffer
				for k, c := range cs {
					c.DivertToBinaryXlog(&xlog[k])
					if err := c.DumpBinary(&dump[k]); err != nil {
						t.Fatal(err)
					}
				}
				add(name, 20, 20)
				add("diverted.miss", 21, 21)
				if !bytes.Equal(dump[0].Bytes(), dump[1].Bytes()) || !bytes.Equal(xlog[0].Bytes(), xlog[1].Bytes()) {
					t.Fatal("dump/diversion differ")
				}
				for _, c := range cs {
					c.DivertToXlog(nil)
				}
				add(name, 22, 22)
				// Tag changes on a previously bound ref must use normalization.
				for _, c := range cs {
					c.SetTagsEnabled(true)
				}
				add(name, 23, 23)
			})
		})
	}
}

func TestPointRefSlotIdentity(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	var ref PointRef
	name := []byte("slot.metric")
	c.AddPointRef(&ref, name, 1, 1)
	old := c.data[ref.shard].items.slots[ref.slot].item
	sh := c.data[ref.shard]
	// Simulate any rehash, including one shrinking past the saved slot.
	sh.mu.Lock()
	sh.items = itemMap{}
	sh.items.set(old.Metric, old)
	ref.slot = ^uint32(0)
	sh.mu.Unlock()
	c.AddPointRef(&ref, name, 2, 2)
	if ref.generation != old.streamGeneration || ref.slot == ^uint32(0) || len(c.Get(string(name))) != 2 {
		t.Fatal("rehash did not refresh the ref")
	}
	c.Pop(string(name))
	c.AddPoint(name, 3, 3)
	c.AddPointRef(&ref, name, 4, 4)
	if ref.generation == old.streamGeneration || len(c.Get(string(name))) != 2 {
		t.Fatal("removed item was reused")
	}
	other := New()
	other.AddPointRef(&ref, name, 5, 5)
	if ref.cache != other || len(other.Get(string(name))) != 1 {
		t.Fatal("ref crossed cache ownership")
	}
	ref = PointRef{}
	other.AddPointRef(&ref, []byte("rebound.metric"), 6, 6)
	other.AddPointRef(nil, []byte("nil.metric"), 7, 7)
	if !other.Has("rebound.metric") || !other.Has("nil.metric") {
		t.Fatal("reset/nil ref lost a point")
	}
}

func TestPointRefConcurrentDumpDiversion(t *testing.T) {
	for range 10 {
		c := New()
		c.SetMaxSize(0)
		const writers, count = 8, 200
		var mu sync.Mutex
		seen := make(map[int]int)
		save := func(p *points.Points) error {
			mu.Lock()
			defer mu.Unlock()
			for _, point := range p.Data {
				seen[int(point.Value)]++
			}
			return nil
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := range writers {
			wg.Go(func() {
				name := []byte(fmt.Sprintf("dump.ref.%d", w))
				var ref PointRef
				<-start
				for i := range count {
					c.AddPointRef(&ref, name, float64(w*count+i), int64(i))
					if i%7 == 0 {
						runtime.Gosched()
					}
				}
			})
		}
		close(start)
		runtime.Gosched()
		c.DivertToPointWriter(save)
		if err := c.DumpPoints(save); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if len(seen) != writers*count {
			t.Fatalf("dump/diversion saved %d points", len(seen))
		}
		for _, n := range seen {
			if n != 1 {
				t.Fatalf("point saved %d times", n)
			}
		}
	}
}

func TestPointRefConcurrentPops(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	c.SetWriteoutBatching(16, time.Minute)
	c.SetBloomSize(1000)
	c.SetNewMetricsChan(make(chan string, 1))
	c.SetMetricExists(func(string) bool { return true })
	const writers, perWriter, series = 8, 3000, 16
	names := make([][]byte, series)
	for i := range names {
		names[i] = []byte(fmt.Sprintf("shared.metric.%d", i))
	}
	var counted atomic.Int64
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			for i, name := range names {
				p, ok := c.PopNotConfirmed(string(name))
				if !ok {
					continue
				}
				if i%3 == 0 {
					c.Requeue(p)
				} else {
					counted.Add(int64(len(p.Data)))
					c.Confirm(p)
				}
			}
		}
	})
	var producers sync.WaitGroup
	for w := range writers {
		producers.Go(func() {
			refs := make([]PointRef, series)
			for i := range perWriter {
				id := (i + w) % series
				c.AddPointRef(&refs[id], names[id], float64(w), int64(i))
			}
		})
	}
	producers.Wait()
	close(done)
	wg.Wait()
	for _, name := range names {
		if p, ok := c.Pop(string(name)); ok {
			counted.Add(int64(len(p.Data)))
		}
	}
	if counted.Load() != writers*perWriter || c.Size() != 0 {
		t.Fatalf("accepted points: %d, remaining %d", counted.Load(), c.Size())
	}
	// Verify all accepted points also reached the input log, including points
	// appended while a preceding batch was in flight or being requeued.
	var logged int
	for i := range shardCount {
		logged += len(c.TakeLogEntries(i, 1, nil))
	}
	if logged != writers*perWriter {
		t.Fatalf("logged %d points", logged)
	}
}

func TestPointRefRealRehash(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	var ref PointRef
	c.AddPointRef(&ref, []byte("rehash.metric"), 1, 1)
	shard := ref.shard
	for i := 0; c.data[shard].items.len() < 100; i++ {
		name := fmt.Sprintf("rehash.other.%d", i)
		if xxh3.HashString(name)&(shardCount-1) == uint64(shard) {
			c.AddPoint([]byte(name), 1, 1)
		}
	}
	c.AddPointRef(&ref, []byte("rehash.metric"), 2, 2)
	if len(c.Get("rehash.metric")) != 2 {
		t.Fatal("table growth lost a point")
	}
}
