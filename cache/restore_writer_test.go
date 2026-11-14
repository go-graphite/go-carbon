package cache

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func restoreBits(data []points.Point) [][2]uint64 {
	out := make([][2]uint64, len(data))
	for i, p := range data {
		out[i] = [2]uint64{math.Float64bits(p.Value), uint64(p.Timestamp)}
	}
	return out
}

type restoreState struct {
	Points  map[string][][2]uint64
	Journal [ShardCount][]struct {
		Name string
		Bits [2]uint64
	}
	Generations [ShardCount]map[uint64]int32
	Space       [ShardCount]bool
	Size        int64
	Refused     uint64
}

func snapshotRestore(c *Cache) restoreState {
	out := restoreState{Points: make(map[string][][2]uint64), Size: c.Size(), Refused: c.stat.nameTooLong.Load()}
	for i, s := range c.data {
		s.mu.Lock()
		for _, slot := range s.items.slots {
			if slot.item != nil {
				out.Points[slot.item.Metric] = restoreBits(slot.item.compact.appendDecoded(nil))
			}
		}
		out.Generations[i] = make(map[uint64]int32)
		for gen, n := range s.log.items {
			out.Generations[i][gen] = n
		}
		out.Space[i] = s.log.restoreSpace(1) == nil
		s.mu.Unlock()
		for _, e := range c.TakeLogEntries(i, 1, nil) {
			out.Journal[i] = append(out.Journal[i], struct {
				Name string
				Bits [2]uint64
			}{e.Metric(), [2]uint64{math.Float64bits(e.Value), uint64(e.Timestamp)}})
		}
	}
	return out
}

// The serial oracle is the pre-change restore driver. Compare every value bit,
// timestamp and per-shard journal record, including recovered history (which
// must not be logged again), tag aliases, source ordering and rejected names.
func TestRestoreWriterDifferential(t *testing.T) {
	for _, spread := range []bool{false, true} {
		for _, workers := range []int{1, 4, 16} {
			t.Run(fmt.Sprintf("spread=%v/workers=%d", spread, workers), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					old, fast := New(), New()
					fast.SetRecoveryDrain(true)
					for _, c := range []*Cache{old, fast} {
						c.SetMaxSize(0)
						c.EnableInputLog()
						c.SetTagsEnabled(true)
						if err := c.SetWriteoutBatching(1000, time.Minute); err != nil {
							t.Fatal(err)
						}
						c.SetWriteoutRestoreSpread(spread)
					}
					var records []*points.Points
					bits := []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7fefffffffffffff}
					for i := range 3000 {
						name := fmt.Sprintf("restore.%d;b=2;a=1", i%79)
						if i%2 == 0 {
							name = fmt.Sprintf("restore.%d;a=1;b=2", i%79)
						}
						if i%97 == 0 {
							name = strings.Repeat("x", points.MaxMetricNameBytes+1)
						}
						data := make([]points.Point, 1+i%23)
						for j := range data {
							data[j] = points.Point{Value: math.Float64frombits(bits[(i+j)%len(bits)]), Timestamp: int64(1700000000 + i/4 - j)}
						}
						records = append(records, &points.Points{Metric: name, Data: data})
					}
					consume := func(c *Cache, p *points.Points) {
						if spread {
							c.AddRestored(p)
						} else {
							c.AddRestoredBeforeInput(p)
						}
					}
					w := fast.NewRestoreWriter(workers, func(p *points.Points) { consume(fast, p) })
					for _, p := range records {
						a, b := *p, *p
						consume(old, &a)
						w.Add(&b)
					}
					w.Flush()
					// Join a file before the next source is admitted, as the real driver does.
					for _, p := range records[:100] {
						a, b := *p, *p
						old.AddRecovered(&a)
						fast.AddRecovered(&b)
					}
					got, want := snapshotRestore(fast), snapshotRestore(old)
					if !reflect.DeepEqual(got, want) {
						t.Fatal("restored cache/journal/accounting differs from serial oracle")
					}
					// Continue after a journal generation change, then compare again.
					for _, p := range records[:100] {
						a, b := *p, *p
						consume(old, &a)
						w.Add(&b)
					}
					w.Flush()

					for i := range 79 {
						name := fmt.Sprintf("restore.%d;a=1;b=2", i)
						a, aok := old.PopNotConfirmed(name)
						b, bok := fast.PopNotConfirmed(name)
						if aok != bok || aok && !reflect.DeepEqual(restoreBits(a.Data), restoreBits(b.Data)) {
							t.Fatal("writeout differs", name)
						}
						if !reflect.DeepEqual(restoreBits(old.Get(name)), restoreBits(fast.Get(name))) {
							t.Fatal("inflight visibility differs", name)
						}
						if aok {
							if i%2 == 0 {
								old.Requeue(a)
								fast.Requeue(b)
							} else {
								old.Confirm(a)
								fast.Confirm(b)
							}
						}
					}
					if !reflect.DeepEqual(snapshotRestore(fast), snapshotRestore(old)) {
						t.Fatal("writeout/requeue accounting differs")
					}
					w.Close()
					if !reflect.DeepEqual(snapshotRestore(fast), snapshotRestore(old)) {
						t.Fatal("second generation differs")
					}
				})
			})
		}
	}
}

// Keep actual persister PopNotConfirmed/Confirm calls competing with restore.
// A fixed circulating history batch models ongoing writeout; persistence itself
// is outside the cache lock. SetBytes reports decoded point bytes, not dump bytes.
func BenchmarkRestoreWriterContended(b *testing.B) {
	for _, workers := range []int{-1, 0, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			c := New()
			c.SetRecoveryDrain(workers >= 0)
			c.SetMaxSize(0)
			const names = 32
			history := make([]*points.Points, names)
			for i := range history {
				data := make([]points.Point, 65536)
				for j := range data {
					data[j] = points.Point{Value: float64(j % 71), Timestamp: int64(j)}
				}
				history[i] = &points.Points{Metric: fmt.Sprintf("history.%d", i), Data: data}
				c.AddRestoredBeforeInput(history[i])
			}
			templates := make([]compactPoints, names)
			for i, p := range history {
				templates[i] = c.GetShard(p.Metric).items.get(p.Metric).compact
			}
			var wg sync.WaitGroup
			stop := make(chan struct{})
			for worker := range 32 {
				wg.Go(func() {
					for {
						for i := worker; i < names; i += 32 {
							select {
							case <-stop:
								return
							default:
							}
							if p, ok := c.PopNotConfirmed(history[i].Metric); ok {
								c.Confirm(p)
								// Refill from an immutable template, excluding ingest encoding
								// from the simulated persister's critical section.
								shard := c.GetShard(history[i].Metric)
								shard.mu.Lock()
								it := newItem(history[i].Metric, 0)
								it.compact = templates[i]
								shard.items.set(it.Metric, it)
								c.addSizeLocked(shard, int64(it.count()), 0)
								c.syncMemoryLocked(shard)
								shard.mu.Unlock()
								time.Sleep(100 * time.Microsecond)
							}
						}
					}
				})
			}
			data := make([]points.Point, 1024)
			for i := range data {
				data[i] = points.Point{Value: float64(i), Timestamp: int64(i)}
			}
			// Pop restored metrics as a second, ordinary persister set to bound memory.
			restored := make([]string, names)
			for i := range restored {
				for n := 0; ; n++ {
					candidate := fmt.Sprintf("restored.%d.%d", i, n)
					if c.GetShard(candidate) == c.GetShard(history[i].Metric) {
						restored[i] = candidate
						break
					}
				}
			}
			for worker := range 8 {
				wg.Go(func() {
					for {
						for i := worker; i < names; i += 8 {
							select {
							case <-stop:
								return
							default:
							}
							if p, ok := c.PopNotConfirmed(restored[i]); ok {
								c.Confirm(p)
							}
						}
					}
				})
			}
			var w *RestoreWriter
			if workers > 0 {
				w = c.NewRestoreWriter(workers, c.AddRestoredBeforeInput)
			}
			b.SetBytes(int64(len(data) * 16))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				p := &points.Points{Metric: restored[i%names], Data: data}
				if w == nil {
					c.AddRestoredBeforeInput(p)
				} else {
					w.Add(p)
				}
			}
			if w != nil {
				w.Close()
			}
			b.StopTimer()
			close(stop)
			wg.Wait()
		})
	}
}

// A full restored staging buffer must release a waiting worker only after its
// actual journal swap, and Flush must include that delayed final record.
func TestRestoreWriterJournalPressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		c.SetMaxSize(0)
		c.EnableInputLog()
		data := make([]points.Point, maxShardRestoredLogEntries)
		for i := range data {
			data[i] = points.Point{Value: float64(i), Timestamp: int64(i)}
		}
		c.AddRestoredBeforeInput(&points.Points{Metric: "pressure", Data: data})
		w := c.NewRestoreWriter(4, c.AddRestoredBeforeInput)
		w.Add(points.OnePoint("pressure", math.Copysign(0, -1), -1))
		synctest.Wait()
		shard := c.GetShard("pressure")
		shard.mu.Lock()
		if shard.log.waitDrained == nil || shard.log.entries.Len() != len(data) {
			t.Fatal("restore did not wait at exact journal limit")
		}
		shard.mu.Unlock()
		for i := range c.data {
			c.TakeLogBatch(i, 1, inputlog.Batch{})
		}
		w.Flush()
		w.Close()
		got := c.Get("pressure")
		if len(got) != len(data)+1 || math.Float64bits(got[len(data)].Value) != 1<<63 || got[len(data)].Timestamp != -1 {
			t.Fatal("pressure lost or reordered record")
		}
	})
}

// Actual concurrent pop/confirmation must conserve the full per-metric stream,
// including correction order, when restore and writeout race on the same item.
func TestRestoreWriterConcurrentWriteout(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.SetRecoveryDrain(true)
	c.EnableInputLog()
	const names = 32
	want := make([][][2]uint64, names)
	got := make([][][2]uint64, names)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range names {
		wg.Go(func() {
			name := fmt.Sprintf("concurrent.%d", i)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if p, ok := c.PopNotConfirmed(name); ok {
					got[i] = append(got[i], restoreBits(p.Data)...)
					c.Confirm(p)
				} else {
					runtime.Gosched()
				}
			}
		})
	}
	w := c.NewRestoreWriter(16, c.AddRestoredBeforeInput)
	for i := range 16000 {
		n := i % names
		p := points.OnePoint(fmt.Sprintf("concurrent.%d", n), math.Float64frombits(uint64(i)*0x9e3779b97f4a7c15), int64(i/3-i%7))
		want[n] = append(want[n], restoreBits(p.Data)...)
		w.Add(p)
	}
	w.Close()
	close(stop)
	wg.Wait()
	for i := range names {
		got[i] = append(got[i], restoreBits(c.Get(fmt.Sprintf("concurrent.%d", i)))...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("concurrent writeout lost, duplicated or reordered points")
	}
	logged := make([][][2]uint64, names)
	for i := range c.data {
		for _, e := range c.TakeLogEntries(i, 1, nil) {
			var n int
			if _, err := fmt.Sscanf(e.Metric(), "concurrent.%d", &n); err != nil {
				t.Fatal(err)
			}
			logged[n] = append(logged[n], [2]uint64{math.Float64bits(e.Value), uint64(e.Timestamp)})
		}
	}
	if !reflect.DeepEqual(logged, want) {
		t.Fatal("concurrent writeout changed input-log bits/order")
	}
}

func TestRestoreWriterReadAheadBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		entered, release, added := make(chan struct{}), make(chan struct{}), make(chan struct{})
		w := c.NewRestoreWriter(4, func(p *points.Points) {
			if len(p.Data) > maxRestorePendingPoints {
				close(entered)
				<-release
			}
			// The callback owns this record, so it may release its decoded data.
			p.Data = nil
		})
		w.Add(&points.Points{Metric: "oversized", Data: make([]points.Point, maxRestorePendingPoints+1)})
		<-entered
		go func() { w.Add(points.OnePoint("next", 1, 1)); close(added) }()
		synctest.Wait()
		select {
		case <-added:
			t.Fatal("oversized record did not run alone")
		default:
		}
		close(release)
		<-added
		w.Close()
		if w.pendingPoints != 0 {
			t.Fatal("read-ahead reservation leaked", w.pendingPoints)
		}
	})
}
