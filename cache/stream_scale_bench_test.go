package cache

import (
	"math/bits"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Opt-in: GOGC=200 GOMEMLIMIT=5GiB GOMAXPROCS=4 go test ./cache -run '^$' \
// -bench '^BenchmarkStreamScale$' -benchtime=3000000x -count=6 -benchmem.
// The fixture owns 10M identities AND cache items, plus 1 GiB of touched live
// heap. A single process reuses it across samples; new burst items are evicted
// outside the timer. Run base and candidate sequentially on a shared machine.
var streamScaleFixture = sync.OnceValue(func() *streamScaleData {
	f := &streamScaleData{c: New()}
	f.c.SetMaxSize(0)
	f.series = make([]*StreamSeries, 10_000_000)
	var name [64]byte
	for i := range f.series {
		n := append(name[:0], "scale.resident.metric."...)
		n = strconv.AppendInt(n, int64(i), 10)
		s := f.c.ResolveStreamName(n).(*StreamSeries)
		f.series[i] = s
		f.c.addStreamSeries(s, 1, 1)
	}
	for _, sh := range f.c.data {
		// Let a pending baseline rebuild finish before capturing resident
		// hits. A capped rebuild can replace identities even on a lookup.
		for _, slot := range sh.streamSeries {
			if s := slot.ref.Value(); s != nil {
				f.c.ResolveStreamName(s.StreamName())
				break
			}
		}
		if len(sh.streamSeries) != maxStreamSeriesSlots {
			panic("scale fixture has not reached the index cap")
		}
		f.saved = append(f.saved, slices.Clone(sh.streamSeries))
		for _, slot := range sh.streamSeries {
			if s := slot.ref.Value(); s != nil {
				f.hits = append(f.hits, s)
			}
		}
	}
	f.heap = make([]byte, 1<<30)
	for i := 0; i < len(f.heap); i += 4096 {
		f.heap[i] = byte(i / 4096)
	}
	runtime.GC()
	return f
})

type streamScaleData struct {
	c            *Cache
	series, hits []*StreamSeries
	saved        [][]seriesSlot
	heap         []byte
	epoch        int
}

func BenchmarkStreamScale(b *testing.B) {
	f := streamScaleFixture()
	for _, kind := range []string{"Hit", "Miss", "HourlyBurst"} {
		b.Run(kind, func(b *testing.B) {
			b.StopTimer()
			f.hits = f.hits[:0]
			for i, sh := range f.c.data {
				copy(sh.streamSeries, f.saved[i])
				for _, slot := range sh.streamSeries {
					if s := slot.ref.Value(); s != nil {
						f.c.ResolveStreamName(s.StreamName())
						break
					}
				}
				for _, slot := range sh.streamSeries {
					if s := slot.ref.Value(); s != nil {
						f.hits = append(f.hits, s)
					}
				}
			}
			f.epoch++
			burst := make([]*StreamSeries, 0)
			if kind == "HourlyBurst" {
				burst = make([]*StreamSeries, b.N)
			}
			// Hit-only workloads do not allocate enough to trigger GOGC. Force full
			// cycles as well as natural GC, so weak promotion is exercised during GC
			// even in the allocation-free case. No synthetic garbage enters B/op.
			stopGC := make(chan struct{})
			var gcWG sync.WaitGroup
			gcWG.Go(func() {
				ticker := time.NewTicker(250 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stopGC:
						return
					case <-ticker.C:
						runtime.GC()
					}
				}
			})
			done := make(chan struct{})
			startWork := make(chan struct{})
			var writers sync.WaitGroup
			var pointCount atomic.Int64
			if kind == "HourlyBurst" {
				for w := range 3 {
					writers.Go(func() {
						<-startWork
						var n int64
						var rand uint64 = uint64(w + 1)
						for {
							select {
							case <-done:
								pointCount.Add(n)
								return
							default:
							}
							rand = rand*6364136223846793005 + 1442695040888963407
							s := f.series[(rand>>16)%uint64(len(f.series))]
							f.c.addStreamSeries(s, float64(w), n+2)
							n++
						}
					})
				}
			}
			resetStreamLockTimes(f.c)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			started := time.Now()
			close(startWork)
			var rand uint64 = 1
			var name [80]byte
			for i := range b.N {
				if kind == "Hit" {
					rand = rand*6364136223846793005 + 1442695040888963407
					s := f.hits[(rand>>16)%uint64(len(f.hits))]
					if f.c.ResolveStreamName(s.StreamName()) != s {
						b.Fatal("fixture hit was not resident")
					}
				} else {
					n := append(name[:0], "scale.new.metric."...)
					n = strconv.AppendInt(n, int64(f.epoch), 10)
					n = append(n, '.')
					n = strconv.AppendInt(n, int64(i), 10)
					s := f.c.ResolveStreamName(n).(*StreamSeries)
					if kind == "HourlyBurst" {
						f.c.addStreamSeries(s, 3, int64(i))
						burst[i] = s
					}
				}
			}
			elapsed := time.Since(started)
			b.StopTimer()
			close(done)
			writers.Wait()
			close(stopGC)
			gcWG.Wait()
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(after.NumGC-before.NumGC), "GCs")
			b.ReportMetric(float64(after.HeapAlloc)/(1<<30), "heap-GiB")
			b.ReportMetric(float64(f.c.StreamSeriesIndexBytes())/(1<<20), "index-MiB")
			if n := pointCount.Load(); n > 0 {
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(n), "point-ns/op")
				b.ReportMetric(float64(n)/elapsed.Seconds(), "points/s")
			}
			reportStreamLockTimes(b, f.c)
			for _, s := range burst {
				f.c.Pop(s.name)
			}
			if kind == "HourlyBurst" {
				runtime.GC()
				// Background points must not accumulate across samples on a
				// memory-limited shared host. Use real eviction/admission to
				// restore one point per original name, outside the timer.
				for _, s := range f.series {
					f.c.Pop(s.name)
					f.c.addStreamSeries(s, 1, 1)
				}
				runtime.GC()
			}
			runtime.KeepAlive(f)
		})
	}
}

// A measurement-only overlay substitutes this for Shard.mu. The normal build
// keeps sync.RWMutex and pays no clock/histogram overhead. Run timing and
// throughput passes separately. RLock is exclusive in the measurement build,
// so its hold time can be recorded safely in the same per-shard histogram.
type streamBenchMutex struct {
	sync.RWMutex
	start time.Time
	holds [512]uint64
	max   uint64
}

func (m *streamBenchMutex) Lock() { m.RWMutex.Lock(); m.start = time.Now() }
func (m *streamBenchMutex) Unlock() {
	ns := uint64(time.Since(m.start).Nanoseconds())
	exponent := bits.Len64(ns)
	bucket := min(exponent*8+int((ns>>max(exponent-4, 0))&7), len(m.holds)-1)
	m.holds[bucket]++
	m.max = max(m.max, ns)
	m.RWMutex.Unlock()
}
func (m *streamBenchMutex) RLock()                             { m.Lock() }
func (m *streamBenchMutex) RUnlock()                           { m.Unlock() }
func (m *streamBenchMutex) streamHolds() ([512]uint64, uint64) { return m.holds, m.max }
func (m *streamBenchMutex) resetStreamHolds()                  { m.holds = [512]uint64{}; m.max = 0 }

func reportStreamLockTimes(b *testing.B, c *Cache) {
	var hist [512]uint64
	var total, maximum uint64
	measured := false
	for _, sh := range c.data {
		if m, ok := any(&sh.mu).(interface {
			streamHolds() ([512]uint64, uint64)
			resetStreamHolds()
		}); ok {
			measured = true
			h, mx := m.streamHolds()
			maximum = max(maximum, mx)
			for i, n := range h {
				hist[i] += n
				total += n
			}
			m.resetStreamHolds()
		}
	}
	if measured {
		if total == 0 {
			b.ReportMetric(0, "shard-p99-ns")
			b.ReportMetric(0, "shard-max-ns")
			b.ReportMetric(0, "shard-locks")
			return
		}
		var n uint64
		for i, count := range hist {
			n += count
			if n*100 >= total*99 {
				b.ReportMetric(float64(uint64(9+i%8)<<max(i/8-4, 0)), "shard-p99-ns")
				break
			}
		}
		b.ReportMetric(float64(maximum), "shard-max-ns")
		b.ReportMetric(float64(total), "shard-locks")
	}
}

func resetStreamLockTimes(c *Cache) {
	for _, sh := range c.data {
		if m, ok := any(&sh.mu).(interface{ resetStreamHolds() }); ok {
			m.resetStreamHolds()
		}
	}
}
