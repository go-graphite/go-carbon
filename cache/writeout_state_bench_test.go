//go:build darwin || linux

package cache

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// Opt-in resident fixture; normal tests allocate nothing. Keep the point/name
// shape and shard count fixed when comparing revisions.
func TestWriteoutStateLoad(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("SAMPLER_METRICS"))
	if n == 0 {
		t.Skip("set SAMPLER_METRICS for the resident sampler experiment")
	}
	c := New()
	c.SetMaxSize(0)
	if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteoutSpread(.25); err != nil {
		t.Fatal(err)
	}
	c.SetWriteoutPacing(true)
	names := make([]string, n)
	data := make([]points.Point, 17)
	for i := range data {
		data[i] = points.Point{Value: float64(i), Timestamp: 1790000000 + int64(i*60)}
	}
	for i := range names {
		names[i] = fmt.Sprintf("services.namespace-%04d.host-%08d.component.requests.region.zone.measurement.value", i%1000, i)
		c.Add(&points.Points{Metric: names[i], Data: data})
	}
	// Model a full steady residence window, including overdue items, rather
	// than a synchronized fresh cache that can skip most index work.
	base := time.Now().Truncate(time.Second)
	for _, shard := range c.data {
		shard.mu.Lock()
		for _, it := range shard.items.all {
			it.firstArrival = base.Add(-time.Duration(it.slot%1200) * time.Second)
		}
		shard.mu.Unlock()
	}
	if fixture, ok := any(c).(interface{ refreshSamplerFixture() }); ok {
		fixture.refreshSamplerFixture()
	}
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	t.Logf("fixture metrics=%d points=%d shards=%d heap=%.1fMiB", n, n*17, shardCount, float64(mem.HeapAlloc)/(1<<20))
	profile := os.Getenv("SAMPLER_PROFILE")
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			t.Fatal(err)
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		defer f.Close()
	}
	c.WriteoutState() // Separate initialisation from steady sampling.
	cpu := func() time.Duration {
		var r syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
			t.Fatal(err)
		}
		return time.Duration(r.Utime.Sec+r.Stime.Sec)*time.Second + time.Duration(r.Utime.Usec+r.Stime.Usec)*time.Microsecond
	}
	start, cpuStart := time.Now(), cpu()
	samples := 0
	for samples < 20 || time.Since(start) < 2*time.Second {
		c.WriteoutState()
		samples++
	}
	t.Logf("idle sample wall=%s cpu=%s", time.Since(start)/time.Duration(samples), (cpu()-cpuStart)/time.Duration(samples))
	if profile != "" {
		pprof.StopCPUProfile()
	}
	measure := func(shard *Shard, now time.Time) WriteoutState {
		var state WriteoutState
		s := c.settings.Load().(*cacheSettings)
		for _, it := range shard.items.all {
			state.Points += int64(it.count())
			if !now.Before(writeoutDeadline(it, s)) {
				state.Eligible++
			}
			if !it.firstArrival.IsZero() {
				state.OldestAge = max(state.OldestAge, now.Sub(s.writeoutWall(it.firstArrival)))
			}
		}
		return state
	}
	if hook, ok := any(c).(interface {
		measureSamplerShard(*Shard, time.Time) WriteoutState
	}); ok {
		measure = hook.measureSamplerShard
	}
	var shardTotal, shardMax time.Duration
	var observed int64
	for _, shard := range c.data {
		shard.mu.RLock()
		at := time.Now()
		observed += measure(shard, at).Points
		held := time.Since(at)
		shardTotal += held
		shardMax = max(shardMax, held)
		shard.mu.RUnlock()
	}
	t.Logf("shard sampler work mean=%s max=%s observed_points=%d", shardTotal/shardCount, shardMax, observed)
	for _, sampling := range []bool{false, true, false, true} {
		var adds, pops, waits, waitNS, maxWait, samples, scanNS atomic.Int64
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for worker := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var completed int64
				defer func() { adds.Add(completed) }()
				state := uint64(worker + 1)
				var buf [128]byte
				for {
					select {
					case <-stop:
						return
					default:
					}
					state = state*6364136223846793005 + 1
					name := names[(state>>32)%uint64(n)]
					copy(buf[:], name)
					c.AddPoint(buf[:len(name)], 1, 1790000060)
					completed++
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var completed int64
			defer func() { pops.Store(completed) }()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				name := names[i%n]
				if p, ok := c.PopNotConfirmed(name); ok {
					c.Confirm(p)
					c.Add(&points.Points{Metric: name, Data: data})
					completed++
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			var probes, total, longest int64
			defer func() { waits.Store(probes); waitNS.Store(total); maxWait.Store(longest) }()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				shard := c.data[i%shardCount]
				at := time.Now()
				shard.mu.Lock()
				elapsed := time.Since(at).Nanoseconds()
				shard.mu.Unlock()
				probes++
				total += elapsed
				longest = max(longest, elapsed)
			}
		}()
		if sampling {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					at := time.Now()
					c.WriteoutState()
					scanNS.Add(time.Since(at).Nanoseconds())
					samples.Add(1)
					// Accelerate the five-second poll to expose contention locally.
					time.Sleep(100 * time.Millisecond)
				}
			}()
		}
		start, cpuStart = time.Now(), cpu()
		time.Sleep(3 * time.Second)
		close(stop)
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("sampling=%t add/s=%.0f pop/s=%.0f lock_mean=%s lock_max=%s samples=%d sample_mean=%s cpu/s=%.2f", sampling, float64(adds.Load())/elapsed.Seconds(), float64(pops.Load())/elapsed.Seconds(), time.Duration(waitNS.Load()/max(1, waits.Load())), time.Duration(maxWait.Load()), samples.Load(), time.Duration(scanNS.Load()/max(1, samples.Load())), (cpu()-cpuStart).Seconds()/elapsed.Seconds())
	}
	if os.Getenv("SAMPLER_RELOAD") == "1" {
		if err := c.SetWriteoutSpread(.2); err != nil {
			t.Fatal(err)
		}
		start, cpuStart = time.Now(), cpu()
		c.WriteoutState()
		t.Logf("one-off reload wall=%s cpu=%s", time.Since(start), cpu()-cpuStart)
	}
}

func BenchmarkSamplerIngest(b *testing.B) {
	for _, lifecycle := range []int{0, 17, 1} {
		b.Run(fmt.Sprintf("popEvery=%d", lifecycle), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
				b.Fatal(err)
			}
			if err := c.SetWriteoutSpread(.25); err != nil {
				b.Fatal(err)
			}
			c.SetWriteoutPacing(true)
			names := make([][]byte, 65536)
			for i := range names {
				names[i] = fmt.Appendf(nil, "sampler.ingest.metric.%08d", i)
				c.AddPoint(names[i], 1, 1790000000)
			}
			c.WriteoutState()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				name := names[i%len(names)]
				if lifecycle > 0 && i%lifecycle == 0 {
					c.Pop(string(name))
				}
				c.AddPoint(name, 1, 1790000060)
			}
		})
	}
}
