//go:build unix

package cache

import (
	"fmt"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func liveBenchCPU(b *testing.B) int64 {
	b.Helper()
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		b.Fatal(err)
	}
	return (int64(r.Utime.Sec)+int64(r.Stime.Sec))*1e9 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1000
}

func BenchmarkLiveSteady(b *testing.B) {
	for _, pop := range []bool{false, true} {
		b.Run(fmt.Sprintf("writeout=%t", pop), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
				b.Fatal(err)
			}
			c.SetWriteoutPacing(true)
			names := make([][]byte, 65536)
			for i := range names {
				names[i] = fmt.Appendf(nil, "services.application.namespace.host.%08d.component.region.requests.value", i)
				c.AddPoint(names[i], 1, 1790000000)
			}
			b.ReportAllocs()
			b.ResetTimer()
			cpu := liveBenchCPU(b)
			for i := range b.N {
				name := names[i%len(names)]
				c.AddPoint(name, 1, 1790000060)
				if pop && i%17 == 0 {
					if p, ok := c.PopNotConfirmed(string(name)); ok {
						c.Confirm(p)
					}
				}
			}
			cpu = liveBenchCPU(b) - cpu
			b.StopTimer()
			b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
		})
	}
}

func BenchmarkLiveQueue(b *testing.B) {
	for _, ready := range []bool{false, true} {
		b.Run(fmt.Sprintf("ready=%t", ready), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
				b.Fatal(err)
			}
			c.SetWriteoutPacing(true)
			for i := range 1000000 {
				name := fmt.Sprintf("services.application.queue.%08d.requests", i)
				if ready {
					c.AddRestored(points.OnePoint(name, 1, 1))
				} else {
					c.AddPoint([]byte(name), 1, 1)
				}
			}
			now := time.Now()
			c.makeQueueAt(now)
			b.ReportAllocs()
			b.ResetTimer()
			cpu := liveBenchCPU(b)
			for range b.N {
				c.makeQueueAt(now)
			}
			cpu = liveBenchCPU(b) - cpu
			b.StopTimer()
			b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
		})
	}
}

// An isolated production-sized shard separates scan work from lock waiting and
// the sink. Opt in to print wall-time distributions; these are not CPU samples.
func TestLiveShardCosts(t *testing.T) {
	if os.Getenv("LIVE_SHARD_COSTS") != "1" {
		t.Skip("set LIVE_SHARD_COSTS=1")
	}
	const count = 32768
	shard := New().data[0]
	settings := &cacheSettings{writeoutMinPoints: 960, writeoutMaxDelay: 960 * time.Second, writeoutPhase: time.Now()}
	for i := range count {
		name := fmt.Sprintf("services.application.shard.namespace.host.%08d.requests", i)
		it := newItem(name, 0)
		for j := range 34 {
			it.compact.append(float64(j%29), int64(j*60))
		}
		it.firstArrival = time.Now().Add(-time.Duration(i%1200) * time.Second)
		shard.items.set(name, it)
	}
	now := time.Now()
	noop := func(*item) {}
	shard.items.readyAt(now, settings, noop)
	legacy := make([]time.Duration, 0, 100)
	for range 100 {
		shard.mu.Lock()
		started := time.Now()
		shard.items.readyAt(now, settings, noop)
		legacy = append(legacy, time.Since(started))
		shard.mu.Unlock()
	}
	slices.Sort(legacy)
	t.Logf("whole-shard readyAt %d slots=%d median=%s p99=%s max=%s", count, len(shard.items.slots), legacy[50], legacy[98], legacy[99])
	// These admitted batches retain the same compact codec and shard mutex as
	// restored and live data; PopNotConfirmed includes its under-lock decode.
	c := New()
	c.SetMaxSize(0)
	for _, size := range []int{34, 1024, 16384} {
		data := make([]points.Point, size)
		for i := range data {
			data[i] = points.Point{Value: float64(i % 29), Timestamp: int64(i)}
		}
		var adds, pops time.Duration
		for range 100 {
			started := time.Now()
			c.AddRestored(&points.Points{Metric: "cost.restore", Data: data})
			adds += time.Since(started)
			started = time.Now()
			p, ok := c.PopNotConfirmed("cost.restore")
			if !ok {
				t.Fatal("missing restored points")
			}
			pops += time.Since(started)
			c.Confirm(p)
		}
		t.Logf("restore points=%d add_mean=%s pop_decode_mean=%s", size, adds/100, pops/100)
	}
}
