//go:build unix

package cache

import (
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/inputlog/loadgen"
)

func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// TestInputLogSustainedRate drives the cache at a production-like rate with
// and without the log and reports what the log writes and costs, then how
// fast the log replays. The metrics are container-style names of 60-200
// bytes, each sending about one point per second as on a busy store, with
// values of counters, gauges, constants and noise. Run it explicitly:
//
//	INPUTLOG_LOAD_SECONDS=30 go test -run TestInputLogSustainedRate -v ./cache/
func TestInputLogSustainedRate(t *testing.T) {
	seconds, _ := strconv.Atoi(os.Getenv("INPUTLOG_LOAD_SECONDS"))
	if seconds <= 0 {
		t.Skip("set INPUTLOG_LOAD_SECONDS to run")
	}
	rate := 4_500_000
	if r, _ := strconv.Atoi(os.Getenv("INPUTLOG_LOAD_RATE")); r > 0 {
		rate = r
	}
	mix := loadgen.New(rate, 1) // about one point per metric per second
	t.Logf("%d metrics, mean name %.0f bytes", len(mix.Names), mix.MeanNameLength())
	for _, enabled := range []bool{false, true} {
		dir := t.TempDir()
		c := New()
		c.SetMaxSize(0)
		var l *inputlog.Log
		if enabled {
			l, _ = startInputLog(t, dir, c, inputlog.Options{SyncInterval: time.Second, MaxAge: 30 * time.Minute,
				TruncateLag: 5 * time.Second})
		}
		sent, elapsed, cpu := driveLoad(c, mix, rate, time.Duration(seconds)*time.Second, func() {
			if l != nil {
				l.Stat(func(string, float64) {}) // reset counters after warm-up
			}
		})
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		t.Logf("log=%v: %.2fM points/s, process CPU %.2f cores, heap %.0f MB for %d metrics", enabled,
			float64(sent)/elapsed.Seconds()/1e6, cpu.Seconds()/elapsed.Seconds(), float64(mem.HeapAlloc)/1e6, c.Len())
		if l == nil {
			continue
		}
		stats := map[string]float64{}
		l.Stat(func(metric string, value float64) { stats[metric] = value })
		per := elapsed.Seconds()
		t.Logf("log: %.1f MB/s, %.2f B/point, %.2f writes/s, %.2f fdatasyncs/s (%.1f ms avg), %.1f frames/s, "+
			"%.2f segment rolls/s (%v created, %v recycled), %v live + %v spare segments, %v dropped, %v errors",
			stats["bytes"]/per/1e6, stats["bytes"]/stats["points"], stats["writes"]/per, stats["syncs"]/per,
			stats["syncTimeMs"]/max(stats["syncs"], 1), stats["frames"]/per, (stats["segmentsCreated"]+stats["segmentsRecycled"])/per,
			stats["segmentsCreated"], stats["segmentsRecycled"], stats["liveSegments"], stats["spareSegments"], stats["dropped"], stats["errors"])
		l.Kill()

		restarted := New()
		restarted.SetMaxSize(0)
		l, replay := startInputLog(t, dir, restarted, inputlog.Options{SyncInterval: time.Second})
		t.Logf("replay: %d points, %.1f MB in %v: %.2fM points/s, %.0f MB/s", replay.Points, float64(replay.Bytes)/1e6,
			replay.Duration.Round(time.Millisecond), float64(replay.Points)/replay.Duration.Seconds()/1e6, float64(replay.Bytes)/replay.Duration.Seconds()/1e6)
		l.Kill()
	}
}

// driveLoad adds rate points/s for warm-up plus d, while a persister writes
// each metric out about every 30s. It returns what was measured after warm-up.
func driveLoad(c *Cache, mix *loadgen.Mix, rate int, d time.Duration, warmedUp func()) (int64, time.Duration, time.Duration) {
	const producers, tick = 8, 10 * time.Millisecond
	const warmup, persistEvery = 10 * time.Second, 30 * time.Second
	names := mix.Names
	var sent atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			first := p * len(names) / producers
			mine := names[first : (p+1)*len(names)/producers]
			perTick := max(rate/producers/int(time.Second/tick), 1)
			r := rand.New(rand.NewPCG(uint64(p), 7))
			ticker := time.NewTicker(tick)
			defer ticker.Stop()
			for i := 0; ; {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				now := time.Now().Unix()
				for range perTick {
					m := i % len(mine)
					c.AddPoint(mine[m], mix.Next(first+m, r), now)
					i++
				}
				sent.Add(int64(perTick))
			}
		})
	}
	wg.Go(func() {
		perTick := max(len(names)/int(persistEvery/tick), 1)
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for i := 0; ; {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			for range perTick {
				if p, ok := c.PopNotConfirmed(string(names[i%len(names)])); ok {
					c.Confirm(p)
				}
				i++
			}
		}
	})
	time.Sleep(warmup)
	warmedUp()
	start, startCPU, startSent := time.Now(), cpuTime(), sent.Load()
	time.Sleep(d)
	elapsed, cpu, n := time.Since(start), cpuTime()-startCPU, sent.Load()-startSent
	close(stop)
	wg.Wait()
	return n, elapsed, cpu
}
