package restartcpu

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// legacyAdmission freezes the pre-reservation, 20ms admission algorithm for a
// differential control experiment. It runs on the same fake meter and clock.
type legacyAdmission struct {
	at           time.Time
	used, credit float64
	read         func() float64
}

func (l *legacyAdmission) delay() time.Duration {
	now := time.Now()
	if now.Sub(l.at) >= 20*time.Millisecond {
		used := l.read()
		l.credit = max(-24*.1, min(24*.04, l.credit+now.Sub(l.at).Seconds()*24-(used-l.used)))
		l.at, l.used = now, used
	}
	if l.credit >= 0 {
		return 0
	}
	return min(100*time.Millisecond, max(20*time.Millisecond, time.Duration(-l.credit/24*float64(time.Second))))
}

type simulationResult struct {
	seconds, drainSeconds, checkpointSeconds, peak, extraMinutes, minRate, meanRate float64
	drained                                                                         bool
}

// This is a capacity model, not a replay of a CPU profile: fixed input/read
// demand is 8 cores, required preparation .5 core, steady writes 7.5 cores at
// 13.7k/s. A cold update costs up to 1.5x a warm update. The checkpoint has
// 17.7M entries, 70s serial service time and 18.47 CPU seconds. Initial backlog
// is 45s of steady updates; steady input continues throughout recovery. Small
// aggregate jobs avoid allocating millions of fake metrics or burning CPU.
func TestRestartPacingSimulation(t *testing.T) {
	for _, mode := range []string{"before_capped", "before_uncapped", "after"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const floor = 13700.
				const entries = 17700000
				started := time.Now()
				var spent atomic.Int64
				var loaded atomic.Int64
				read := func() float64 { return 8.5*time.Since(started).Seconds() + float64(spent.Load())/1e9 }
				b := NewWithCPU(24, read)
				b.SetSteadyRate(floor)
				b.ObserveWriteout(floor, 0)
				b.BeginWriteout()
				defer b.EndWriteout()
				b.BeginPrefetch()
				defer b.EndPrefetch()
				legacy := &legacyAdmission{at: started, read: read}
				// The stream and writeout share the same controller and CPU meter.
				var legacyMu sync.Mutex
				legacyDelay := func() time.Duration {
					legacyMu.Lock()
					defer legacyMu.Unlock()
					return legacy.delay()
				}
				ctx, cancel := context.WithCancel(context.Background())
				checkpointDone := make(chan struct{})
				var checkpointSeconds float64
				go func() {
					defer close(checkpointDone)
					var p *Checkpoint
					if mode == "after" {
						p = b.NewCheckpoint(nil)
						defer p.Finish()
					}
					for loaded.Load() < entries {
						if p != nil {
							if p.Wait(ctx) != nil {
								return
							}
						} else {
							for {
								delay := legacyDelay()
								if delay == 0 {
									break
								}
								timer := time.NewTimer(delay)
								select {
								case <-ctx.Done():
									timer.Stop()
									return
								case <-timer.C:
								}
							}
						}
						n := min(int64(4096), entries-loaded.Load())
						wall := time.Duration(float64(n) / entries * 70 * float64(time.Second))
						timer := time.NewTimer(wall)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
						spent.Add(int64(float64(n) / entries * 18.47 * 1e9))
						loaded.Add(n)
					}
					checkpointSeconds = time.Since(started).Seconds()
				}()
				// Simulation tasks are serialized by the fake scheduler; sync at every
				// clock step also makes observation of the completed checkpoint explicit.
				pending, tokens := floor*45, 0.
				ceiling := 3 * floor
				if mode == "before_capped" {
					ceiling = 10000
				}
				previousCPU, previousWrites, writes := 0., 0., 0.
				nextAdmission := started
				result := simulationResult{minRate: math.Inf(1)}
				for step := 1; step <= 600000; step++ {
					time.Sleep(time.Millisecond)
					synctest.Wait()
					now := time.Now()
					pending += floor * .001
					tokens = min(ceiling*.04, tokens+ceiling*.001)
					if !now.Before(nextAdmission) {
						for tokens >= 8 && pending >= 8 {
							delay := time.Duration(0)
							if mode == "after" {
								delay = b.reserveDelay(now, true, 8*b.writeCost())
							} else {
								delay = legacyDelay()
							}
							if delay > 0 {
								nextAdmission = now.Add(delay)
								break
							}
							cost := 7.5 / floor * (1 + .5*(1-float64(loaded.Load())/entries))
							spent.Add(int64(8 * cost * 1e9))
							pending -= 8
							tokens -= 8
							writes += 8
						}
					}
					if step%1000 == 0 {
						cpu := read()
						rate := writes - previousWrites
						cores := cpu - previousCPU
						result.peak = max(result.peak, cores)
						result.extraMinutes += max(0, cores-16) / 60
						result.minRate = min(result.minRate, rate)
						b.ObserveWriteout(floor, rate)
						previousCPU, previousWrites = cpu, writes
					}
					if pending < 16 && result.drainSeconds == 0 {
						result.drainSeconds = time.Since(started).Seconds()
					}
					if loaded.Load() == entries && pending < 16 {
						result.drained = true
						break
					}
				}
				result.seconds = time.Since(started).Seconds()
				if tail := result.seconds - math.Floor(result.seconds); tail > 0 {
					result.extraMinutes += max(0, read()-previousCPU-16*tail) / 60
				}
				result.meanRate = writes / result.seconds
				cancel()
				<-checkpointDone
				result.checkpointSeconds = checkpointSeconds
				t.Logf("drained=%t drain=%.3fs recovery=%.3fs checkpoint=%.3fs peak_1s_cpu=%.3f allowance=24 extra_core_minutes=%.3f rate_min_1s=%.0f rate_mean=%.0f floor=13700", result.drained, result.drainSeconds, result.seconds, result.checkpointSeconds, result.peak, result.extraMinutes, result.minRate, result.meanRate)
				if mode == "after" {
					if !result.drained || result.checkpointSeconds > 600 || result.peak > 24.5 || result.minRate < .99*floor || result.extraMinutes > 16 {
						t.Fatalf("restart capacity goals failed: %+v", result)
					}
				} else if mode == "before_capped" {
					if result.drained || result.meanRate >= floor || result.peak >= 24 {
						t.Fatalf("did not reproduce spare-CPU shortfall: %+v", result)
					}
				}
			})
		})
	}
}
