package persister

import (
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
)

func TestLiveDemandOrdinaryLimits(t *testing.T) {
	for _, ceiling := range []int{75000, 0, -1} {
		for _, pressure := range []bool{false, true} {
			t.Run(fmt.Sprintf("ceiling=%d/stalled=%v", ceiling, pressure), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					// No restart budget: ordinary input must stand on its own.
					p := &Whisper{}
					p.SetWorkers(96)
					p.autoUpdate.floor, p.autoUpdate.ceiling = 25000, ceiling
					defer p.stopAutoUpdateRate()
					s := cache.WriteoutState{Metrics: 100000, Points: 1000000, MeanDelay: 960 * time.Second, MaxDelay: 960 * time.Second}
					p.autoUpdate.capacitySample = healthyCapacity()
					p.autoUpdate.capacitySample.Stalled = pressure
					p.sampleAutoUpdateRateLocked(s)
					time.Sleep(5 * time.Second)
					s.LivePoints += 5000000 // 1M points/s in confirmed batches of ten.
					p.writeStats.shared.points.Store(5000000)
					p.writeStats.shared.updates.Store(500000)
					p.sampleAutoUpdateRateLocked(s)
					rate := p.autoUpdate.ordinary
					switch {
					case ceiling > 0:
						if rate != ceiling {
							t.Fatalf("live demand did not respect explicit ceiling: %d", rate)
						}
					case ceiling == -1 && pressure:
						if rate >= 100000 {
							t.Fatalf("live demand bypassed hard storage pressure: %d", rate)
						}
					case rate < 103000:
						t.Fatalf("ordinary live demand was underfunded: %d", rate)
					}
					for range 120 {
						time.Sleep(5 * time.Second)
						p.sampleAutoUpdateRateLocked(s)
					}
					if got := p.autoUpdate.ordinary; got > 25100 {
						t.Fatalf("quiet input retained learned floor: %d", got)
					}
				})
			})
		}
	}
}

// Exercise the cache, eligibility queue, real worker/token driver and automatic
// sampler together. Only the storage syscall is replaced by a confirmed write.
func TestLiveDemandSurvivesRestartWindow(t *testing.T) {
	// Preserve every demand/floor ratio and the high-rate (>1000/s) token
	// path while avoiding millions of distinct names under the race detector.
	const scale = 20
	// Admission counts metric writes; retain the 3:2 surge in points per
	// write without copying 960 identical values into every cache entry.
	const batchPoints = 16
	synctest.Test(t, func(t *testing.T) {
		c := cache.New()
		c.SetMaxSize(40_000_000_000 / scale)
		if err := c.SetWriteoutBatching(batchPoints, 960*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := c.SetWriteoutSpread(.25); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		budget := restartcpu.NewWithCPU(88, func() float64 { return 30 * time.Since(started).Seconds() })
		budget.Warmed()
		p := &Whisper{recv: c.WriteoutQueue().Get}
		p.SetWorkers(96)
		p.SetMaxUpdatesPerSecond(-1)
		p.SetAutoUpdateRate(c.WriteoutState, 25000/scale, 75000/scale)
		p.SetWriteoutReady(c.WriteoutReady)
		p.SetRestartBudget(budget, nil)
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(name string) {
				if batch, ok := c.PopNotConfirmed(name); ok {
					p.writeStats.shared.observe(len(batch.Data), time.Microsecond, nil)
					p.committedPointsTotal.Add(uint64(len(batch.Data)))
					c.Confirm(batch)
					p.recordUpdate()
				}
			}, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		// An empty cold start, with no replay or manufactured counter samples.
		// Move through the idle part without executing millions of empty ticks.
		time.Sleep(29*time.Minute + 40*time.Second)
		synctest.Wait()
		var accepted uint64
		var name int
		for second := range 60 {
			batchSize := batchPoints
			if second >= 25 && second < 40 {
				batchSize = batchPoints * 3 / 2
			}
			count := 40000 / scale
			if second >= 45 { // Falling demand must release its learned floor.
				count = 3000 / scale
			}
			data := make([]points.Point, batchSize)
			for i := range data {
				data[i] = points.Point{Value: 1, Timestamp: time.Now().Unix()}
			}
			for range count {
				name++
				c.Add(&points.Points{Metric: "live." + strconv.Itoa(name), Data: data})
			}
			accepted += uint64(count * batchSize)
			time.Sleep(time.Second)
			synctest.Wait()
			s := c.WriteoutState()
			if s.LivePoints != accepted || s.ReplayPoints != 0 || s.Restored {
				t.Fatalf("cold live accounting: accepted=%d state=%+v", accepted, s)
			}
			if second%5 == 4 {
				// Include a quiet second so queue refresh latency cannot masquerade
				// as a throughput deficit. Offered load still exceeds the configured floor.
				time.Sleep(time.Second)
				synctest.Wait()
				s = c.WriteoutState()
			}
			if second >= 10 && second%5 == 4 && second < 45 {
				rate, _, reason := p.autoUpdateObservation()
				if !budget.Active() && rate > 75000/scale {
					t.Fatalf("live demand escaped auto maximum: %.0f", rate)
				}
				t.Logf("elapsed=%s accepted=%d committed=%d cache=%d oldest=%s rate=%.0f reason=%s", time.Since(started), accepted, p.CommittedPoints(), s.Points, s.OldestAge, rate, reason)
				if p.CommittedPoints() != accepted || s.Points != 0 || s.OldestAge > time.Second {
					t.Fatalf("live input underfunded across restart expiry at %s: accepted=%d committed=%d cache=%d oldest=%s", time.Since(started), accepted, p.CommittedPoints(), s.Points, s.OldestAge)
				}
			}
		}
		if budget.Active() || p.mandatoryRestartRate() != 0 {
			t.Fatal("test did not leave temporary restart admission")
		}
		time.Sleep(15 * time.Minute)
		synctest.Wait()
		if rate, _, _ := p.autoUpdateObservation(); rate > 25250.0/scale {
			t.Fatalf("falling input retained excessive admission: %.0f", rate)
		}
		if p.CommittedPoints() != accepted || !c.IsEmpty() {
			t.Fatal("accepted points were not all committed")
		}
	})
}
