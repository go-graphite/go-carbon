package persister

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

func TestAutoRestartFloorOverridesCeiling(t *testing.T) {
	t.Parallel()
	for _, history := range []bool{false, true} {
		t.Run(map[bool]string{false: "queue_floor", true: "saved_floor"}[history], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const floor = 37900
				const ceiling = 26500
				started := time.Now()
				var writes atomic.Uint64
				budget := restartcpu.NewWithCPU(2, func() float64 { return .3*time.Since(started).Seconds() + float64(writes.Load())*.0001 })
				if history {
					budget.SetSteadyRate(floor)
				}
				budget.Warmed()
				var drained atomic.Bool
				budget.SetDrain(drained.Load)
				time.Sleep(31 * time.Minute)
				p := &Whisper{recv: func(chan bool) string { return "floor.metric" }}
				p.SetWorkers(96)
				p.SetMaxUpdatesPerSecond(-1)
				p.SetRestartBudget(budget, func() float64 {
					if history {
						return 100
					}
					return floor
				})
				state := cache.WriteoutState{Metrics: 30_000_000, Eligible: 7_000_000, MaxDelay: 960 * time.Second, MeanDelay: 960 * time.Second}
				p.SetAutoUpdateRate(func() cache.WriteoutState { return state }, 1, ceiling)
				p.SetMockStore(func() (StoreFunc, func()) {
					return func(string) { time.Sleep(time.Millisecond); writes.Add(1); p.recordUpdate() }, nil
				})
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				defer p.Stop()
				check := func(want float64) {
					t.Helper()
					time.Sleep(2 * time.Second)
					synctest.Wait()
					previous := writes.Load()
					time.Sleep(3 * time.Second)
					synctest.Wait()
					rate := float64(writes.Load()-previous) / 3
					applied, target, reason := p.autoUpdateObservation()
					t.Logf("active=%v actual=%.1f/s applied=%.0f target=%.0f reason=%s", budget.Active(), rate, applied, target, reason)
					if applied != want || target != want || rate < want*.95 || rate > want*1.02 {
						t.Fatalf("mandatory/steady rate: actual %.1f want %.0f applied %.0f target %.0f", rate, want, applied, target)
					}
				}
				check(floor)
				drained.Store(true)
				check(ceiling)
				if budget.Active() {
					t.Fatal("restart admission did not finish")
				}
			})
		})
	}
}
