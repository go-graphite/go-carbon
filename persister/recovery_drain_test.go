package persister

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

func TestAutoRecoveryDrainDemand(t *testing.T) {
	for _, ceiling := range []int{-1, 0, 80000} {
		now := time.Now()
		s := cache.WriteoutState{Metrics: 1500000, Eligible: 1500000, MeanDelay: 1800 * time.Second, MaxDelay: 1800 * time.Second, Draining: true}
		c := autoUpdateController{}
		want := 300000
		if ceiling > 0 {
			want = ceiling
		}
		if got := c.step(now, s, 1, ceiling); got != want {
			t.Fatalf("drain ceiling %d: got %d want %d", ceiling, got, want)
		}
		s.Draining = false
		if got := c.step(now.Add(autoUpdateInterval), s, 1, ceiling); got > 860 {
			t.Fatal("completed drain leaked its high rate into steady state", got)
		}
	}
}

func TestRecoveryDrainPreservesFixedRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := cache.New()
		core.SetRecoveryDrain(true)
		var writes atomic.Uint64
		p := &Whisper{recv: func(exit chan bool) string {
			select {
			case <-exit:
				return ""
			default:
				return "restore.metric"
			}
		}}
		p.SetWorkers(1)
		p.SetMaxUpdatesPerSecond(20)
		p.SetBacklogged(core.Backlogged)
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(string) { writes.Add(1); time.Sleep(time.Millisecond) }, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if n := writes.Load(); n < 38 || n > 42 {
			t.Fatalf("recovery drain changed fixed 20/s dispatch: %d writes in 2s", n)
		}
	})
}

// Ordered restore runs before startupReady. Its faster rate must still pass
// through process CPU admission rather than the old pre-input bypass.
func TestAutoRecoveryDrainCPUAdmissionBeforeInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		var writes atomic.Uint64
		used := func() float64 { return .5*time.Since(started).Seconds() + float64(writes.Load())*.005 }
		budget := restartcpu.NewWithCPU(2, used)
		budget.Warmed()
		p := &Whisper{recv: func(exit chan bool) string {
			if writes.Load() >= 1200 {
				<-exit
				return ""
			}
			return "restore.metric"
		}}
		p.SetWorkers(4)
		p.SetMaxUpdatesPerSecond(-1)
		p.SetStartupGate(make(chan struct{}), make(chan struct{}), time.Hour, nil)
		p.SetRestartBudget(budget, nil)
		p.SetAutoUpdateRate(func() cache.WriteoutState {
			return cache.WriteoutState{Metrics: 1200, Eligible: 1200, MeanDelay: 1800 * time.Second, MaxDelay: 1800 * time.Second, Draining: true}
		}, 1, 1000)
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(string) { writes.Add(1); p.recordUpdate() }, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		for time.Since(started) < 10*time.Second {
			time.Sleep(20 * time.Millisecond)
			synctest.Wait()
			if used() > 2*time.Since(started).Seconds()+.25 {
				t.Fatalf("restore escaped restart CPU admission: used=%g elapsed=%s", used(), time.Since(started))
			}
			if writes.Load() >= 1200 {
				t.Logf("restore completed before input readiness in %v, CPU %.2fs", time.Since(started), used())
				return
			}
		}
		t.Fatal("restore did not use available restart CPU", writes.Load())
	})
}
