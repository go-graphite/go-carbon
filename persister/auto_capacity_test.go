package persister

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/hostcapacity"
)

func TestRecoveryPressureBridgesFiftySeventyGap(t *testing.T) {
	s := cache.WriteoutState{Points: 10_000_000_000, MaxPoints: 20_000_000_000, Metrics: 40_000_000, Eligible: 40_000_000, MaxDelay: 1800 * time.Second, MeanDelay: 1800 * time.Second, RecoveryBlocked: true}
	var controller autoUpdateController
	if rate := controller.step(time.Now(), s, 1, 0); rate < 400000 || controller.reason != "recovery_blocked" {
		t.Fatal("blocked replay cannot accelerate drain below 70%", rate, controller.reason)
	}
	controller = autoUpdateController{}
	if rate := controller.step(time.Now(), s, 1, 40000); rate != 40000 {
		t.Fatal("positive fixed override lost", rate)
	}
}

func TestCapacitySampleCannotOverwriteReload(t *testing.T) {
	p := &Whisper{}
	p.liveSettings.Store(&whisperSettings{maxUpdatesPerSecond: -1, updateTicker: helper.NewThrottleTicker(0)})
	state := func() cache.WriteoutState { return cache.WriteoutState{Metrics: 1000000, MeanDelay: time.Second} }
	p.SetAutoUpdateRate(state, 1, -1)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.SetAutoCapacity(func() AutoCapacitySample { close(entered); <-release; return healthyCapacity() })
	go func() { defer close(done); p.sampleAutoUpdateRate() }()
	<-entered
	next := &Whisper{}
	next.SetMaxUpdatesPerSecond(-1)
	next.SetAutoUpdateRate(state, 1, 1000)
	p.ReloadSettings(next)
	close(release)
	<-done
	defer p.stopAutoUpdateRate()
	if rate, _, _ := p.autoUpdateObservation(); rate != 1000 {
		t.Fatal("old sample replaced operator override", rate)
	}
}

func TestCapacityTokenDriverGrowsAndBacksOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var stalled atomic.Bool
		p := &Whisper{recv: func(chan bool) string { return "queued" }}
		p.SetWorkers(4)
		p.SetMaxUpdatesPerSecond(-1)
		p.SetAutoUpdateRate(func() cache.WriteoutState {
			return cache.WriteoutState{Metrics: 1000, Eligible: 1000000, Points: 10000000, MaxPoints: 20000000, MeanDelay: time.Second, MaxDelay: time.Second, RecoveryBlocked: true}
		}, 100, -1)
		p.SetAutoCapacity(func() AutoCapacitySample {
			s := healthyCapacity()
			written, _ := p.WriteStats()
			s.Host.ProcessCPU = float64(written.Updates) * .00001
			s.Stalled = stalled.Load()
			return s
		})
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(string) { p.writeStats.shared.observe(16, 100*time.Microsecond, nil); p.recordUpdate() }, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		initial, _, _ := p.autoUpdateObservation()
		time.Sleep(45 * time.Second)
		peak, _, _ := p.autoUpdateObservation()
		if peak <= initial {
			t.Fatal("actual token-bound workers never grew", initial, peak)
		}
		stalled.Store(true)
		time.Sleep(6 * time.Second)
		after, _, _ := p.autoUpdateObservation()
		if after >= peak {
			t.Fatal("stalled storage did not reduce installed ticker", peak, after)
		}
		t.Logf("real worker/token driver: %.0f -> %.0f -> %.0f updates/s", initial, peak, after)
	})
}

func healthyCapacity() AutoCapacitySample {
	return AutoCapacitySample{Host: hostcapacity.Sample{Valid: true, DiskValid: true, MemoryValid: true, Idle: .95, Memory: .7, DiskBusy: .05}, L0: 1, L0Stop: 12, SpaceValid: true, FreeBytes: 10 << 40, DiskBytes: 20 << 40}
}

func TestAutoCapacityGrowsOnTokensThenBacksOff(t *testing.T) {
	s := healthyCapacity()
	q := cache.WriteoutState{Eligible: 40_000_000, RecoveryBlocked: true}
	c := autoCapacityController{limit: 40000} // retain the 40k operator ceiling on mode switch
	now := time.Now()
	rate := c.step(now, s, q, 1000, 96, 192, 40000, 81, 1e6, 0)
	initial := rate
	var reads hostcapacity.Latency
	for i := 1; i <= 12; i++ {
		for range 100 {
			reads.Observe(time.Millisecond)
		}
		s.Reads = reads.Snapshot()
		s.updates += uint64(rate * 5)
		s.Host.ProcessCPU += float64(rate*5) * .0002
		s.writeSeconds += float64(rate*5) * .0003
		rate = c.step(now.Add(time.Duration(i)*5*time.Second), s, q, 1000, 96, 192, rate, 81, 1e6, 0)
	}
	peak := rate
	if peak <= initial*15/10 {
		t.Fatal("token-bound idle host stayed artificially capped", initial, peak)
	}
	for range 100 {
		reads.Observe(time.Millisecond)
	}
	s.Reads = reads.Snapshot()
	s.updates += uint64(rate * 5)
	s.Host.ProcessCPU += float64(rate*5) * .0002
	s.writeSeconds += float64(rate*5) * .0003
	rate = c.step(now.Add(65*time.Second), s, q, 1000, 96, 192, rate, 81, 1e6, 0)
	for range 100 {
		reads.Observe(50 * time.Millisecond)
	}
	s.Reads = reads.Snapshot()
	s.updates += uint64(rate * 5)
	before := rate
	rate = c.step(now.Add(70*time.Second), s, q, 1000, 96, 192, rate, 81, 1e6, 0)
	if rate >= before || c.reason != "read regression after increase" {
		t.Fatal("read-p99 regression did not back off", before, rate, c.reason)
	}
	t.Logf("incident-shaped simulation: initial=%d/s healthy=%d/s read-pressure=%d/s", initial, peak, rate)
}

func TestAutoCapacityPressureAndNoDemand(t *testing.T) {
	for _, reason := range []string{"stall", "cpu", "disk", "disk full", "gc", "memory", "invalid", "no demand", "no tokens"} {
		t.Run(reason, func(t *testing.T) {
			s := healthyCapacity()
			q := cache.WriteoutState{Eligible: 10000}
			now := time.Now()
			c := autoCapacityController{}
			initial := c.step(now, s, q, 1, 80, 192, 40000, 81, 1e6, 0)
			waiting := int64(81)
			switch reason {
			case "stall":
				s.Stalled = true
			case "cpu":
				s.Host.Idle = .05
			case "disk":
				s.Host.DiskBusy = .95
			case "disk full":
				s.FreeBytes = 1
			case "gc":
				s.Host.GC = .4
			case "memory":
				s.Host.Memory = .01
			case "invalid":
				s.Host.Valid = false
			case "no demand":
				q.Eligible = 0
			case "no tokens":
				waiting = 0
			}
			rate := initial
			for i := 1; i <= 4; i++ {
				s.updates += uint64(rate * 5)
				rate = c.step(now.Add(time.Duration(i)*5*time.Second), s, q, 1, 80, 192, rate, waiting, 1e6, 0)
			}
			if reason == "no demand" || reason == "no tokens" {
				if rate != initial {
					t.Fatal("grew without bound backlog/tokens", rate)
				}
			} else if rate >= initial {
				t.Fatal("pressure failed to shrink", rate)
			}
		})
	}
}
