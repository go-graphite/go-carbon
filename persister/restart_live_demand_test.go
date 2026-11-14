package persister

import (
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

func TestRestartLiveInputAdmissionWithYoungCache(t *testing.T) {
	for _, mode := range []int{-1, 1000} {
		t.Run(map[int]string{-1: "automatic", 1000: "fixed"}[mode], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const input = 340000
				const batch = 10
				started := time.Now()
				var writes atomic.Uint64
				budget := restartcpu.NewWithCPU(20, func() float64 {
					return 2*time.Since(started).Seconds() + float64(writes.Load())*.00001
				})
				budget.SetSteadyRate(1000) // The predecessor's smaller load cannot fund current input.
				p := &Whisper{recv: func(chan bool) string { return "live.metric" }}
				p.SetWorkers(96)
				p.SetMaxUpdatesPerSecond(mode)
				p.SetRestartBudget(budget, func() float64 { return 100000. / 1800 })
				p.SetAutoUpdateRate(func() cache.WriteoutState {
					return cache.WriteoutState{LivePoints: uint64(time.Since(started).Seconds() * input),
						Metrics: 100000, Points: 1000000, Eligible: 100000,
						MaxDelay: 30 * time.Minute, MeanDelay: 30 * time.Minute, OldestAge: time.Since(started)}
				}, 1, 5000)
				p.SetMockStore(func() (StoreFunc, func()) {
					return func(string) {
						time.Sleep(time.Millisecond)
						p.writeStats.shared.observe(batch, time.Millisecond, nil)
						writes.Add(1)
						p.recordUpdate()
					}, nil
				})
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				defer p.Stop()
				time.Sleep(8 * time.Second)
				synctest.Wait()
				previous := writes.Load()
				time.Sleep(3 * time.Second)
				synctest.Wait()
				admitted := float64(writes.Load()-previous) / 3
				t.Logf("mode=%d input=%d points/s batch=%d committed=%.0f updates/s live floor=%.0f", mode, input, batch, admitted, p.restartLiveRate())
				if admitted*batch < input {
					t.Fatalf("young cache underfunded live input: %.0f points/s < %d", admitted*batch, input)
				}
				if mode == -1 {
					applied, _, _ := p.autoUpdateObservation()
					if applied < input/batch {
						t.Fatalf("auto tokens %.0f below input requirement", applied)
					}
				}
				if budget.State().Floor < input/batch {
					t.Fatalf("CPU floor below live input: %+v", budget.State())
				}
			})
		})
	}
}

func TestRestartLiveDemandObservations(t *testing.T) {
	now := time.Now()
	d := restartLiveDemand{}
	s := cache.WriteoutState{Metrics: 100, Points: 1000}
	if got := d.step(now, s, 0, 0); got != 0 {
		t.Fatal(got)
	}
	s.LivePoints = 5000
	// Huge restored batches must not overestimate the live batch size (10).
	if got := d.step(now.Add(5*time.Second), s, 100000, 10); math.Abs(got-103) > 1e-9 {
		t.Fatalf("restored batch diluted input demand: %g", got)
	}
	s.LivePoints += 10000
	if got := d.step(now.Add(10*time.Second), s, 100010, 20); math.Abs(got-2060) > 1e-9 {
		t.Fatalf("smaller current batch/growing input delayed: %g", got)
	}
	if got := d.step(now.Add(15*time.Second), s, 100010, 20); got <= 1800 || got >= 2060 {
		t.Fatalf("quiet sample lost floor immediately: %g", got)
	}
	s.LivePoints = 0
	if got := d.step(now.Add(20*time.Second), s, 0, 0); got != 0 {
		t.Fatalf("counter reset: %g", got)
	}
}
