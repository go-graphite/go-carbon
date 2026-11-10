package persister

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

// Exercise real worker dispatch and rate tokens with enough workers and queue
// supply to exclude I/O and queue preparation as the throughput bottleneck.
// Keep this worker integration on the real clock: virtual-time timer delivery
// crashes the Go 1.26 race runtime with this many concurrent mock workers.
// Budget accounting itself remains deterministic through the injected CPU model;
// measure the actual elapsed interval rather than assuming timers fire exactly.
func TestRestartPacingUsesSpareCPUAbovePredecessorFloor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		saved float64
		cap   int
	}{
		{"normal_cap", 13700, 10000},
		{"low_saved_rate", 10000. / 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const floor = 13700.
			const allowance = 24.
			started := time.Now()
			var micros atomic.Uint64
			used := func() float64 { return 8*time.Since(started).Seconds() + float64(micros.Load())/1e6 }
			budget := restartcpu.NewWithCPU(allowance, used)
			budget.SetSteadyRate(tc.saved)
			p := &Whisper{recv: func(exit chan bool) string {
				select {
				case <-exit:
					return ""
				default:
					return "metric"
				}
			}}
			p.SetWorkers(96)
			p.SetMaxUpdatesPerSecond(tc.cap)
			p.SetRestartBudget(budget, func() float64 { return floor })
			p.SetMockStore(func() (StoreFunc, func()) {
				return func(string) { time.Sleep(time.Millisecond); micros.Add(584); p.recordUpdate() }, nil
			})
			if err := p.Start(); err != nil {
				t.Fatal(err)
			}
			defer p.Stop()
			time.Sleep(time.Second)
			measuredAt := time.Now()
			before, cpu := budget.Updates(), used()
			time.Sleep(3 * time.Second)
			elapsed := time.Since(measuredAt).Seconds()
			rate := float64(budget.Updates()-before) / elapsed
			cores := (used() - cpu) / elapsed
			p.restartRateMu.Lock()
			restartRate := p.restartRate
			p.restartRateMu.Unlock()
			t.Logf("writeout=%.0f/s floor=%.0f/s process=%.2f allowance=%.2f restart_tick=%d/s", rate, floor, cores, allowance, restartRate)
			if rate < 1.1*floor {
				t.Fatalf("spare CPU left idle below catch-up target: %.0f/s", rate)
			}
			if cores > allowance+.5 {
				t.Fatalf("process exceeded admission window tolerance: %.2f", cores)
			}
		})
	}
}
