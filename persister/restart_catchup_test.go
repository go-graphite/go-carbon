package persister

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

// Keep offering steady input while an initial backlog drains. The CPU model
// includes mandatory work and each confirmed write, so raising the safety
// ceiling alone cannot pass by escaping the shared process allowance.
func TestRestartCatchupDrainsWithinAvailableCPUHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mandatory float64
		backlog   int
		deadline  time.Duration
	}{
		{"one_spare_core", .5, 1200, 10 * time.Second},
		{"quarter_spare_core", 1.25, 500, 15 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				var writeMicros, offered atomic.Uint64
				used := func() float64 {
					return tc.mandatory*time.Since(started).Seconds() + float64(writeMicros.Load())/1e6
				}
				budget := restartcpu.NewWithCPU(2, used)
				budget.SetSteadyRate(100)
				budget.Warmed() // Admission must continue after checkpoint loading.
				pending := make(chan string, tc.backlog+2000)
				for range tc.backlog {
					pending <- "metric"
				}
				offered.Store(uint64(tc.backlog))
				p := &Whisper{recv: func(exit chan bool) string {
					select {
					case name := <-pending:
						return name
					case <-exit:
						return ""
					}
				}}
				p.SetWorkers(4)
				p.SetRestartBudget(budget, func() float64 { return 100 })
				p.SetMockStore(func() (StoreFunc, func()) {
					return func(string) {
						writeMicros.Add(5000) // 100 steady writes/s use half a core.
						p.recordUpdate()
					}, nil
				})
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				defer p.Stop()
				inputStop, inputDone := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(inputDone)
					ticker := time.NewTicker(10 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-inputStop:
							return
						case <-ticker.C:
							offered.Add(1)
							select {
							case pending <- "metric":
							case <-inputStop:
								return
							}
						}
					}
				}()
				defer func() { close(inputStop); <-inputDone }()
				for time.Since(started) < tc.deadline {
					time.Sleep(20 * time.Millisecond)
					synctest.Wait()
					// Account for the controller's short sampling/grant window.
					if cpu := used(); cpu > 2*time.Since(started).Seconds()+.25 {
						t.Fatalf("catch-up escaped process CPU allowance: used=%g elapsed=%s", cpu, time.Since(started))
					}
					if budget.Updates() == offered.Load() {
						if time.Since(started) < time.Second || offered.Load() <= uint64(tc.backlog) {
							t.Fatal("did not exercise continuing input")
						}
						return
					}
				}
				remaining := offered.Load() - budget.Updates()
				t.Fatalf("unused CPU headroom left %d writes queued after %s", remaining, tc.deadline)
			})
		})
	}
}
