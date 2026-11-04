package restartcpu

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Mandatory intake/read demand alone exceeds the saved process allowance.
// Exercise admission, rather than BeginWriteout (which intentionally bypasses it).
func TestWriteoutFloorSurvivesMandatoryCPUOverload(t *testing.T) {
	for _, batch := range []int{1, 8, 128} {
		t.Run(strconv.Itoa(batch), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const floor = 569.4254382153856
				started := time.Now()
				var writes atomic.Uint64
				b := NewWithCPU(.12365959574006986*1.5, func() float64 {
					return .28*time.Since(started).Seconds() + float64(writes.Load())*.0001
				})
				b.SetSteadyRate(floor)
				b.ObserveWriteout(floor, 0)
				time.Sleep(time.Second) // Exhaust even the initial CPU credit.
				exit := make(chan bool)
				done := make(chan struct{})
				go func() {
					defer close(done)
					for b.WaitWriteoutBatch(exit, batch) {
						writes.Add(uint64(batch))
						for range batch {
							b.RecordWrite()
						}
						b.EndWriteout()
					}
				}()
				time.Sleep(time.Minute)
				synctest.Wait()
				close(exit)
				<-done
				rate := float64(writes.Load()) / 60
				t.Logf("mandatory=.28 allowance=%.6f batch=%d writeout=%.3f/s floor=%.3f/s", b.Cores(), batch, rate, floor)
				if rate < .99*floor || rate > 1.05*floor+float64(batch)/60 {
					t.Fatalf("write floor must progress without permitting catch-up: %.3f/s", rate)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if b.Wait(ctx) == nil {
					t.Fatal("optional warming escaped CPU debt")
				}
			})
		})
	}
}

func TestFloorPreparationProgressAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		b := NewWithCPU(.185, func() float64 { return .28 * time.Since(started).Seconds() })
		b.ObserveWriteout(569, 0)
		time.Sleep(time.Second)
		exit := make(chan bool)
		// Both the cache queue builder and the cold prefetch worker must progress.
		for range 3 {
			if !b.WaitPrefetch(exit) {
				t.Fatal("preparation did not progress")
			}
			b.EndPrefetch()
		}
		if elapsed := time.Since(started); elapsed < 1200*time.Millisecond || elapsed > 1300*time.Millisecond {
			t.Fatal("required preparation must have bounded escape windows", elapsed)
		}
		// Required legacy validation must not trap the last cold queue window.
		if err := b.WaitCheckpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if b.WaitCheckpoint(ctx) == nil {
			t.Fatal("validation ignored cancellation")
		}
		go func() { time.Sleep(time.Millisecond); close(exit) }()
		if b.WaitPrefetch(exit) {
			b.EndPrefetch()
			t.Fatal("preparation ignored shutdown")
		}
	})
}

func TestWriteoutEscalationAndWarningRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(2, func() float64 { return 0 })
		warnings, failures := 0, 0
		for second := 0; second <= 900; second++ {
			warn, failed := b.ObserveWriteoutBacklog(100, 0, true)
			if warn {
				warnings++
			}
			if failed {
				failures++
			}
			if second == 179 && b.State().Stalled {
				t.Fatal("escalated early")
			}
			time.Sleep(time.Second)
		}
		if warnings != 3 || failures != 1 || !b.State().Stalled || b.State().Stalls != 1 {
			t.Fatal("unbounded logging or missing escalation", warnings, failures, b.State())
		}

		reg := prometheus.NewPedanticRegistry()
		b.InitPrometheus(reg)
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, f := range families {
			switch f.GetName() {
			case "restart_cpu_writeout_stalled":
				if f.Metric[0].GetGauge().GetValue() != 1 {
					t.Fatal("missing stalled gauge", f)
				}
				found++
			case "restart_cpu_writeout_stalls_total":
				if f.Metric[0].GetCounter().GetValue() != 1 {
					t.Fatal("incorrect escalation counter", f)
				}
				found++
			}
		}
		if found != 2 {
			t.Fatal("missing shortfall metrics", found)
		}

		b.ObserveWriteoutBacklog(100, 100, true)
		if b.State().Stalled {
			t.Fatal("recovery did not clear stalled gauge")
		}
		b.ObserveWriteoutBacklog(100, 1, true)
		time.Sleep(3 * time.Minute)
		if _, failed := b.ObserveWriteoutBacklog(100, 1, true); !failed || b.State().Stalls != 2 {
			t.Fatal("new severe shortfall did not escalate")
		}
		b.ObserveWriteoutBacklog(100, 0, false)
		time.Sleep(10 * time.Minute)
		if warn, failed := b.ObserveWriteoutBacklog(100, 0, false); warn || failed || b.State().Stalled {
			t.Fatal("ordinary deferred batching counted as stalled writeout")
		}
	})
}

func TestMandatoryFloorDoesNotBankAnIdleBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		b := NewWithCPU(.185, func() float64 { return .28 * time.Since(started).Seconds() })
		b.ObserveWriteout(569, 0)
		b.reserveAdmission(time.Now(), writeoutWork, b.writeCost(), 1)
		time.Sleep(time.Hour)
		admitted := 0
		for admitted < 10000 && b.reserveAdmission(time.Now(), writeoutWork, b.writeCost(), 1) == 0 {
			admitted++
		}
		if admitted > 24 {
			t.Fatal("idle floor credit funded an unbounded burst", admitted)
		}
	})
}

func TestWarningLimitSurvivesBriefFloorRecoveries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(2, func() float64 { return 0 })
		warnings := 0
		for range 10 {
			b.ObserveWriteout(100, 0)
			time.Sleep(11 * time.Second)
			if b.ObserveWriteout(100, 0) {
				warnings++
			}
			b.ObserveWriteout(100, 100)
			time.Sleep(time.Second)
		}
		if warnings != 1 {
			t.Fatal("brief recoveries bypassed the warning rate limit", warnings)
		}
	})
}
