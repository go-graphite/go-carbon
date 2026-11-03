package restartcpu

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestCheckpointWriteoutAndWarmingSplit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		b := NewWithCPU(24, func() float64 { return 23.5 * time.Since(started).Seconds() })
		p := b.NewCheckpoint(nil)
		defer p.Finish()
		b.BeginPrefetch()
		defer b.EndPrefetch()
		b.ObserveWriteout(13700, 10000)
		if b.State().Checkpoint != 1 {
			t.Fatal("checkpoint slowed despite spare CPU")
		}
		time.Sleep(time.Second)
		b.ObserveWriteout(13700, 10000)
		s := b.State()
		if s.Checkpoint != .25 || s.Writeout <= 0 || s.Warming <= 0 || s.Checkpoint+s.Writeout+s.Warming > s.Allowance {
			t.Fatalf("invalid below-floor split: %+v", s)
		}
		// Writeout demand survives rate-token and queue waits between jobs.
		if s.Writeout == 0 {
			t.Fatal("required warming hid writeout headroom")
		}
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(4 * time.Millisecond)
		start := time.Now()
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < 12*time.Millisecond {
			t.Fatal("checkpoint did not yield below floor", elapsed)
		}
		b.ObserveWriteout(13700, 15000)
		if s := b.State(); s.Checkpoint != 1 || s.Warming+s.Writeout+s.Checkpoint > 24 {
			t.Fatal(s)
		}
	})
}

func TestCheckpointMandatoryShareAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		b := NewWithCPU(24, func() float64 { return 25 * time.Since(started).Seconds() })
		p := b.NewCheckpoint(nil)
		defer p.Finish()
		time.Sleep(time.Second)
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal("checkpoint starved under process debt", err)
		}
		if s := b.State(); s.Checkpoint != 1 || math.IsInf(s.Checkpoint, 0) {
			t.Fatal(s)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := p.Wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal("checkpoint ignored cancellation", err)
		}
	})
}

func TestAdmissionReservesCostBetweenCPUSamples(t *testing.T) {
	b := NewWithCPU(24, func() float64 { return 0 })
	b.SetSteadyRate(13700)
	admitted := 0
	for ; admitted < 10000; admitted++ {
		if b.reserveDelay(b.at, true, b.writeCost()) > 0 {
			break
		}
	}
	// The old unsampled window admitted every call. Credit now covers only
	// five milliseconds plus one estimated update, regardless of caller count.
	if admitted == 10000 || float64(admitted)*b.writeCost() > 24*.005+b.writeCost() {
		t.Fatal("unbounded unsampled CPU admission", admitted)
	}
}

func TestCheckpointProgressRetainsLastBytesOnFailure(t *testing.T) {
	b := NewWithCPU(24, func() float64 { return 0 })
	work, total := uint64(4096), uint64(8192)
	p := b.NewCheckpoint(func() (uint64, uint64) { return work, total })
	if err := p.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	work, total = 0, 0 // The loader resets its gate projection after failure.
	p.Finish()
	s := b.State()
	if s.CheckpointLoading || s.CheckpointWork != 4096 || s.CheckpointTotal != 8192 || s.CheckpointGrants != 1 {
		t.Fatal(s)
	}
}

func TestOverlappingCheckpointStreamsShareOneCore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(24, func() float64 { return 0 })
		p, q := b.NewCheckpoint(nil), b.NewCheckpoint(nil)
		defer p.Finish()
		defer q.Finish()
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(8 * time.Millisecond)
		start := time.Now()
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) < 8*time.Millisecond {
			t.Fatal("overlapping streams each claimed the full checkpoint share")
		}
		if b.State().Checkpoint != 1 {
			t.Fatal("aggregate checkpoint grant changed")
		}
	})
}

func TestCheckpointMetricsExposeProgressAndAllowance(t *testing.T) {
	b := NewWithCPU(24, func() float64 { return 0 })
	p := b.NewCheckpoint(func() (uint64, uint64) { return 32, 64 })
	defer p.Finish()
	b.SetRateCeiling(41100)
	if err := p.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewPedanticRegistry()
	b.InitPrometheus(reg)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, f := range families {
		switch f.GetName() {
		case "restart_cpu_allowance_cores":
			if f.Metric[0].GetGauge().GetValue() != 24 {
				t.Fatal(f)
			}
			found[f.GetName()] = true
		case "restart_cpu_writeout_ceiling_updates_per_second":
			if f.Metric[0].GetGauge().GetValue() != 41100 {
				t.Fatal(f)
			}
			found[f.GetName()] = true
		case "restart_cpu_checkpoint_loading":
			if f.Metric[0].GetGauge().GetValue() != 1 {
				t.Fatal(f)
			}
			found[f.GetName()] = true
		case "restart_cpu_checkpoint_load_bytes":
			for _, m := range f.Metric {
				expected := 32.
				if m.Label[0].GetValue() == "total" {
					expected = 64
				}
				if m.GetGauge().GetValue() != expected {
					t.Fatal(m)
				}
			}
			found[f.GetName()] = true
		case "restart_cpu_granted_cores", "restart_cpu_grants_total":
			for _, m := range f.Metric {
				if m.Label[0].GetValue() == "checkpoint_load" {
					value := m.GetGauge().GetValue() + m.GetCounter().GetValue()
					if value != 1 {
						t.Fatal(m)
					}
					found[f.GetName()] = true
				}
			}
		}
	}
	if len(found) != 6 {
		t.Fatal("missing restart diagnostics", found)
	}
}

func TestCheckpointCoalescesTinyDutyDelays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(2, func() float64 { return 0 }) // quarter-core duty share
		p := b.NewCheckpoint(nil)
		defer p.Finish()
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for range 100 {
			time.Sleep(10 * time.Microsecond)
			if err := p.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if elapsed := time.Since(start); elapsed != time.Millisecond {
			t.Fatal("created timers for sub-window duty debt", elapsed)
		}
		if p.owed > 5*time.Millisecond {
			t.Fatal("unbounded duty debt", p.owed)
		}
	})
}

func TestCheckpointDutyStillBoundsMandatoryShareUnderDebt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := NewWithCPU(8, func() float64 { return 9 * time.Since(start).Seconds() })
		p, q := b.NewCheckpoint(nil), b.NewCheckpoint(nil)
		defer p.Finish()
		defer q.Finish()
		time.Sleep(time.Second)
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(8 * time.Millisecond) // one worker's measured batch
		before := time.Now()
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if time.Since(before) < 8*time.Millisecond {
			t.Fatal("mandatory bypass escaped half-core duty pacing")
		}
		if s := b.State(); s.Checkpoint != 1 || s.Process <= s.Allowance {
			t.Fatal(s)
		}
	})
}

func TestCheckpointValidationProgressWithoutWriteoutFloor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := NewWithCPU(1, func() float64 { return 2 * time.Since(start).Seconds() })
		time.Sleep(time.Second)
		// Required queue preparation keeps its existing process-credit path;
		// it cannot consume the checkpoint's validation progress window.
		b.BeginPrefetch()
		defer b.EndPrefetch()
		if err := b.WaitCheckpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := b.WaitCheckpoint(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("required validation escaped bounded duty", err)
		}
		before := time.Now()
		if err := b.WaitCheckpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
		if time.Since(before) < 50*time.Millisecond {
			t.Fatal("validation progress window unbounded")
		}
	})
}
