package restartcpu

import (
	"context"
	"errors"
	"math"
	"runtime"
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

func TestOverlappingCheckpointWorkersShareHalfAllowance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(2, func() float64 { return 0 })
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
		b := NewWithCPU(.5, func() float64 { return 0 }) // quarter-core duty share
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

func TestCheckpointPoolBounds(t *testing.T) {
	for _, cores := range []float64{.25, 1, 6, 23, 96} {
		b := NewWithCPU(cores, func() float64 { return 0 })
		workers := b.CheckpointWorkers(0)
		if workers < 1 || workers > runtime.GOMAXPROCS(0) || workers > int(math.Ceil(cores/2)) || b.CheckpointWorkers(1) != 1 {
			t.Fatal(cores, workers)
		}
		var pool []*Checkpoint
		for range workers * 2 { // Two overlapping loads share the same cap.
			pool = append(pool, b.NewCheckpoint(nil))
		}
		if got := b.State().Checkpoint; got != min(cores/2, float64(len(pool))) {
			t.Fatal("pool exceeded its share", cores, got)
		}
		for _, p := range pool {
			p.Finish()
		}
		if b.State().CheckpointLoading {
			t.Fatal("pool did not release its grant")
		}
	}
}

func TestCheckpointIdleDoesNotChargeBufferWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(.5, func() float64 { return 0 })
		p := b.NewCheckpoint(nil) // quarter-core active duty
		defer p.Finish()
		ctx := context.Background()
		if err := p.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // last active window
		start := time.Now()
		if err := p.Idle(ctx); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 6*time.Millisecond {
			t.Fatal("last active window was not settled", elapsed)
		}
		time.Sleep(time.Second) // waiting for the next reader/writer buffer
		start = time.Now()
		if err := p.Wait(ctx); err != nil || time.Since(start) != 0 {
			t.Fatal("buffer wait was charged as CPU work", err, time.Since(start))
		}
		if b.State().CheckpointGrants != 2 {
			t.Fatal("idle admitted work that was not requested")
		}
	})
}

func TestCheckpointDutyStillBoundsMandatoryShareUnderDebt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := NewWithCPU(2, func() float64 { return 3 * time.Since(start).Seconds() })
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
		// Continuous required queue preparation must not consume the
		// checkpoint's separate validation progress window.
		b.ObserveWriteout(100, 0)
		if !b.WaitPrefetch(make(chan bool)) {
			t.Fatal("prefetch failed")
		}
		b.EndPrefetch()
		b.ObserveWriteout(0, 0)
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

func TestTailReplayDutyAndMetadataOwnershipUnderCPUDebt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		b := NewWithCPU(2, func() float64 { return 3 * time.Since(start).Seconds() })
		metadata := b.NewCheckpoint(func() (uint64, uint64) { return 32, 64 })
		if err := metadata.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		metadata.Idle(context.Background())
		before := b.State()
		// One worker and the decoder must share a quarter core, including when
		// mandatory receivers have exhausted the process credit.
		time.Sleep(time.Second)
		p := b.NewReplay(2)
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal("historical recovery starved under mandatory CPU debt", err)
		}
		time.Sleep(time.Millisecond)
		at := time.Now()
		if err := p.Idle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(at); elapsed != 7*time.Millisecond {
			t.Fatal("tail participant escaped shared quarter-core duty", elapsed)
		}
		p.Finish()
		after := b.State()
		if !after.CheckpointLoading || after.CheckpointWork != before.CheckpointWork || after.CheckpointTotal != before.CheckpointTotal || after.CheckpointGrants != before.CheckpointGrants {
			t.Fatalf("tail replay corrupted accelerator ownership/progress: before %+v, after %+v", before, after)
		}
		// Idle queue time must not become CPU debt on the next batch.
		time.Sleep(time.Second)
		q := b.NewReplay(2)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		defer cancel()
		if err := q.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
		if err := q.Idle(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("duty delay ignored cancellation", err)
		}
		q.Finish()
		metadata.Finish()
		if b.State().CheckpointLoading {
			t.Fatal("tail replay leaked metadata checkpoint ownership")
		}
	})
}

func TestTailReplayWorkersBoundInFlightRecovery(t *testing.T) {
	for _, cores := range []float64{.25, 1, 6, 23, 96} {
		b := NewWithCPU(cores, func() float64 { return 0 })
		workers := b.ReplayWorkers(1024)
		if workers < 1 || workers > runtime.GOMAXPROCS(0) || workers > int(math.Ceil(cores/8)) || b.ReplayWorkers(1) != 1 {
			t.Fatal("tail concurrency exceeds its restart share", cores, workers)
		}
	}
}
