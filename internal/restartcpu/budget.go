// Package restartcpu paces optional restart work behind mandatory writeout.
package restartcpu

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Budget accounts for the entire process, including receivers, reads and GC.
// Checkpoint loading, warming and unfinished restart writeout share admission.
// Credits expire after 5ms so idle time cannot fund a catch-up burst. Work
// already admitted may overshoot briefly. Mandatory input and reads never wait;
// writeout at its rate floor, duty-paced checkpoints and bounded required
// preparation can bypass debt.
type Budget struct {
	mu                   sync.Mutex
	cores, credit, used  float64
	reserved             float64
	at                   time.Time
	observedAt           time.Time
	observedCPU          float64
	processReady         bool
	read                 func() float64
	until                time.Time
	warmed               atomic.Bool
	drained              atomic.Pointer[func() bool]
	historyState         atomic.Pointer[func() (int64, bool)]
	drainDone            atomic.Bool
	nextDrain            atomic.Int64
	writers              atomic.Int64
	prefetchers          atomic.Int64
	written              atomic.Uint64
	warmGrants           atomic.Uint64
	writeGrants          atomic.Uint64
	checkpointGrants     atomic.Uint64
	checkpoints          atomic.Int64
	checkpointWork       atomic.Uint64
	checkpointTotal      atomic.Uint64
	checkpointFailures   atomic.Uint64
	checkpointTimeouts   atomic.Uint64
	rateCeiling          atomic.Uint64
	steadyRate           float64
	source               string
	samplerStop          context.CancelFunc
	samplerDone          chan struct{}
	historyErr           error
	released             bool
	floor, rate, process float64
	belowAt, warnedAt    time.Time
	floorAt, preparedAt  time.Time
	checkpointPreparedAt time.Time
	floorCredit          float64
	floorPending         int
	floorGrants          uint64
	severeAt             time.Time
	stalled              bool
	stalls               uint64
}

func New(cores float64) *Budget {
	return NewWithCPU(cores, CPU)
}

// NewWithCPU supplies cumulative process CPU seconds, including mandatory work.
// It also lets deterministic tests model saturated receivers without burning CPU.
func NewWithCPU(cores float64, read func() float64) *Budget {
	now, used := time.Now(), read()
	return &Budget{cores: cores, credit: cores * .005, at: now, used: used, observedAt: now, observedCPU: used, read: read, until: now.Add(30 * time.Minute)}
}

func (b *Budget) Cores() float64 { return b.cores }

// Active keeps optional pacing through the startup window and unfinished warming.
// A later steady workload is governed by the ordinary writeout settings.
func (b *Budget) Active() bool {
	if b == nil || b.cores <= 0 {
		return false
	}
	if b.until.IsZero() || time.Now().Before(b.until) || !b.warmed.Load() {
		return true
	}
	drained := b.drained.Load()
	if drained == nil || b.drainDone.Load() {
		return false
	}
	// Checking a cache's exact size may visit its shards. Sample that work
	// once per second rather than once per update after the startup window.
	now := time.Now().UnixNano()
	next := b.nextDrain.Load()
	if now >= next && b.nextDrain.CompareAndSwap(next, now+int64(time.Second)) && (*drained)() {
		b.drainDone.Store(true)
	}
	return !b.drainDone.Load()
}

// SetDrain keeps restart priority active while a held backlog remains.
func (b *Budget) SetDrain(drained func() bool) {
	b.drainDone.Store(false)
	b.nextDrain.Store(0)
	if drained == nil {
		b.drained.Store(nil)
	} else {
		b.drained.Store(&drained)
	}
}

func (b *Budget) Warmed() {
	if b != nil {
		b.warmed.Store(true)
	}
}

func (b *Budget) delay(now time.Time) time.Duration {
	return b.admissionDelay(now, false)
}

func (b *Budget) admissionDelay(now time.Time, writeout bool) time.Duration {
	return b.reserveDelay(now, writeout, 0)
}

// Reserve the expected cost immediately, then reconcile it with actual whole-
// process CPU at the next sample. Without this, every worker and the loader
// could admit unlimited work between samples, then all sleep through headroom.
func (b *Budget) reserveDelay(now time.Time, writeout bool, cost float64) time.Duration {
	return b.reserveWork(now, writeout, cost, false)
}

type workClass uint8

const (
	optionalWork workClass = iota
	writeoutWork
	preparationWork
	checkpointWork
	checkpointValidationWork
)

func (b *Budget) reserveWork(now time.Time, writeout bool, cost float64, checkpoint bool) time.Duration {
	class := optionalWork
	if checkpoint {
		class = checkpointWork
	} else if writeout {
		class = writeoutWork
	}
	return b.reserveAdmission(now, class, cost, 0)
}

func (b *Budget) reserveAdmission(now time.Time, class workClass, cost float64, updates int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.Sub(b.at) >= 5*time.Millisecond {
		used := b.read()
		if b.observedAt.IsZero() {
			b.observedAt, b.observedCPU = b.at, b.used
		}
		// Keep reporting separate from the short admission interval. A scrape
		// after a quiet 20ms slice must not report one core for a process
		// spending ten cores across the preceding second.
		if elapsed := now.Sub(b.observedAt); elapsed >= time.Second {
			value := (used - b.observedCPU) / elapsed.Seconds()
			if !b.processReady {
				b.process, b.processReady = value, true
			} else {
				// Reporting uses a five-second EWMA, updated at most once a
				// second. Admission below always charges the raw CPU delta.
				b.process += -math.Expm1(-elapsed.Seconds()/5) * (value - b.process)
			}
			b.observedAt, b.observedCPU = now, used
		}
		// Preserve burst charges across quiet slices. A 20ms debt limit lost
		// CPU whenever receivers/GC outran admission for longer than that,
		// allowing sustained excess even though average mandatory CPU fit.
		// Bound debt to one second of allowance: prolonged mandatory overload
		// must not create an unbounded repayment obligation after it ends.
		b.credit = max(-b.cores, min(b.cores*.005, b.credit+b.reserved+now.Sub(b.at).Seconds()*b.cores-(used-b.used)))
		b.reserved = 0
		b.at, b.used = now, used
	}
	if class == optionalWork && (b.writers.Load() > 0 || b.prefetchers.Load() > 0 || !b.belowAt.IsZero()) {
		return 100 * time.Millisecond
	}
	// Keep one sampling slice of the checkpoint share available to its next
	// batch. Writers may use all other credit, including idle checkpoint CPU,
	// but cannot repeatedly win the race for every newly replenished slice.
	minimum := 0.
	if class != checkpointWork {
		minimum = b.checkpointShareLocked() * .005
	}
	// Durability has a mandatory rate floor, just as intake and reads have
	// mandatory CPU. Debit every admitted write, including CPU-funded catch-up,
	// so a CPU shortfall cannot add a second stream of floor-rate writes.
	floor := max(b.floor, b.steadyRate)
	burst := max(1, floor*.04)
	if floor > 0 && (updates > 0 || class == preparationWork) {
		b.refillFloorLocked(now)
	}
	mandatory := floor > 0 && b.floorCredit >= 0 && (updates > 0 ||
		class == preparationWork && (b.preparedAt.IsZero() || now.Sub(b.preparedAt) >= 100*time.Millisecond))
	// Bulk checkpoint workers already duty-pace their measured windows to
	// their shared allowance. Whole-process debt must not veto that bounded
	// share: unrelated maintenance/receivers can otherwise starve it forever.
	// Legacy validation gets one required preparation window per 100ms even
	// without a writeout floor; it must also finish before warming can end.
	mandatory = mandatory || class == checkpointWork || class == checkpointValidationWork &&
		(b.checkpointPreparedAt.IsZero() || now.Sub(b.checkpointPreparedAt) >= 100*time.Millisecond)
	if b.credit >= minimum || mandatory {
		if b.credit < minimum && updates > 0 {
			b.floorGrants++
		}
		if floor > 0 && updates > 0 {
			// Bound debt as well as idle credit: earlier catch-up must not
			// postpone the mandatory floor for more than one short window.
			b.floorCredit = max(-max(burst, float64(updates)), b.floorCredit-float64(updates))
		}
		if class == preparationWork {
			b.preparedAt = now
		}
		if class == checkpointValidationWork {
			b.checkpointPreparedAt = now
		}
		b.credit -= cost
		b.reserved += cost
		return 0
	}
	delay := min(20*time.Millisecond, max(time.Millisecond, time.Duration((minimum-b.credit)/b.cores*float64(time.Second))))
	if floor > 0 && updates > 0 {
		delay = min(delay, max(time.Millisecond, time.Duration(-b.floorCredit/floor*float64(time.Second))))
	}
	return delay
}

// Retain at most one second of service owed to pending durability work. A
// scheduler, queue or I/O pause must not discard floor tokens just because it
// lasts longer than the small idle burst. CPU-funded writes debit the same
// balance; this repays missed floor service, not extra catch-up throughput.
func (b *Budget) refillFloorLocked(now time.Time) {
	floor := max(b.floor, b.steadyRate)
	burst := max(1, floor)
	if b.floorPending == 0 {
		// Idle time adds no credit beyond the idle burst, but must not erase
		// service owed to the preceding busy window. A shrinking queue floor
		// still bounds retained service to one second at the current rate.
		burst = min(burst, max(max(1, floor*.04), b.floorCredit))
	}
	if !b.floorAt.IsZero() {
		b.floorCredit += now.Sub(b.floorAt).Seconds() * floor
	}
	b.floorCredit = min(burst, b.floorCredit)
	b.floorAt = now
}

// Account transitions as well as admissions, so genuinely idle time still banks
// only 40ms and cannot be mistaken for a pending writer's missed service.
func (b *Budget) floorWork(delta int) {
	b.mu.Lock()
	b.refillFloorLocked(time.Now())
	b.floorPending += delta
	b.mu.Unlock()
}

func (b *Budget) writeCost() float64 {
	rate := math.Float64frombits(b.rateCeiling.Load())
	if rate <= 0 {
		rate = 3 * b.steadyRate
	}
	if rate > 0 {
		return b.cores / rate
	}
	return min(.00001, b.cores*.001)
}

// Wait is only for optional warming/validation, never mandatory writeout.
// Call before acquiring a database view or a metric lock.
func (b *Budget) Wait(ctx context.Context) error { return b.waitCPU(ctx, false) }

// WaitCheckpoint admits required legacy validation with a bounded progress
// window even under CPU debt and without a writeout floor. Bulk checkpoint
// streams use Checkpoint.Wait instead.
// No view or shard lock may be held.
func (b *Budget) WaitCheckpoint(ctx context.Context) error { return b.waitCPU(ctx, true) }

func (b *Budget) waitCPU(ctx context.Context, checkpoint bool) error {
	if !b.Active() {
		return ctx.Err()
	}
	if checkpoint {
		b.floorWork(1)
		defer b.floorWork(-1)
	}
	for {
		if !b.Active() {
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		class := optionalWork
		if checkpoint {
			class = checkpointValidationWork
		}
		delay := b.reserveAdmission(time.Now(), class, max(.001, b.writeCost()), 0)
		if delay == 0 {
			b.warmGrants.Add(1)
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}
