// Package restartcpu paces optional restart work behind mandatory writeout.
package restartcpu

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Budget accounts for the entire process, including receivers, reads and GC.
// Warming and unfinished restart writeout share admission. Credits expire after
// 40ms so idle time cannot fund a catch-up burst. Work already admitted may overshoot briefly.
type Budget struct {
	mu                   sync.Mutex
	cores, credit, used  float64
	at                   time.Time
	observedAt           time.Time
	observedCPU          float64
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
	steadyRate           float64
	source               string
	samplerStop          context.CancelFunc
	samplerDone          chan struct{}
	historyErr           error
	released             bool
	floor, rate, process float64
	belowAt, warnedAt    time.Time
}

func New(cores float64) *Budget {
	return NewWithCPU(cores, CPU)
}

// NewWithCPU supplies cumulative process CPU seconds, including mandatory work.
// It also lets deterministic tests model saturated receivers without burning CPU.
func NewWithCPU(cores float64, read func() float64) *Budget {
	now, used := time.Now(), read()
	return &Budget{cores: cores, at: now, used: used, observedAt: now, observedCPU: used, read: read, until: now.Add(30 * time.Minute)}
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
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.Sub(b.at) >= 20*time.Millisecond {
		used := b.read()
		if b.observedAt.IsZero() {
			b.observedAt, b.observedCPU = b.at, b.used
		}
		// Keep reporting separate from the short admission interval. A scrape
		// after a quiet 20ms slice must not report one core for a process
		// spending ten cores across the preceding second.
		if elapsed := now.Sub(b.observedAt); elapsed >= time.Second {
			b.process = (used - b.observedCPU) / elapsed.Seconds()
			b.observedAt, b.observedCPU = now, used
		}
		// Mandatory work may exceed the allowance indefinitely. It must not
		// leave optional work repaying an unbounded historical CPU debt.
		b.credit = max(-b.cores*.1, min(b.cores*.04, b.credit+now.Sub(b.at).Seconds()*b.cores-(used-b.used)))
		b.at, b.used = now, used
	}
	if !writeout && (b.writers.Load() > 0 || b.prefetchers.Load() > 0 || !b.belowAt.IsZero()) {
		return 100 * time.Millisecond
	}
	if b.credit >= 0 {
		return 0
	}
	return min(100*time.Millisecond, max(20*time.Millisecond, time.Duration(-b.credit/b.cores*float64(time.Second))))
}

// Wait is only for optional warming/validation, never mandatory writeout.
// Call before acquiring a database view or a metric lock.
func (b *Budget) Wait(ctx context.Context) error { return b.waitCPU(ctx, false) }

// WaitCheckpoint streams the saved accelerator and the validation needed by
// upcoming cold writes alongside writes, within the same process credit. It
// must not starve behind the writes it accelerates. No view or shard lock may be held.
func (b *Budget) WaitCheckpoint(ctx context.Context) error { return b.waitCPU(ctx, true) }

func (b *Budget) waitCPU(ctx context.Context, checkpoint bool) error {
	if !b.Active() {
		return ctx.Err()
	}
	for {
		if !b.Active() {
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := b.admissionDelay(time.Now(), checkpoint)
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
