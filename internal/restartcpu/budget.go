// Package restartcpu paces optional restart work behind mandatory writeout.
package restartcpu

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type sample struct {
	At    time.Time
	Cores float64
}

type history struct {
	Host    string
	Samples []sample
}

// Budget accounts for the entire process, including receivers, reads and GC.
// Warming and unfinished restart writeout share admission. Credits expire after
// 100ms so idle time cannot fund a catch-up burst. Work already admitted may overshoot briefly.
type Budget struct {
	mu                   sync.Mutex
	cores, credit, used  float64
	at                   time.Time
	read                 func() float64
	until                time.Time
	warmed               atomic.Bool
	drained              atomic.Pointer[func() bool]
	drainDone            atomic.Bool
	nextDrain            atomic.Int64
	writers              atomic.Int64
	written              atomic.Uint64
	warmGrants           atomic.Uint64
	writeGrants          atomic.Uint64
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
	return &Budget{cores: cores, at: time.Now(), used: read(), read: read, until: time.Now().Add(30 * time.Minute)}
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
		b.process = (used - b.used) / now.Sub(b.at).Seconds()
		// Mandatory work may exceed the allowance indefinitely. It must not
		// leave optional work repaying an unbounded historical CPU debt.
		b.credit = max(-b.cores*.1, min(b.cores*.1, b.credit+now.Sub(b.at).Seconds()*b.cores-(used-b.used)))
		b.at, b.used = now, used
	}
	if !writeout && (b.writers.Load() > 0 || !b.belowAt.IsZero()) {
		return 100 * time.Millisecond
	}
	if b.credit >= 0 {
		return 0
	}
	return min(100*time.Millisecond, max(20*time.Millisecond, time.Duration(-b.credit/b.cores*float64(time.Second))))
}

// Wait is only for optional warming/validation, never mandatory writeout.
// Call before acquiring a database view or a metric lock.
func (b *Budget) Wait(ctx context.Context) error {
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
		delay := b.delay(time.Now())
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

// Start uses an explicit allowance, or 1.05 times the previous steady median.
// Missing history leaves pacing disabled; the first rollout needs an explicit
// allowance. Steady samples exclude the first 30 minutes of this process.
func Start(ctx context.Context, path string, cores float64) *Budget {
	host, _ := os.Hostname()
	h := history{Host: host}
	if data, err := os.ReadFile(path); err == nil {
		var previous history
		if json.Unmarshal(data, &previous) == nil && previous.Host == host {
			h = previous
		}
	}
	if cores == 0 {
		cores = median(h.Samples, time.Now()) * 1.05
	}
	b := New(cores)
	go func() {
		started, last, used := time.Now(), time.Now(), CPU()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case at := <-ticker.C:
				next := CPU()
				if at.Sub(started) >= 31*time.Minute && b.warmed.Load() && !b.Active() {
					h.Samples = append(h.Samples, sample{at, (next - used) / at.Sub(last).Seconds()})
					if len(h.Samples) > 30 {
						h.Samples = h.Samples[len(h.Samples)-30:]
					}
					data, _ := json.Marshal(h)
					if err := os.WriteFile(path+".tmp", data, 0600); err == nil {
						_ = os.Rename(path+".tmp", path)
					}
				}
				last, used = at, next
			case <-ctx.Done():
				return
			}
		}
	}()
	return b
}

func median(samples []sample, now time.Time) float64 {
	values := make([]float64, 0, 30)
	for _, s := range samples {
		if s.At.After(now.Add(-31*time.Minute)) && !s.At.After(now) && s.Cores > 0 && !math.IsInf(s.Cores, 0) && !math.IsNaN(s.Cores) {
			values = append(values, s.Cores)
		}
	}
	if len(values) < 30 {
		return 0
	}
	sort.Float64s(values)
	return (values[14] + values[15]) / 2
}
