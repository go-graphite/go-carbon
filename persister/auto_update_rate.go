package persister

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	"go.uber.org/zap"
)

const autoUpdateInterval = 5 * time.Second
const autoUpdateMargin = 1.03
const autoUpdateSmoothing = time.Minute

// autoUpdateSlack is the catch-up window beyond the configured maximum delay.
// Capacity limits and storage stalls can prevent meeting it; they do not remove
// the configured ceiling. Restart drain retains its separate CPU admission.
func autoUpdateSlack(delay time.Duration) time.Duration {
	return max(30*time.Second, min(4*time.Minute, delay/4))
}

type autoUpdateController struct {
	at, started, restoredAt         time.Time
	need, equilibrium, rate, target float64
	reason                          string
}

func (c *autoUpdateController) step(now time.Time, s cache.WriteoutState, floor, ceiling int) int {
	floor = max(1, floor)
	dt := now.Sub(c.at)
	first := c.at.IsZero()
	if first {
		c.started = now
		dt = autoUpdateInterval
	}
	c.at = now
	// Stable eligibility phases repeat once per mean delay. Slack is only
	// for queueing and catch-up; including it in steady demand underfunds work.
	seconds := max(1, s.MeanDelay.Seconds())
	need := float64(s.Metrics) / seconds
	alpha := 1 - math.Exp(-dt.Seconds()/autoUpdateSmoothing.Seconds())
	// Follow growth immediately; only decreases need a quiet observation window.
	if first || need > c.need {
		c.need = need
	} else {
		c.need += alpha * (need - c.need)
	}
	target, reason := c.need*autoUpdateMargin, "steady"
	// Restored arrivals intentionally get a new batching clock. The saved
	// backlog still distinguishes this workload from an empty cache filling,
	// even after the urgent recovery flag clears at startup release.
	if s.Restored && !s.Recovering && s.Points > 0 && c.restoredAt.IsZero() {
		c.restoredAt = now
		c.equilibrium = float64(s.Points)
	}
	if first {
		c.equilibrium = float64(s.Points)
	}
	// Do not mistake the initial filling of an empty cache for overload.
	if (!c.restoredAt.IsZero() || now.Sub(c.started) >= s.MaxDelay) && c.equilibrium > 0 {
		if growth := target * float64(s.Points) / (1.2 * c.equilibrium); growth > target {
			target, reason = growth, "growth"
		}
	}
	// Compare with recent peaks, not the mean of the batching sawtooth. Normal
	// filling can exceed that mean by nearly 2x even at constant input.
	// Until the first restored deadline, compare with the complete restore,
	// rather than accepting every growing five-second sample as equilibrium.
	// After that window the ordinary age/deadline signal is available again.
	if c.restoredAt.IsZero() || now.Sub(c.restoredAt) >= s.MaxDelay {
		c.equilibrium = max(float64(s.Points), c.equilibrium*math.Exp(-dt.Seconds()/max(1, 4*s.MaxDelay.Seconds())))
	}
	if s.Pressure || s.MaxPoints > 0 && float64(s.Points)/float64(s.MaxPoints) >= .70 {
		remaining := .05
		if !s.Pressure {
			remaining = max(.05, 1-float64(s.Points)/float64(s.MaxPoints))
		}
		if pressure := need / remaining; pressure > target {
			target, reason = pressure, "pressure"
		}
	}
	// The oldest arrival bounds the time left for all eligible work. This is
	// deliberately conservative for synchronized starts, then quiet at equilibrium.
	// Leave a sample interval for polling and token/queue scheduling.
	left := max(autoUpdateInterval, s.MaxDelay+autoUpdateSlack(s.MaxDelay)-s.OldestAge-autoUpdateInterval)
	if deadline := float64(s.Eligible) / left.Seconds(); deadline > target {
		target, reason = deadline, "deadline"
	}
	// Urgent restoration has to drain within the ordinary delay, rather
	// than making every worker unbounded while any saved source remains.
	if s.Recovering {
		left := max(autoUpdateInterval, s.MaxDelay-s.OldestAge-autoUpdateInterval)
		if recovery := float64(s.Eligible) / left.Seconds(); recovery > target {
			target, reason = recovery, "recovery"
		}
	}
	if target < float64(floor) {
		target, reason = float64(floor), "floor"
	}
	if ceiling > 0 && target > float64(ceiling) {
		target, reason = float64(ceiling), "ceiling"
	}
	c.target = target
	c.reason = reason
	if first || target >= c.rate {
		c.rate = target
	} else {
		c.rate += alpha * (target - c.rate)
	}
	if ceiling > 0 {
		c.rate = min(c.rate, float64(ceiling))
	}
	c.rate = max(float64(floor), c.rate)
	// Keep float-to-int conversion away from its rounded overflow boundary.
	return int(math.Ceil(min(c.rate, float64(int(^uint(0)>>1)/2))))
}

type autoUpdateRate struct {
	snapshot                cache.WriteoutState
	mu                      sync.Mutex
	controller              autoUpdateController
	state                   func() cache.WriteoutState
	floor, ceiling, applied int
	ordinary                int
	target                  float64
	reason                  string
	stopping                bool
	ticker                  atomic.Pointer[helper.ThrottleTicker]
	metrics                 *autoUpdateMetrics
	observed                atomic.Pointer[autoUpdateObservation]
}

type autoUpdateObservation struct {
	rate, target float64
	reason       string
}

// SetAutoUpdateRate supplies cache state and limits for -1 mode before Start.
func (p *Whisper) SetAutoUpdateRate(state func() cache.WriteoutState, floor, ceiling int) {
	p.autoUpdate.state, p.autoUpdate.floor, p.autoUpdate.ceiling = state, floor, ceiling
}

func (p *Whisper) sampleAutoUpdateRate() {
	if p.settings().maxUpdatesPerSecond != -1 {
		return
	}
	a := &p.autoUpdate
	a.mu.Lock()
	state, stopping := a.state, a.stopping
	a.mu.Unlock()
	if stopping {
		return
	}
	// The cache scan can take seconds on a large cache. Metrics, the stats
	// collector and restart pacing share autoUpdate.mu, so never scan under it.
	s := p.autoUpdateState(state)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stopping || p.settings().maxUpdatesPerSecond != -1 {
		return
	}
	p.sampleAutoUpdateRateLocked(s)
}

func (p *Whisper) autoUpdateState(state func() cache.WriteoutState) cache.WriteoutState {
	s := state()
	if p.recovering != nil {
		s.Recovering = p.recovering()
	}
	return s
}

// Caller holds autoUpdate.mu. Also used before publishing a new rate mode.
func (p *Whisper) sampleAutoUpdateRateLocked(s cache.WriteoutState) {
	a := &p.autoUpdate
	a.snapshot = s
	a.ordinary = a.controller.step(time.Now(), s, a.floor, a.ceiling)
	p.applyAutoUpdateRateLocked()
}

// Keep the ordinary controller independent of the temporary durability floor.
// Caller holds autoUpdate.mu, including during reload before mode publication.
func (p *Whisper) applyAutoUpdateRateLocked() {
	a := &p.autoUpdate
	defer func() {
		a.observed.Store(&autoUpdateObservation{float64(a.applied), a.target, a.reason})
	}()
	if a.stopping || a.ordinary <= 0 {
		return
	}
	s := a.snapshot
	previous, rate := a.applied, a.ordinary
	a.target, a.reason = a.controller.target, a.controller.reason
	if floor := p.mandatoryRestartRate(); float64(rate) < floor {
		rate = int(math.Ceil(min(floor, float64(int(^uint(0)>>1)/2))))
		a.target, a.reason = max(a.target, floor), "restart"
	}
	p.setAutoRestartRate(rate)
	// Avoid rebuilding tickers for sub-percent measurement noise.
	if a.ticker.Load() != nil && (previous == rate || previous >= a.floor && (a.ceiling <= 0 || previous <= a.ceiling) && previous > 0 && math.Abs(float64(rate-previous)) < max(1, float64(previous)*.01)) {
		return
	}
	a.applied = rate
	if a.metrics != nil {
		a.metrics.adjust(a.reason)
	}
	if p.logger != nil && (previous == 0 || math.Abs(float64(rate-previous)) >= float64(previous)*.25) {
		p.logger.Info("automatic writeout rate adjusted", zap.Int("updates_per_second", rate),
			zap.Int("previous_updates_per_second", previous), zap.Float64("target_updates_per_second", a.target),
			zap.String("reason", a.reason), zap.Int64("cached_metrics", s.Metrics), zap.Duration("oldest_age", s.OldestAge))
	}
	old := a.ticker.Swap(helper.NewThrottleTickerWithBurst(rate, max(1, rate/25)))
	if old != nil {
		old.Stop()
	}
}

func (p *Whisper) startAutoUpdateRate() {
	a := &p.autoUpdate
	a.mu.Lock()
	a.stopping, a.applied = false, 0
	a.mu.Unlock()
	p.sampleAutoUpdateRate()
	p.Go(func(exit chan bool) {
		ticker := time.NewTicker(autoUpdateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-exit:
				return
			case <-ticker.C:
				p.sampleAutoUpdateRate()
			}
		}
	})
}

func (p *Whisper) stopAutoUpdateRate() {
	a := &p.autoUpdate
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopping = true
	if ticker := a.ticker.Swap(nil); ticker != nil {
		ticker.Stop()
	}
}

func (p *Whisper) throttleUpdates() bool {
	settings := p.settings()
	if settings.maxUpdatesPerSecond == -1 {
		return true
	}
	if p.restartPaced() {
		return true
	}
	return p.backlogged == nil || !p.backlogged()
}
