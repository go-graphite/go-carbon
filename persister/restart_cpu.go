package persister

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	"go.uber.org/zap"
)

func (p *Whisper) recordUpdate() {
	atomic.AddUint32(&p.updateOperations, 1)
	if p.restartBudget != nil {
		p.restartBudget.RecordWrite()
	}
}

func (p *Whisper) updateTokens() <-chan bool {
	// Auto has one admission budget, including during restart. An installed
	// restart ticker can outlive the CPU budget while its observer waits for
	// startup signals, so it must never shadow the automatic limit.
	if p.maxUpdatesPerSecond == -1 {
		if ticker := p.autoUpdate.ticker.Load(); ticker != nil {
			return ticker.C
		}
		return nil
	}
	if ticker := p.restartTicker.Load(); ticker != nil {
		return ticker.C
	}
	return p.updateTicker.C
}

// CPU admission controls catch-up speed from the remaining process allowance.
// A 3x predecessor-rate ceiling limits exceptional cheap work without leaving
// ordinary CPU headroom idle. The normal rate cap cannot constrain recovery
// below the predecessor's throughput; process CPU admission remains in force.
func (p *Whisper) setRestartRate() {
	if p.maxUpdatesPerSecond == -1 {
		return
	}
	rateLimit := float64(p.maxUpdatesPerSecond)
	floor := 0.
	if p.restartFloor != nil {
		floor = p.restartFloor()
	}
	if p.restartBudget != nil && p.restartBudget.SteadyRate() > 0 {
		rateLimit = 3 * max(floor, p.restartBudget.SteadyRate())
	} else if rateLimit > 0 {
		// Even without qualified history a finite normal cap cannot hold
		// recovery below the queue's required rate. Zero stays unlimited.
		rateLimit = max(rateLimit, 3*floor)
	}
	if rateLimit <= 0 || math.IsNaN(rateLimit) || math.IsInf(rateLimit, 0) {
		return
	}
	p.restartRateMu.Lock()
	defer p.restartRateMu.Unlock()
	rate := int(math.Ceil(rateLimit))
	if p.restartStopping || rate == p.restartRate {
		return
	}
	p.restartRate = rate
	if p.restartBudget != nil {
		p.restartBudget.SetRateCeiling(rateLimit)
	}
	// Store stalls must not bank a full second of catch-up credit. Keep at
	// most 40ms, plus the throttle's one in-flight scheduling chunk.
	old := p.restartTicker.Swap(helper.NewThrottleTickerWithBurst(rate, max(1, (rate+24)/25)))
	if old != nil {
		old.Stop()
	}
}

func (p *Whisper) stopRestartTicker() {
	p.restartRateMu.Lock()
	defer p.restartRateMu.Unlock()
	p.restartStopping = true
	if p.restartBudget != nil {
		p.restartBudget.SetRateCeiling(0)
	}
	if ticker := p.restartTicker.Swap(nil); ticker != nil {
		ticker.Stop()
	}
}

func (p *Whisper) startRestartWriteout() {
	if p.restartBudget == nil {
		return
	}
	p.restartRateMu.Lock()
	p.restartStopping, p.restartRate = false, 0
	p.restartRateMu.Unlock()
	p.setRestartRate()
	p.Go(func(exit chan bool) {
		for _, ready := range []<-chan struct{}{p.startupReady, p.startupGate} {
			if ready != nil {
				select {
				case <-ready:
				case <-exit:
					return
				}
			}
		}
		budget := p.restartBudget
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		last, previous := time.Now(), budget.Updates()
		observe := func(rate float64) {
			floor := budget.SteadyRate()
			if p.restartFloor != nil {
				floor = max(floor, p.restartFloor())
			}
			p.setRestartRate()
			if budget.ObserveWriteout(floor, rate) && p.logger != nil {
				s := budget.State()
				p.logger.Warn("restart writeout below floor", zap.Float64("floor_updates_per_second", floor),
					zap.Float64("updates_per_second", rate), zap.Duration("below_floor_for", s.BelowFor),
					zap.Float64("process_cpu_cores", s.Process), zap.Float64("allowance_cores", s.Allowance),
					zap.Float64("warming_granted_cores", s.Warming), zap.Float64("writeout_granted_cores", s.Writeout),
					zap.Float64("checkpoint_load_granted_cores", s.Checkpoint), zap.Float64("ceiling_updates_per_second", s.Ceiling), zap.Int("workers", p.workersCount))
			}
		}
		observe(0)
		for {
			select {
			case <-exit:
				return
			case at := <-ticker.C:
				if !budget.Active() {
					p.stopRestartTicker()
					return
				}
				written := budget.Updates()
				observe(float64(written-previous) / at.Sub(last).Seconds())
				last, previous = at, written
			}
		}
	})
}

func (p *Whisper) restartPaced() bool {
	if p.restartBudget == nil || !p.restartBudget.Active() {
		return false
	}
	if p.startupReady != nil {
		select {
		case <-p.startupReady:
		default:
			return false
		}
	}
	return true
}
