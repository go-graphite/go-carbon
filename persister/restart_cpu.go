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
	if ticker := p.restartTicker.Load(); ticker != nil {
		return ticker.C
	}
	return p.updateTicker.C
}

// Raise an undersized configured rate to the floor without an unlimited
// backlog bypass. Keep the high-water rate until the startup window ends.
func (p *Whisper) raiseRestartRate(floor float64) {
	if floor <= 0 || math.IsNaN(floor) || math.IsInf(floor, 0) || p.maxUpdatesPerSecond <= 0 {
		return
	}
	p.restartRateMu.Lock()
	defer p.restartRateMu.Unlock()
	rate := int(math.Ceil(floor))
	if p.restartStopping || rate <= max(p.maxUpdatesPerSecond, p.restartRate) {
		return
	}
	p.restartRate = rate
	old := p.restartTicker.Swap(helper.NewThrottleTicker(rate))
	if old != nil {
		old.Stop()
	}
}

func (p *Whisper) stopRestartTicker() {
	p.restartRateMu.Lock()
	defer p.restartRateMu.Unlock()
	p.restartStopping = true
	if ticker := p.restartTicker.Swap(nil); ticker != nil {
		ticker.Stop()
	}
}

func (p *Whisper) startRestartWriteout() {
	if p.restartBudget == nil {
		return
	}
	p.restartStopping, p.restartRate = false, 0
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
			floor := 0.0
			if p.restartFloor != nil {
				floor = p.restartFloor()
			}
			p.raiseRestartRate(floor)
			if budget.ObserveWriteout(floor, rate) && p.logger != nil {
				s := budget.State()
				p.logger.Warn("restart writeout below floor", zap.Float64("floor_updates_per_second", floor),
					zap.Float64("updates_per_second", rate), zap.Duration("below_floor_for", s.BelowFor),
					zap.Float64("process_cpu_cores", s.Process), zap.Float64("allowance_cores", s.Allowance),
					zap.Float64("warming_granted_cores", s.Warming))
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
