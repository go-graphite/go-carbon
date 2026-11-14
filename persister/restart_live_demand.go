package persister

import (
	"math"
	"time"

	"github.com/go-graphite/go-carbon/cache"
)

// restartLiveDemand separates continuing input from the finite restored backlog.
// All observations use cumulative counters, independently of periodic Stat resets.
type restartLiveDemand struct {
	at                    time.Time
	live, points, updates uint64
	rate                  float64
	batch                 float64
}

func (d *restartLiveDemand) step(now time.Time, s cache.WriteoutState, points, updates uint64) float64 {
	if d.at.IsZero() || s.LivePoints < d.live || points < d.points || updates < d.updates {
		d.at, d.live, d.points, d.updates = now, s.LivePoints, points, updates
		d.rate, d.batch = 0, 0
		return 0
	}
	dt := now.Sub(d.at)
	if dt < autoUpdateInterval {
		return d.rate
	}
	// A restored batch can be much larger than current live batches. Never use
	// it to predict more points per update than the cache currently holds per name.
	batch := 1.
	if s.Metrics > 0 {
		batch = max(1, float64(s.Points)/float64(s.Metrics))
	}
	if updates > d.updates && points > d.points {
		d.batch = max(1, float64(points-d.points)/float64(updates-d.updates))
	}
	if d.batch > 0 {
		batch = min(batch, d.batch)
	}
	need := float64(s.LivePoints-d.live) / dt.Seconds() / batch * autoUpdateMargin
	if need >= d.rate {
		d.rate = need
	} else {
		d.rate += -math.Expm1(-dt.Seconds()/autoUpdateSmoothing.Seconds()) * (need - d.rate)
	}
	d.at, d.live, d.points, d.updates = now, s.LivePoints, points, updates
	return d.rate
}

func (p *Whisper) restartLiveRate() float64 {
	return math.Float64frombits(p.autoUpdate.liveRate.Load())
}

// Caller holds autoUpdate.mu. The same demand drives token and CPU-floor
// admission, so CPU headroom cannot leave live input waiting on a lower ticker.
func (p *Whisper) sampleRestartLiveDemandLocked(now time.Time, s cache.WriteoutState) {
	if p.restartBudget == nil || !p.restartBudget.Active() {
		return
	}
	shared, original := p.WriteStats()
	rate := p.autoUpdate.liveDemand.step(now, s, shared.Points+original.Points, shared.Updates+original.Updates)
	p.autoUpdate.liveRate.Store(math.Float64bits(rate))
}

func (p *Whisper) sampleRestartLiveDemand() {
	a := &p.autoUpdate
	a.mu.Lock()
	state, generation := a.state, a.generation
	due := a.liveDemand.at.IsZero() || time.Since(a.liveDemand.at) >= autoUpdateInterval
	a.mu.Unlock()
	if state == nil || !due {
		return
	}
	s := state() // Cache sampling must not hold the automatic controller mutex.
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.stopping && generation == a.generation {
		p.sampleRestartLiveDemandLocked(time.Now(), s)
	}
}
