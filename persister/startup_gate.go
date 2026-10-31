package persister

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.uber.org/zap"
)

// SetStartupGate defers only writeout workers until metadata is warm. Its
// deadline begins after input is ready; Start itself never waits. full may
// release the gate early to keep pending points within the cache budget.
// Set before Start.
func (p *Whisper) SetStartupGate(ready, done <-chan struct{}, limit time.Duration, full func() bool) {
	p.startupReady, p.startupDone, p.startupWait, p.startupFull = ready, done, limit, full
	if done != nil && limit > 0 && p.startupReleases == nil {
		p.startupReleases = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "persister_metadata_prewarm_gate_releases_total",
			Help: "Startup metadata prewarm gate releases by reason.",
		}, []string{"reason"})
		p.initStartupGateMetrics()
	}
}

// SetStartupProgress makes the deadline adaptive within SetStartupGate's cap.
// Unknown total work waits for progress or the cap, without another census.
func (p *Whisper) SetStartupProgress(progress func() (uint64, uint64)) {
	p.startupProgress = progress
}

// SetStartupCheckpointLoading holds a checkpoint gate until completion, pressure
// or its cap. A slow byte projection must not release an almost empty cache.
func (p *Whisper) SetStartupCheckpointLoading(loading func() bool) {
	p.startupCheckpointLoading = loading
}

// SetStartupRelease resumes the cache clock before workers cross the gate.
func (p *Whisper) SetStartupRelease(released func(time.Time)) {
	p.startupReleased = released
}

func (p *Whisper) startStartupGate() {
	p.startupGate = nil
	if p.startupDone == nil || p.startupWait <= 0 {
		return
	}
	gate := make(chan struct{})
	p.startupGate = gate
	p.Go(func(exit chan bool) {
		if p.waitStartup(exit) {
			if p.startupReleased != nil {
				p.startupReleased(time.Now())
			}
			close(gate)
		}
	})
}

func (p *Whisper) waitStartup(exit <-chan bool) bool {
	if p.startupDone == nil || p.startupWait <= 0 {
		return true
	}
	select {
	case <-p.startupReady:
	case <-exit:
		return false
	}
	started := time.Now()
	reason := "stopped"
	var scanned, total uint64
	var projection time.Duration
	defer func() {
		if p.startupProgress != nil {
			scanned, total = p.startupProgress()
			projection = startupProjection(time.Since(started), scanned, total)
		}
		if reason != "stopped" && p.startupReleases != nil {
			p.startupReleases.WithLabelValues(reason).Inc()
		}
		if p.logger != nil {
			p.logger.Info("metadata prewarm writeout wait ended", zap.String("reason", reason), zap.Duration("runtime", time.Since(started)), zap.Uint64("scanned", scanned), zap.Uint64("total", total), zap.Duration("projected_finish", projection), zap.Bool("projection_known", scanned > 0 && total > 0), zap.Duration("cap", p.startupWait))
		}
	}()
	timer := time.NewTimer(p.startupWait)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastProgress, lastReport := started, started
	grace := p.startupGrace
	if grace <= 0 {
		grace = 30 * time.Second
	}
	for {
		select {
		case <-exit:
			return false
		default:
		}
		select {
		case <-p.startupDone:
			reason = "done"
			return true
		default:
		}
		if p.startupFull != nil && p.startupFull() {
			reason = "cache"
			return true
		}
		if p.startupProgress != nil {
			now := time.Now()
			names, expected := p.startupProgress()
			if names > scanned {
				lastProgress = now
			}
			scanned, total = names, expected
			projection = startupProjection(now.Sub(started), scanned, total)
			checkpoint := p.startupCheckpointLoading != nil && p.startupCheckpointLoading()
			if !checkpoint && now.Sub(started) >= grace && scanned > 0 &&
				(now.Sub(lastProgress) >= grace || projection > p.startupWait) {
				reason = "deadline"
				return true
			}
			if p.logger != nil && now.Sub(lastReport) >= 10*time.Second {
				p.logger.Info("metadata prewarm writeout projection", zap.Uint64("scanned", scanned), zap.Uint64("total", total),
					zap.Duration("runtime", now.Sub(started)), zap.Duration("projected_finish", projection), zap.Bool("projection_known", scanned > 0 && total > 0), zap.Duration("cap", p.startupWait))
				lastReport = now
			}
		}
		select {
		case <-p.startupDone:
			reason = "done"
			return true
		case <-timer.C:
			reason = "deadline"
			return true
		case <-ticker.C:
		case <-exit:
			return false
		}
	}
}

func (p *Whisper) initStartupGateMetrics() {
	if p.startupRegisterer == nil || p.startupReleases == nil {
		return
	}
	if err := p.startupRegisterer.Register(p.startupReleases); err != nil {
		var registered prometheus.AlreadyRegisteredError
		if !errors.As(err, &registered) {
			panic(err)
		}
		p.startupReleases = registered.ExistingCollector.(*prometheus.CounterVec)
	}
	for _, reason := range []string{"done", "deadline", "cache"} {
		p.startupReleases.WithLabelValues(reason)
	}
}

// Zero means that the first catalog pass has not established total work yet.
func startupProjection(elapsed time.Duration, scanned, total uint64) time.Duration {
	if scanned == 0 || total == 0 {
		return 0
	}
	// Keep the float-to-duration conversion below its rounded overflow boundary.
	return time.Duration(min(float64(elapsed)*float64(max(total, scanned))/float64(scanned), float64(1<<63-1024)))
}
