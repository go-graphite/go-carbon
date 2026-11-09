package carbon

import (
	"context"
	"sync"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Recovery can outlast any cache-independent estimate. Warn after this time;
// optional maintenance must still wait for warming and pacing to finish.
const storeMaintenanceWarnAfter = 35 * time.Minute

// One gate belongs to the store lifetime, including SIGHUP policy changes.
// Waiters own their cancellation; no goroutine or timer survives shutdown.
type storeMaintenanceGate struct {
	warmed    <-chan struct{}
	paced     func() bool
	warnAt    time.Time
	warnAfter time.Duration
	done      chan struct{}
	once      sync.Once
	warned    sync.Once
	reason    string // published by closing done
}

func newStoreMaintenanceGate(warmed <-chan struct{}, paced func() bool, warnAfter time.Duration) *storeMaintenanceGate {
	return &storeMaintenanceGate{warmed: warmed, paced: paced, warnAt: time.Now().Add(warnAfter), warnAfter: warnAfter, done: make(chan struct{})}
}

func (g *storeMaintenanceGate) wait(ctx context.Context, name string) error {
	if g == nil {
		return ctx.Err()
	}
	started := time.Now()
	logger := zapwriter.Logger("storage")
	select {
	case <-g.done:
	default:
		logger.Info("store maintenance waiting for restart warm-up", zap.String("maintenance", name), zap.Duration("warning_after", max(0, time.Until(g.warnAt))))
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ready := false
		select {
		case <-g.warmed:
			ready = g.paced == nil || !g.paced()
		default:
		}
		if ready {
			g.once.Do(func() {
				g.reason = "restart warming and pacing complete"
				close(g.done)
			})
		} else if !time.Now().Before(g.warnAt) {
			g.warned.Do(func() {
				logger.Warn("store maintenance still waiting for restart recovery", zap.Duration("warning_after", g.warnAfter))
			})
		}
		// Keep a bounded polling interval after the warning too. An expired
		// warning time must neither release maintenance nor create a busy loop.
		delay := time.Second
		if left := time.Until(g.warnAt); left > 0 {
			delay = min(delay, left)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-g.done:
			timer.Stop()
			logger.Info("store maintenance starting", zap.String("maintenance", name), zap.String("reason", g.reason), zap.Duration("waited", time.Since(started)))
			return nil
		case <-timer.C:
		}
	}
}
