package carbon

import (
	"context"
	"sync"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Restart admission normally lasts at least 30 minutes and may extend while a
// backlog drains. Maintenance cannot be held forever by broken warming/drain.
const storeMaintenanceMaxWait = 35 * time.Minute

// One gate belongs to the store lifetime, including SIGHUP policy changes.
// Waiters own their cancellation; no goroutine or timer survives shutdown.
type storeMaintenanceGate struct {
	warmed <-chan struct{}
	paced  func() bool
	until  time.Time
	limit  time.Duration
	done   chan struct{}
	once   sync.Once
	reason string // published by closing done
}

func newStoreMaintenanceGate(warmed <-chan struct{}, paced func() bool, limit time.Duration) *storeMaintenanceGate {
	return &storeMaintenanceGate{warmed: warmed, paced: paced, until: time.Now().Add(limit), limit: limit, done: make(chan struct{})}
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
		logger.Info("store maintenance waiting for restart warm-up", zap.String("maintenance", name), zap.Duration("maximum_wait_remaining", max(0, time.Until(g.until))))
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
		if ready || !time.Now().Before(g.until) {
			g.once.Do(func() {
				g.reason = "restart warming and pacing complete"
				if !ready {
					g.reason = "maximum wait exceeded"
					logger.Warn("store maintenance restart wait expired", zap.Duration("maximum_wait", g.limit))
				}
				close(g.done)
			})
		}
		timer := time.NewTimer(max(0, min(time.Second, time.Until(g.until))))
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
