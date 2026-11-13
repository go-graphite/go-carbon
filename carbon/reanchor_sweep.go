package carbon

import (
	"context"
	"time"
)

type reanchorSweep struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startReanchorSweep repairs classic archives whose writes the re-anchor
// budget deferred, once the post-warm-up maintenance gate opens. Each repair
// is an ordinary store update without points, under the metric's mutation
// lock and in one indexed batch, like the inline repair.
func (app *App) startReanchorSweep() {
	pacer := app.reanchorPacer
	if pacer == nil || app.MetricStore == nil || app.reanchorSweep != nil {
		return
	}
	db := app.MetricStore
	ctx, cancel := context.WithCancel(context.Background())
	s := &reanchorSweep{cancel: cancel, done: make(chan struct{})}
	app.reanchorSweep = s
	go func() {
		defer close(s.done)
		pacer.Sweep(ctx, func(ctx context.Context) error {
			return app.storeMaintenance.wait(ctx, "classic-reanchor-sweep")
		}, func(ctx context.Context, name string) error {
			return db.UpdateMany(ctx, name, nil)
		})
	}()
}

func (app *App) stopReanchorSweep() {
	s := app.reanchorSweep
	if s == nil {
		return
	}
	app.reanchorSweep = nil
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(time.Minute):
	}
}
