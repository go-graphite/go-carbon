package carbon

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const expirationPageSize = 256

type expirationStore interface {
	ListPage(context.Context, string, string, int) ([]store.Metadata, error)
	InitializeActivity(context.Context, string, store.Metadata) error
	DeleteIfUnchanged(context.Context, string, store.Metadata) error
	AsyncFlush() (<-chan struct{}, error)
}

type expirationStats struct {
	examined, initialized, deleted, conflicts, pending, errors atomic.Uint64
	duration, lastSuccess                                      atomic.Int64
}

func (s *expirationStats) Stat(send helper.StatCallback) {
	send("examined", float64(s.examined.Swap(0)))
	send("initialized", float64(s.initialized.Swap(0)))
	send("deleted", float64(s.deleted.Swap(0)))
	send("conflicts", float64(s.conflicts.Swap(0)))
	send("pending", float64(s.pending.Swap(0)))
	send("errors", float64(s.errors.Swap(0)))
	send("sweepDurationNs", float64(s.duration.Load()))
	send("lastSuccessTime", float64(s.lastSuccess.Load()))
}

// An expirer owns one immutable policy. Reload and shutdown join its worker
// before replacing the policy or closing the store and catalog refresher.
type metricExpirer struct {
	db         expirationStore
	rules      persister.WhisperExpirationRules
	expiration time.Duration
	interval   time.Duration
	rate       int
	now        func() time.Time
	pending    func(string) bool
	notify     func()
	stats      *expirationStats
	cancel     context.CancelFunc
	done       chan struct{}
	restored   <-chan struct{}
}

func validateExpirationConfig(cfg *Config) error {
	if cfg.Whisper.StoreExpiration == nil || cfg.Whisper.StoreExpiration.Value() < 0 {
		return errors.New("whisper.store-expiration must be a non-negative duration")
	}
	if cfg.Whisper.StoreExpirationCheckInterval == nil || cfg.Whisper.StoreExpirationCheckInterval.Value() <= 0 {
		return errors.New("whisper.store-expiration-check-interval must be a positive duration")
	}
	if cfg.Whisper.StoreExpirationScanRate <= 0 {
		return errors.New("whisper.store-expiration-scan-rate must be positive")
	}
	return nil
}

func loadExpirationConfig(cfg *Config) error {
	if cfg.Whisper.StorageBackend != "pebble-chunk" || cfg.Whisper.StoreExpirationFilename == "" {
		return nil
	}
	rules, err := persister.ReadWhisperExpiration(cfg.Whisper.StoreExpirationFilename)
	if err != nil {
		return fmt.Errorf("read expiration policy %s: %w", cfg.Whisper.StoreExpirationFilename, err)
	}
	cfg.Whisper.ExpirationRules = rules
	return nil
}

func (app *App) startExpiration() {
	cfg := app.Config.Whisper
	if app.MetricStore == nil || (cfg.StoreExpiration.Value() == 0 && !cfg.ExpirationRules.Enabled()) {
		return
	}
	core := app.Cache
	r := &metricExpirer{
		db: app.MetricStore, rules: cfg.ExpirationRules,
		expiration: cfg.StoreExpiration.Value(), interval: cfg.StoreExpirationCheckInterval.Value(),
		rate: cfg.StoreExpirationScanRate, now: time.Now,
		pending:  core.Has,
		stats:    app.expirationStats,
		restored: app.storeRestoreDone,
	}
	if app.metricStoreIndex != nil {
		r.notify = app.metricStoreIndex.notify
	}
	r.start()
	app.expirer = r
}

func (app *App) stopExpiration() {
	if app.expirer != nil {
		app.expirer.close()
		app.expirer = nil
	}
}

func (r *metricExpirer) start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done = cancel, make(chan struct{})
	go func() {
		defer close(r.done)
		// Dumps contain pending points that are invisible to the cache check
		// until replay loads them. Keep reads and receivers available meanwhile.
		if r.restored != nil {
			select {
			case <-ctx.Done():
				return
			case <-r.restored:
			}
		}
		for {
			if ctx.Err() != nil {
				return
			}
			if err := r.sweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
				zapwriter.Logger("storage").Error("expire shared metrics", zap.Error(err))
			}
			timer := time.NewTimer(r.interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (r *metricExpirer) close() {
	r.cancel()
	<-r.done
}

func (r *metricExpirer) sweep(ctx context.Context) (err error) {
	started := time.Now()
	var examined, deleted uint64
	defer func() {
		// Flush tombstones even after a partial sweep so idle stores can
		// reclaim data through compaction. Do not wait for the flush here.
		// A flush failure is counted and logged on its own so a cancelled
		// sweep cannot hide it; it is never folded into the sweep error.
		var flushErr error
		if deleted > 0 {
			if _, flushErr = r.db.AsyncFlush(); flushErr != nil {
				r.stats.errors.Add(1)
				zapwriter.Logger("storage").Error("flush expired shared metrics", zap.Error(flushErr))
			}
		}
		r.stats.duration.Store(int64(time.Since(started)))
		if err == nil && flushErr == nil {
			r.stats.lastSuccess.Store(r.now().Unix())
		} else if err != nil && !errors.Is(err, context.Canceled) {
			r.stats.errors.Add(1)
		}
		zapwriter.Logger("storage").Info("shared metric expiration sweep",
			zap.Uint64("examined", examined), zap.Uint64("deleted", deleted),
			zap.Duration("runtime", time.Since(started)), zap.Error(err))
	}()
	// One reusable ticker bounds both scans and deletes without keeping a
	// database snapshot or a metric lock open while waiting for budget.
	ticker := time.NewTicker(max(time.Nanosecond, time.Second/time.Duration(r.rate)))
	defer ticker.Stop()
	return store.EachPage(ctx, r.db.ListPage, "", expirationPageSize, func(page []store.Metadata) error {
		for _, m := range page {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			examined++
			r.stats.examined.Add(1)
			removed, metricErr := r.expire(ctx, m)
			if metricErr != nil {
				return fmt.Errorf("expire metric %q: %w", m.Name, metricErr)
			}
			if removed {
				deleted++
				// The buffered refresher signal also covers cancellation or an
				// error partway through a page; it never starts a scan inline.
				if r.notify != nil {
					r.notify()
				}
			}
		}
		return nil
	})
}

func (r *metricExpirer) expire(ctx context.Context, m store.Metadata) (bool, error) {
	if m.LastUpdate.IsZero() {
		err := r.db.InitializeActivity(ctx, m.Name, m)
		if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
			r.stats.conflicts.Add(1)
			return false, nil
		}
		if err == nil {
			r.stats.initialized.Add(1)
		}
		return false, err
	}
	ttl := r.rules.Match(m.Name, r.expiration)
	if ttl == 0 || r.now().Sub(m.LastUpdate) < ttl {
		return false, nil
	}
	if r.pending != nil && r.pending(m.Name) {
		r.stats.pending.Add(1)
		return false, nil
	}
	// New arrivals after the cache check may race this deletion. Store revision
	// validation protects committed writes; the persister requeues a missing
	// metric and recreates it if deletion wins before the new write.
	err := r.db.DeleteIfUnchanged(ctx, m.Name, m)
	if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
		r.stats.conflicts.Add(1)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	r.stats.deleted.Add(1)
	return true, nil
}
