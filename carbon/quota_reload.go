package carbon

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

func validateQuotaReloadConfig(cfg *Config) error {
	if cfg.Whisper.QuotasReloadInterval.Value() < 0 {
		return errors.New("whisper.quotas-reload-interval must be a non-negative duration")
	}
	return nil
}

// quotaReloader owns an immutable configuration snapshot. Its mutex also orders
// SIGHUP publication against polls without taking the application's lifecycle lock.
type quotaReloader struct {
	mu       sync.Mutex
	config   *Config
	listener *carbonserver.CarbonserverListener
	changed  chan struct{}
	stop     chan struct{}
	done     chan struct{}
}

func startQuotaReloader(cfg *Config, listener *carbonserver.CarbonserverListener) *quotaReloader {
	r := &quotaReloader{
		config: cfg, listener: listener,
		changed: make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go r.run()
	return r
}

func (r *quotaReloader) configureLocked(cfg *Config) {
	r.config = cfg
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *quotaReloader) intervalLocked() time.Duration {
	if !r.config.Whisper.Enabled || r.config.Whisper.QuotasFilename == "" {
		return 0
	}
	return r.config.Whisper.QuotasReloadInterval.Value()
}

func (r *quotaReloader) run() {
	defer close(r.done)
	var ticker *time.Ticker
	var ticks <-chan time.Time
	resetTicker := func() {
		if ticker != nil {
			ticker.Stop()
		}
		ticks = nil
		r.mu.Lock()
		interval := r.intervalLocked()
		r.mu.Unlock()
		if interval > 0 {
			ticker = time.NewTicker(interval)
			ticks = ticker.C
		}
	}
	resetTicker()
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		select {
		case <-r.stop:
			return
		case <-r.changed:
			resetTicker()
			// configure() reads the quota file before taking r.mu, so a poll that
			// lands in between can be overwritten by the older SIGHUP snapshot.
			// Re-reading now converges immediately instead of after one interval.
			if err := r.reload(); err != nil {
				zapwriter.Logger("quota").Error("quota file reload failed", zap.Error(err))
			}
		case <-ticks:
			if err := r.reload(); err != nil {
				zapwriter.Logger("quota").Error("quota file reload failed", zap.Error(err))
			}
		}
	}
}

func (r *quotaReloader) reload() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.stop:
		return nil
	default:
	}
	if r.intervalLocked() <= 0 {
		return nil
	}
	quotas, err := persister.ReadWhisperQuotas(r.config.Whisper.QuotasFilename)
	if err != nil {
		return fmt.Errorf("read %s: %w", r.config.Whisper.QuotasFilename, err)
	}
	if slices.Equal(quotas, r.config.Whisper.Quotas) {
		return nil
	}
	// app.Config is intentionally left untouched: its Quotas are only read at
	// startup, and SIGHUP re-reads the file. r.config is the live snapshot.
	cfg := *r.config
	cfg.Whisper.Quotas = quotas
	if err := validateStorageConfig(&cfg); err != nil {
		return err
	}
	if err := r.listener.ReloadQuotas(cfg.getCarbonserverQuotas(cfg.Carbonserver.QuotaUsageReportFrequency.Value())); err != nil {
		return err
	}
	r.config = &cfg
	return nil
}

func (r *quotaReloader) close() {
	close(r.stop)
	<-r.done
}
