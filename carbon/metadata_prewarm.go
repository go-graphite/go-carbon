package carbon

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/persister"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// startMetadataPrewarm streams the catalog after input/handoff readiness.
// A bounded persister wait lets the entire scan finish first. The store
// cancels and joins it on shutdown.
func startMetadataPrewarm(db *store.Store, conf whisperConfig, source string, names iter.Seq[string], ready <-chan struct{}, progress *store.PrewarmProgress, warmed func(), budget *restartcpu.Budget) chan struct{} {
	if !conf.StoreMetadataCachePrewarm || db == nil {
		return nil
	}
	logger := zapwriter.Logger("storage")
	options := store.PrewarmOptions{Progress: progress, StartAfter: ready, Workers: conf.StoreMetadataCachePrewarmWorkers, Rate: conf.StoreMetadataCachePrewarmRate, ByteRate: conf.StoreMetadataCachePrewarmByteRate}
	if budget != nil && budget.Cores() > 0 {
		options.Wait = budget.Wait
	}
	if options.Workers == 0 {
		options.Workers = store.DefaultPrewarmWorkers()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if budget != nil {
			defer budget.Warmed()
		}
		if warmed != nil {
			defer warmed()
		}
		// Stream the saved accelerator alongside writes within the shared
		// credit; unlike optional validation it must not starve behind writers.
		checkpointOptions := options
		if budget != nil {
			checkpointOptions.Wait = budget.WaitCheckpoint
		}
		checkpoint, loadErr := db.LoadMetadataCheckpointAfter(context.Background(), checkpointOptions)
		// Legacy lazy checkpoints may contain provisional records. Validate
		// upcoming writes first, then finish the remainder under the same
		// allowance before reporting warming complete. v2's clean fence
		// already certifies unchanged names without any revision I/O.
		if checkpoint.Valid && checkpoint.Validated+checkpoint.Fenced < checkpoint.Entries {
			for _, next := range []iter.Seq[string]{names, nil} {
				validated, err := db.ValidateMetadataCheckpoint(context.Background(), next, options)
				checkpoint.Validated += validated.Validated
				checkpoint.ReadBytes += validated.ReadBytes
				checkpoint.Duration += validated.Duration
				if err != nil {
					checkpoint.Valid, loadErr = false, err
					break
				}
				if next == nil {
					break
				}
			}
		}
		logger.Info("metadata cache checkpoint loaded", zap.String("source", source), zap.Bool("valid", checkpoint.Valid), zap.Int64("entries", checkpoint.Entries), zap.Int64("validated", checkpoint.Validated), zap.Int64("fenced", checkpoint.Fenced), zap.Int64("bytes", checkpoint.Bytes), zap.Int64("read_bytes", checkpoint.ReadBytes), zap.Duration("runtime", checkpoint.Duration), zap.Error(loadErr))
		// Warm-first writeout prefetches its own queue windows; a missing or
		// invalid checkpoint does not need the catalog walk.
		if checkpoint.Valid || metadataWarmFirst(conf) {
			return
		}
		// The catalog walk avoids a catalog seek per restored name. The
		// bounded startup wait lets pending metrics finish first.
		var stats store.PrewarmStats
		var err error
		if names == nil {
			stats, err = db.PrewarmMetadata(context.Background(), nil, options)
		} else {
			stats, err = db.PrewarmPendingMetadata(context.Background(), names, options)
		}
		if err == nil && !stats.BudgetFull && stats.Filtered > 0 {
			remaining, scanErr := db.PrewarmMetadata(context.Background(), nil, options)
			stats.Names += remaining.Names
			stats.Prewarmed += remaining.Prewarmed
			stats.Bytes += remaining.Bytes
			stats.ReadBytes += remaining.ReadBytes
			stats.Cached += remaining.Cached
			stats.Absent += remaining.Absent
			stats.Raced += remaining.Raced
			stats.Full += remaining.Full
			stats.Failed += remaining.Failed
			stats.Duration += remaining.Duration
			stats.BudgetFull = remaining.BudgetFull
			err = scanErr
		}

		if errors.Is(err, store.ErrMetadataCacheDisabled) {
			logger.Warn("store-metadata-cache-prewarm needs store-metadata-cache-size; not prewarming")
			return
		}
		logger.Info("metadata cache prewarmed", zap.String("source", source), zap.Int64("names", stats.Names),
			zap.Int64("prewarmed", stats.Prewarmed), zap.Int64("bytes", stats.Bytes), zap.Int64("read_bytes", stats.ReadBytes), zap.Int64("filtered", stats.Filtered), zap.Int64("skipped_cached", stats.Cached),
			zap.Int64("skipped_absent", stats.Absent), zap.Int64("skipped_raced", stats.Raced), zap.Int64("skipped_full", stats.Full),
			zap.Int64("failed", stats.Failed), zap.Bool("budget_full", stats.BudgetFull), zap.Int("workers", options.Workers),
			zap.Int("rate", options.Rate), zap.Int("byte_rate", options.ByteRate), zap.Duration("runtime", stats.Duration), zap.Error(err))
	}()
	return done
}

// startMetadataPrewarm starts the prewarm for the shared store and records
// its end. Assumes we are holding app.Lock().
func (app *App) startMetadataPrewarm(source string, names iter.Seq[string]) {
	if done := startMetadataPrewarm(app.MetricStore, app.Config.Whisper, source, names, app.metadataPrewarmReady, app.metadataPrewarmProgress, app.metadataPrewarmComplete(), app.restartBudget); done != nil {
		app.metadataPrewarmDone.Store(&done)
	}
}

// Capture this start's gate: a later start uses a different channel.
func (app *App) metadataPrewarmComplete() func() {
	gate, once := app.metadataPrewarmGate, app.metadataPrewarmGateOnce
	if gate == nil {
		return nil
	}
	return func() { once.Do(func() { close(gate) }) }
}

func (app *App) setMetadataPrewarmGate(p *persister.Whisper) {
	conf := app.Config.Whisper
	if app.MetricStore == nil || !conf.StoreMetadataCachePrewarm || conf.StoreMetadataCacheSize <= 0 {
		return
	}
	limit := metadataPrewarmLimit(app.Config)
	p.SetWarmFirst(metadataWarmFirst(conf))
	if app.restartBudget != nil && app.restartBudget.Cores() > 0 {
		core, delay := app.Cache, app.Config.Cache.WriteoutMaxDelay.Value()
		core.SetRestartOrder(app.restartBudget.Active)
		p.SetRestartBudget(app.restartBudget, func() float64 {
			if delay > 0 {
				return float64(core.Len()) / delay.Seconds()
			}
			return 0
		})
	}
	if limit > 0 {
		app.Cache.PauseStartupWriteout()
		p.SetStartupRelease(app.Cache.ResumeStartupWriteout)
		if conf.StoreMetadataCachePrewarmWait == nil {
			p.SetStartupProgress(app.metadataPrewarmProgress.Snapshot)
			p.SetStartupCheckpointLoading(app.metadataPrewarmProgress.CheckpointLoading)
		}
	}
	p.SetStartupGate(app.metadataPrewarmReady, app.metadataPrewarmGate, limit,
		metadataPrewarmFull(app.Cache, app.Config.Cache.MaxSize, app.Config.Cache.MaxBytes))
}

func metadataPrewarmFull(core *cache.Cache, maxSize, maxBytes uint64) func() bool {
	return func() bool {
		return maxSize > 0 && uint64(core.Size()) >= maxSize/2+maxSize%2 ||
			maxBytes > 0 && uint64(core.EstimatedBytes()) >= maxBytes
	}
}

func metadataPrewarmWait(conf whisperConfig) time.Duration {
	if wait := conf.StoreMetadataCachePrewarmWait; wait != nil {
		return wait.Value()
	}
	if cap := conf.StoreMetadataCachePrewarmWaitCap; cap != nil {
		return cap.Value()
	}
	return 600 * time.Second
}

// Arm before restoration/inherited input can create a first batching epoch,
// including starts that take input before the shared database can reopen.
func (app *App) pauseMetadataPrewarmWriteout(core *cache.Cache) {
	conf := app.Config.Whisper
	if conf.Enabled && conf.StorageBackend == "pebble-chunk" && conf.StoreMetadataCachePrewarm && conf.StoreMetadataCacheSize > 0 && metadataPrewarmLimit(app.Config) > 0 {
		core.PauseStartupWriteout()
	}
}

func metadataCheckpointEnabled(conf whisperConfig) bool {
	return conf.StoreMetadataCachePrewarm && conf.StoreMetadataCacheSize > 0 && (conf.StoreMetadataCacheCheckpoint == nil || *conf.StoreMetadataCacheCheckpoint)
}

func metadataWarmFirst(conf whisperConfig) bool {
	return conf.StoreMetadataCacheWarmFirst == nil || *conf.StoreMetadataCacheWarmFirst
}

func metadataPrewarmLimit(cfg *Config) time.Duration {
	if metadataWarmFirst(cfg.Whisper) {
		return 0
	}
	limit := metadataPrewarmWait(cfg.Whisper)
	// Leave a normal writeout window and a quarter of log age for draining.
	if cfg.InputLog.Enabled {
		age := inputLogMaxAge(cfg)
		limit = min(limit, max(time.Duration(0), age-age/4-cfg.Cache.WriteoutMaxDelay.Value()))
	}
	return limit
}
