package carbon

import (
	"context"
	"errors"
	"iter"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// startMetadataPrewarm loads the shared store's metadata cache, in the
// background, with the committed state of names, the metrics a restart
// restored into the cache (see store.Store.PrewarmMetadata). The persister
// writes every one of them within one writeout cycle; the cache starts empty,
// so until each has been written once its first update reads that state from
// the database. Pass the names in the order the persister is expected to
// write them, so the prewarm stays ahead of it. The store's StopBackground or
// Close stops the prewarm. Returns the channel closed when it ends, or nil
// when it does not start (store-metadata-cache-prewarm off, no shared store).
func startMetadataPrewarm(db *store.Store, conf whisperConfig, source string, names iter.Seq[string]) chan struct{} {
	if !conf.StoreMetadataCachePrewarm || db == nil {
		return nil
	}
	logger := zapwriter.Logger("storage")
	options := store.PrewarmOptions{Workers: conf.StoreMetadataCachePrewarmWorkers, Rate: conf.StoreMetadataCachePrewarmRate}
	if options.Workers == 0 {
		options.Workers = store.DefaultPrewarmWorkers
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		stats, err := db.PrewarmMetadata(context.Background(), names, options)
		if errors.Is(err, store.ErrMetadataCacheDisabled) {
			logger.Warn("store-metadata-cache-prewarm needs store-metadata-cache-size; not prewarming")
			return
		}
		logger.Info("metadata cache prewarmed", zap.String("source", source), zap.Int64("names", stats.Names),
			zap.Int64("prewarmed", stats.Prewarmed), zap.Int64("bytes", stats.Bytes), zap.Int64("skipped_cached", stats.Cached),
			zap.Int64("skipped_absent", stats.Absent), zap.Int64("skipped_raced", stats.Raced), zap.Int64("skipped_full", stats.Full),
			zap.Int64("failed", stats.Failed), zap.Bool("budget_full", stats.BudgetFull), zap.Int("workers", options.Workers),
			zap.Int("rate", options.Rate), zap.Duration("runtime", stats.Duration), zap.Error(err))
	}()
	return done
}

// startMetadataPrewarm starts the prewarm for the shared store and records
// its end. Assumes we are holding app.Lock().
func (app *App) startMetadataPrewarm(source string, names iter.Seq[string]) {
	if done := startMetadataPrewarm(app.MetricStore, app.Config.Whisper, source, names); done != nil {
		app.metadataPrewarmDone.Store(&done)
	}
}
