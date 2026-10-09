package carbon

import (
	"errors"
	"os"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// The fast read path preserves the existing compressed-history intake gate.
// Tagged input uses a different normalization boundary in legacy WAL replay.
func (app *App) pendingReadsCompatible() bool {
	c := app.Config
	return c.Dump.Enabled && c.Whisper.Enabled && !c.Tags.Enabled && app.Carbonserver != nil &&
		(c.Whisper.Compressed || c.Whisper.Schemas.AnyCompressed())
}

func (app *App) restoreWithPendingReads(core *cache.Cache, newMetrics chan string) (bool, error) {
	if !app.pendingReadsCompatible() {
		return false, nil
	}
	cs := app.Carbonserver
	started := time.Now()
	checkpointLoaded := make(chan struct{})
	var bundle *recovery.Bundle
	var err, prepareErr error
	preparedNames := 0
	prepared := false
	var prepareTime time.Duration
	cs.WarmupIndexWithPending(func(indexID string, visit func(string) error) error {
		<-checkpointLoaded
		if err != nil || bundle.ReadIndexID() == "" || bundle.ReadIndexID() != indexID {
			return nil
		}
		prepareStart := time.Now()
		prepareErr = bundle.NewNames(func(name string) error {
			preparedNames++
			return visit(name)
		})
		prepareTime = time.Since(prepareStart)
		prepared = prepareErr == nil
		return prepareErr
	})
	bundle, err = recovery.OpenBundle(app.Config.Dump.Path, app.Config.Whisper.DataDir)
	close(checkpointLoaded)
	opened := time.Now()
	logger := zapwriter.Logger("app")
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warn("pending read checkpoint unavailable; using ordered restore", zap.Error(err))
		}
		return false, nil
	}
	cs.WaitForWarmup()
	indexReady := time.Now()
	if prepareErr != nil {
		_ = bundle.Close()
		return false, prepareErr
	}
	if !prepared || !cs.HasMappedIndex() || cs.RecoveryIndexID() != bundle.ReadIndexID() {
		_ = bundle.Close()
		logger.Info("pending read checkpoint does not match read index; using ordered restore")
		return false, nil
	}
	if err = core.AttachPendingRecovery(bundle); err != nil {
		_ = bundle.Close()
		return false, err
	}
	// The first filesystem scan must not run before checkpoint-only metrics
	// have Whisper files, or it would drop them from the index.
	gate := make(chan struct{})
	cs.SetStartupScanGate(gate)
	defer func() {
		if app.pendingDone == nil {
			close(gate)
		}
	}()
	if err = app.listenCarbonserver(core, newMetrics); err != nil {
		return false, err
	}
	logger.Info("serving reads from pending checkpoint", zap.Duration("runtime", time.Since(started)),
		zap.Duration("checkpoint_open_time", opened.Sub(started)), zap.Duration("index_wait_time", indexReady.Sub(opened)),
		zap.Duration("pending_names_time", prepareTime), zap.Int("pending_new_names", preparedNames),
		zap.Uint64("points", bundle.Points()), zap.Uint64("metrics", bundle.Metrics()))
	// Receivers open now: the first live write of a metric claims its saved
	// history into the same cache item (see cache.pendingRecovery), so saved
	// points still reach disk no later than newer live points of that metric.
	stop, done := make(chan struct{}), make(chan struct{})
	app.pendingStop, app.pendingDone, app.storeRestoreDone = stop, done, done
	go func() {
		defer close(done)
		defer close(gate)
		if err := core.RecoverPending(stop, app.Config.Dump.RestorePerSecond); err != nil {
			logger.Warn("pending recovery stopped", zap.Error(err))
			return
		}
		loaded := time.Now()
		for core.PendingOutstanding() != 0 {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		if err := core.FinishPendingRecovery(); err != nil {
			logger.Error("pending recovery not retired", zap.Error(err))
			return
		}
		stats := make(map[string]float64)
		app.Persister.Stat(func(name string, value float64) { stats[name] = value })
		logger.Info("pending checkpoint persisted", zap.Duration("load_seconds", loaded.Sub(started)), zap.Duration("drain_seconds", time.Since(loaded)), zap.Any("persister_stats", stats))
	}()
	return true, nil
}

// stopPendingRecovery ends background recovery before a dump. Unclaimed saved
// metrics are then written by DumpPoints and claimed ones are in cache.
func (app *App) stopPendingRecovery() {
	if app.pendingStop != nil {
		close(app.pendingStop)
		<-app.pendingDone
		app.pendingStop, app.pendingDone = nil, nil
	}
}
