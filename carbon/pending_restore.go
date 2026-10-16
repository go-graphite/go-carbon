package carbon

import (
	"errors"
	"os"
	"sync"
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
	gate, openGate, stopGateLimit := newScanGate(pendingScanGateLimit, func() {
		logger.Warn("first scan no longer waits for pending recovery",
			zap.Duration("limit", pendingScanGateLimit), zap.Uint64("outstanding", core.PendingOutstanding()))
	})
	cs.SetStartupScanGate(gate)
	defer func() {
		if app.pendingDone == nil {
			stopGateLimit()
			openGate()
		}
	}()
	if err = app.listenCarbonserver(core, newMetrics); err != nil {
		return false, err
	}
	logger.Info("serving reads from pending checkpoint", zap.Duration("runtime", time.Since(started)),
		zap.Duration("checkpoint_open_time", opened.Sub(started)), zap.Duration("index_wait_time", indexReady.Sub(opened)),
		zap.Duration("pending_names_time", prepareTime), zap.Int("pending_new_names", preparedNames),
		zap.Uint64("points", bundle.Points()), zap.Uint64("metrics", bundle.Metrics()))
	// Tail and live writes claim their own saved history first. Leave unrelated
	// checkpoint work until the tail has opened intake, so its decoding and
	// persistence do not compete with the work keeping fresh input closed.
	stop, done := make(chan struct{}), make(chan struct{})
	app.pendingStop, app.pendingDone, app.storeRestoreDone = stop, done, done
	inputReady := make(chan struct{})
	app.pendingInputReady = inputReady
	go func() {
		defer close(done)
		defer openGate()
		defer stopGateLimit()
		select {
		case <-inputReady:
		case <-stop:
			return
		}
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

// pendingScanGateLimit bounds how long the first scan waits for pending
// recovery. Recovery normally drains within minutes; a saved metric the
// persister keeps failing to write in a way the cache cannot observe must not
// stop the store from scanning until its next restart.
var pendingScanGateLimit = time.Hour

// newScanGate returns a gate that open closes, or that closes by itself after
// limit, calling onLimit first. stop cancels the limit.
func newScanGate(limit time.Duration, onLimit func()) (gate chan struct{}, open func(), stop func()) {
	gate = make(chan struct{})
	var once sync.Once
	open = func() { once.Do(func() { close(gate) }) }
	timer := time.AfterFunc(limit, func() {
		onLimit()
		open()
	})
	return gate, open, func() { timer.Stop() }
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
