package carbon

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
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

func (app *App) restoreWithPendingReads(core *cache.Cache, newMetrics *helper.MetricQueue) (bool, error) {
	if app.hasStartupResume() || !app.pendingReadsCompatible() {
		return false, nil
	}
	cs := app.Carbonserver
	started := time.Now()
	checkpointLoaded := make(chan struct{})
	var bundle *recovery.Bundle
	var err, prepareErr error
	preparedNames := 0
	prepared := false
	matchedIndex := false
	var prepareTime time.Duration
	open := app.takeCheckpointOpen()
	cs.WarmupIndexWithPending(func(indexID string, visit, reconcile func(string) error) error {
		<-checkpointLoaded
		if err != nil {
			if cs.UsesSharedCatalogIndex() {
				return err
			}
			return nil
		}
		prepareStart := time.Now()
		matchedIndex = bundle.ReadIndexID() != "" && bundle.ReadIndexID() == indexID
		if matchedIndex {
			preparedNames = int(bundle.NewNameCount())
			prepareErr = bundle.FilterNewNames(cs.PendingMetricStorableBytes, visit)
		} else {
			// Streaming shutdown deliberately keeps the old point sources so a
			// newer dump cannot reorder corrections ahead of an older suffix.
			// The discovery catalog may have advanced independently. Validate
			// every saved name against it; never trust the old new-name flags.
			prepareErr = bundle.VisitMetrics(func(_ uint64, name string) error {
				preparedNames++
				return reconcile(name)
			})
		}
		prepareTime = time.Since(prepareStart)
		prepared = prepareErr == nil
		return prepareErr
	})
	bundle, err = open.wait()
	close(checkpointLoaded)
	opened := time.Now()
	logger := zapwriter.Logger("app")
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warn("pending read checkpoint unavailable; using ordered restore", zap.Error(err),
				zap.Duration("checkpoint_verify_time", open.runtime), zap.Duration("checkpoint_opened_early", open.early))
		}
		return false, nil
	}
	cs.WaitForWarmup()
	indexReady := time.Now()
	if prepareErr != nil {
		_ = bundle.Close()
		return false, prepareErr
	}
	if !prepared || !cs.HasMappedIndex() || matchedIndex && cs.RecoveryIndexID() != bundle.ReadIndexID() {
		_ = bundle.Close()
		logger.Info("pending read checkpoint does not match read index; using ordered restore")
		return false, nil
	}
	attach := core.AttachPendingRecoveryWithPreparedNames
	if !matchedIndex {
		// Conservatively treat all slots as urgent: their old classification
		// no longer proves which names the current store already contains.
		attach = core.AttachPendingRecovery
	}
	if err = attach(bundle); err != nil {
		_ = bundle.Close()
		return false, err
	}
	// The persister is idle until input opens and recovery starts; then it
	// writes these metrics, live-claimed ones first, the rest in this order.
	// A handoff gets here before the store opens; startStorage starts it then.
	app.startMetadataPrewarm("pending checkpoint", core.PendingNames())
	app.setRestoredDump(bundle.Bytes())
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
		zap.Duration("checkpoint_open_time", opened.Sub(started)), zap.Duration("checkpoint_verify_time", open.runtime),
		zap.Duration("checkpoint_opened_early", open.early), zap.Duration("index_wait_time", indexReady.Sub(opened)),
		zap.Duration("pending_names_time", prepareTime), zap.Int("pending_new_names", int(bundle.NewNameCount())),
		zap.Int("pending_names_checked", preparedNames), zap.Bool("read_index_matched", matchedIndex),
		zap.Uint64("points", bundle.Points()), zap.Uint64("metrics", bundle.Metrics()))
	// Tail and live writes claim their own saved history first. Leave unrelated
	// checkpoint work until the tail has opened intake, so its decoding and
	// persistence do not compete with the work keeping fresh input closed.
	stop, done := make(chan struct{}), make(chan struct{})
	app.pendingStop, app.pendingDone, app.storeRestoreDone = stop, done, done
	loadedDone := make(chan struct{})
	app.storeRestoreLoaded = loadedDone
	captureInput := app.startupInput
	restoreRate := app.Config.Dump.RestorePerSecond
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
		if capture := captureInput; capture != nil {
			select {
			case <-capture.legacyDone:
			case <-stop:
				return
			}
		}
		if err := core.RecoverPending(stop, restoreRate); err != nil {
			select {
			case <-stop:
				return
			default:
			}
			app.pendingRecoveryFailures.Add(1)
			logger.Error("pending recovery stopped", zap.Error(err))
			return
		}
		// Every saved name is now in cache or persisted. Native publication
		// checks both under the store creation lock; it need not wait for the
		// potentially hours-long writeout of already loaded history.
		close(loadedDone)
		loaded := time.Now()
		app.setRestoreDuration(loaded.Sub(started))
		// Metrics the saved read index held may wait for a spread writeout
		// deadline (cache.writeout-restore-spread); the store already has
		// them, so the first scan opens once the rest are on disk. The
		// sources retire only when every saved metric is.
		for core.PendingUrgentOutstanding() != 0 {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		stopGateLimit()
		openGate()
		for core.PendingOutstanding() != 0 {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		// Persistence confirmation can precede the periodic store WAL sync.
		// Keep the checkpoint as recovery authority until every completed
		// native mutation is durable, including points claimed from its tail.
		if app.MetricStore != nil {
			if err := app.MetricStore.Sync(); err != nil {
				app.pendingRecoveryFailures.Add(1)
				logger.Error("pending recovery not synced; checkpoint retained", zap.Error(err))
				return
			}
		}
		if err := core.FinishPendingRecovery(); err != nil {
			app.pendingRecoveryFailures.Add(1)
			logger.Error("pending recovery not retired", zap.Error(err))
			return
		}
		stats := make(map[string]float64)
		app.Persister.Stat(func(name string, value float64) { stats[name] = value })
		logger.Info("pending checkpoint persisted", zap.Duration("load_seconds", loaded.Sub(started)), zap.Duration("drain_seconds", time.Since(loaded)), zap.Any("persister_stats", stats))
	}()
	return true, nil
}

// checkpointOpen is a pending checkpoint being opened and verified ahead of
// restoreWithPendingReads. OpenBundle checks every checksum and the whole
// index before it returns a bundle, so opening earlier serves nothing sooner
// that is not verified; a damaged checkpoint still fails here and the start
// restores in order.
type checkpointOpen struct {
	done    chan struct{}
	bundle  *recovery.Bundle
	err     error
	runtime time.Duration
	// early is how long before restoreWithPendingReads asked for it the open
	// started.
	early   time.Duration
	started time.Time
}

func openCheckpoint(dir, root string) *checkpointOpen {
	c := &checkpointOpen{done: make(chan struct{}), started: time.Now()}
	go func() {
		phasecpu.Set(phasecpu.Restore)
		defer phasecpu.Set(phasecpu.Other)
		defer close(c.done)
		c.bundle, c.err = recovery.OpenBundle(dir, root)
		c.runtime = time.Since(c.started)
	}()
	return c
}

func (c *checkpointOpen) wait() (*recovery.Bundle, error) {
	<-c.done
	return c.bundle, c.err
}

// pendingReadsConfigured is pendingReadsCompatible from configuration alone,
// before the carbonserver exists.
func (app *App) pendingReadsConfigured() bool {
	c := app.Config
	return c.Dump.Enabled && c.Whisper.Enabled && !c.Tags.Enabled && c.Carbonserver.Enabled &&
		(c.Whisper.Compressed || c.Whisper.Schemas.AnyCompressed())
}

// startCheckpointOpen starts opening the pending checkpoint, if this start
// may serve one, so that its verification overlaps opening the input log,
// the shared store and the persister. The directory holds this checkpoint's
// final state by now: a predecessor publishes it before it lets this
// process start. Assumes we are holding app.Lock().
func (app *App) startCheckpointOpen() {
	if app.hasStartupResume() || !app.pendingReadsConfigured() {
		return
	}
	if w := app.Config.Whisper; app.handoffClaim != nil && w.StoreMirrorFiles && w.StoreLiveMigration {
		return // openStoreWithHandoff checks the checkpoint itself first
	}
	if _, err := os.Stat(filepath.Join(app.Config.Dump.Path, recovery.ManifestName)); err != nil {
		return
	}
	app.checkpointOpen = openCheckpoint(app.Config.Dump.Path, app.Config.Whisper.DataDir)
}

// takeCheckpointOpen returns the checkpoint opened early, or starts opening
// it now.
func (app *App) takeCheckpointOpen() *checkpointOpen {
	c := app.checkpointOpen
	app.checkpointOpen = nil
	if c == nil {
		return openCheckpoint(app.Config.Dump.Path, app.Config.Whisper.DataDir)
	}
	c.early = time.Since(c.started)
	return c
}

// discardCheckpointOpen closes a checkpoint opened early that the start did
// not use.
func (app *App) discardCheckpointOpen() {
	c := app.checkpointOpen
	app.checkpointOpen = nil
	if c == nil {
		return
	}
	if bundle, err := c.wait(); err == nil {
		_ = bundle.Close()
	}
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
