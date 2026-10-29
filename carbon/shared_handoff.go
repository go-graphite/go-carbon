package carbon

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Serve the validated checkpoint and, when recovery fits the cache budget,
// continue inherited input before opening Pebble. Even an uncontended Open can
// take seconds. The ordinary cache claims old history before live corrections;
// persistence remains gated until the database and persister are ready.
func (app *App) openStoreWithHandoff(core *cache.Cache, options store.Options) (*store.Store, error) {
	dir := sharedStorePath(app.Config)
	conf := app.Config.Whisper
	if app.handoffClaim == nil || !conf.StoreMirrorFiles || !conf.StoreLiveMigration || !app.Config.Dump.Enabled || app.Config.Tags.Enabled {
		return store.Open(dir, options)
	}
	preflight, err := recovery.OpenBundle(app.Config.Dump.Path, conf.DataDir)
	if err != nil {
		return store.Open(dir, options)
	}
	savedPoints, readID := preflight.Points(), preflight.ReadIndexID()
	if err := preflight.Close(); err != nil {
		return nil, err
	}
	if readID == "" {
		return store.Open(dir, options)
	}
	zapwriter.Logger("app").Info("taking mirrored sockets before reopening predecessor storage")
	newMetrics, err := app.configureCarbonserver(core)
	if err != nil {
		return nil, err
	}
	started, err := app.restoreWithPendingReads(core, newMetrics)
	if err != nil {
		return nil, err
	}
	if !started {
		return nil, errors.New("locked mirrored storage requires a valid pending read checkpoint for handoff")
	}
	if app.inputClaim == nil {
		return nil, errors.New("mirrored storage handoff did not retain its socket claim")
	}
	claim := app.inputClaim
	app.inputClaim = nil
	app.inheritedInput, err = claim.TakeInput()
	if err != nil {
		return nil, fmt.Errorf("take input before predecessor storage release: %w", err)
	}
	app.storeHandoffReady = true
	app.storeHandoffMetrics = newMetrics
	if app.inheritedInput != nil && app.earlyHandoffInputFits(savedPoints) {
		if err := app.startReceivers(core); err != nil {
			return nil, err
		}
		app.storeHandoffInputStarted = true
		zapwriter.Logger("app").Info("inherited input active while shared storage opens")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		db, err := store.Open(dir, options)
		if !errors.Is(err, syscall.EAGAIN) {
			return db, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("predecessor storage lock remained held after socket handoff: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A binary point contains at least two varints. Bound every tail's possible
// point count by bytes/2 and include all checkpoint history. If it may fill the
// cache, leave intake on the ordinary path where the persister can drain it;
// early tail replay must never wait for a persister that has not started.
func (app *App) earlyHandoffInputFits(saved uint64) bool {
	// Logged input from a crash must be replayed before any live input.
	if l := app.inputLog.Load(); l != nil && l.NeedsReplay() {
		return false
	}
	limit := app.Config.Cache.MaxSize
	if limit == 0 || saved >= limit {
		return false
	}
	available := limit - saved
	entries, err := os.ReadDir(app.Config.Dump.Path)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || !recovery.IsTail(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
			return false
		}
		bound := uint64(info.Size()/2 + info.Size()%2)
		if bound >= available {
			return false
		}
		available -= bound
	}
	return true
}

// All callers own App's lock. Clearing the separate statistics lease drains
// admin callbacks without requiring them to acquire App's lifecycle mutex.
func (app *App) closeMetricStore() error { return app.closeMetricStoreFor(false) }

// closeMetricStoreForExit releases the store for a successor when this process
// exits right after, without closing every cached table reader first.
func (app *App) closeMetricStoreForExit() error { return app.closeMetricStoreFor(true) }

func (app *App) closeMetricStoreFor(forExit bool) error {
	app.storeInfoMu.Lock()
	app.storeInfoDB = nil
	app.storeInfoMu.Unlock()
	if app.MetricStore == nil {
		return nil
	}
	var err error
	if forExit {
		err = app.MetricStore.CloseForExit()
	} else {
		err = app.MetricStore.Close()
	}
	ct := app.MetricStore.LastCloseTimings()
	zapwriter.Logger("dump").Info("shared store closed", zap.Duration("ingest", ct.Ingest), zap.Duration("membership", ct.Membership),
		zap.Duration("periodic_sync", ct.PeriodicSync), zap.Duration("snapshot_release", ct.SnapshotRelease), zap.Duration("wal_syncer", ct.WALSyncer),
		zap.Duration("pebble", ct.Pebble), zap.Strings("pebble_steps", ct.PebbleSteps), zap.Error(err))
	app.MetricStore = nil
	return err
}
