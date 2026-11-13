package carbon

import (
	"os"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

func (app *App) loadCompleteIntakeCompatible() bool {
	c := app.Config
	return app.MetricStore != nil && app.inputClaim == nil && app.inheritedInput == nil && app.startupInput == nil && !c.Tags.Enabled && !c.Whisper.StoreMirrorFiles &&
		!c.Whisper.StoreIncrementalMigration && !c.Whisper.StoreLiveMigration &&
		(app.Carbonserver == nil || c.Carbonserver.TrieIndex && c.Carbonserver.ConcurrentIndex && c.Carbonserver.RealtimeIndex > 0)
}

// restoreAtLoadComplete admits cache and input dumps before any live sink.
// startReceivers retains the subsequent tail/journal fences. No global empty
// wait follows intake: continuous live traffic cannot extend the saved prefix.
func (app *App) restoreAtLoadComplete(core *cache.Cache) error {
	started := time.Now()
	core.SetRecoveryDrain(true)
	if app.Carbonserver != nil {
		app.Carbonserver.WarmupIndex()
	}
	app.Restore(core.AddRestoredBeforeInput, app.Config.Dump.Path, app.Config.Dump.RestorePerSecond)
	// An active predecessor may still own a tail. Its socket takeover and tail
	// retirement retain the existing drain-first lifecycle; do not snapshot a
	// prefix while an older source can still grow.
	entries, err := os.ReadDir(app.Config.Dump.Path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if recovery.IsTail(entry.Name()) {
			app.restoreBeforeReceivers(core)
			core.SetRecoveryDrain(false)
			return nil
		}
	}
	names := core.TrackRestorePrefix()
	app.startMetadataPrewarmWithCheckpointGate("restored dump", names, nil)
	if cs := app.Carbonserver; cs != nil {
		cs.WaitForWarmup()
		if err := cs.PrepareRestoreIndex(names); err != nil {
			return err
		}
		cs.SetRestorePending(core.RestorePending)
	}
	loaded := time.Now()
	app.setRestoreDuration(loaded.Sub(started))
	stop, done := make(chan struct{}), make(chan struct{})
	app.legacyDrainStop, app.legacyDrainDone = stop, done
	app.storeRestoreDone = done
	loadedDone := make(chan struct{})
	close(loadedDone)
	app.storeRestoreLoaded = loadedDone
	zapwriter.Logger("app").Info("dump loaded, opening live input", zap.Duration("load_seconds", loaded.Sub(started)), zap.Int64("outstanding_points", core.RestoreOutstanding()))
	go func() {
		defer close(done)
		defer core.SetRecoveryDrain(false)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for core.RestoreOutstanding() != 0 {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
		// Collector owns interval-resetting Stat calls once startup completes.
		zapwriter.Logger("app").Info("loaded dump prefix persisted", zap.Duration("drain_seconds", time.Since(loaded)))
	}()
	return nil
}
