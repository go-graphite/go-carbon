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
	cs.WarmupIndex()
	started := time.Now()
	bundle, err := recovery.OpenBundle(app.Config.Dump.Path, app.Config.Whisper.DataDir)
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
	if !cs.HasMappedIndex() || bundle.ReadIndexID() == "" || cs.RecoveryIndexID() != bundle.ReadIndexID() {
		_ = bundle.Close()
		logger.Info("pending read checkpoint does not match read index; using ordered restore")
		return false, nil
	}
	preparedNames := 0
	if err = cs.PreparePendingReadIndex(func(visit func(string) error) error {
		return bundle.NewNames(func(name string) error {
			preparedNames++
			return visit(name)
		})
	}); err != nil {
		_ = bundle.Close()
		return false, err
	}
	prepared := time.Now()
	if err = core.AttachPendingRecovery(bundle); err != nil {
		_ = bundle.Close()
		return false, err
	}
	gate := make(chan struct{})
	cs.SetStartupScanGate(gate)
	defer close(gate)
	if err = app.listenCarbonserver(core, newMetrics); err != nil {
		return false, err
	}
	logger.Info("serving reads from pending checkpoint", zap.Duration("runtime", time.Since(started)),
		zap.Duration("checkpoint_open_time", opened.Sub(started)), zap.Duration("index_wait_time", indexReady.Sub(opened)),
		zap.Duration("pending_index_time", prepared.Sub(indexReady)), zap.Int("pending_new_names", preparedNames),
		zap.Uint64("points", bundle.Points()), zap.Uint64("metrics", bundle.Metrics()))
	if err = core.RecoverPending(nil, app.Config.Dump.RestorePerSecond); err != nil {
		return true, err
	}
	loaded := time.Now()
	for !core.IsEmpty() {
		time.Sleep(10 * time.Millisecond)
	}
	if err = core.FinishPendingRecovery(); err != nil {
		return true, err
	}
	stats := make(map[string]float64)
	app.Persister.Stat(func(name string, value float64) { stats[name] = value })
	logger.Info("pending checkpoint persisted, starting receivers", zap.Duration("load_seconds", loaded.Sub(started)), zap.Duration("drain_seconds", time.Since(loaded)), zap.Any("persister_stats", stats))
	return true, nil
}
