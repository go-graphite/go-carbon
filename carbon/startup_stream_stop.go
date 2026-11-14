package carbon

import (
	"errors"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Save the cache together with exact replay frontiers before handing over.
// The resume cursor orders the saved cache before every unread source suffix;
// ordinary chronological dump recovery cannot express that boundary.
func (app *App) stopStreamingInputForHandoff(started time.Time) error {
	s := app.startupInput
	if app.MetricStore != nil {
		app.MetricStore.StopCompactionPacing()
	}
	<-app.stopStoreBackground()
	s.pauseReplay()
	app.stopPendingRecovery()
	app.resolveSuccessor()
	logger := zapwriter.Logger("dump")
	app.stopExpiration()
	app.stopReanchorSweep()
	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}
	// Join reclamation before recording source frontiers. Capture keeps appending.
	s.stopOnce.Do(func() { close(s.stop) })
	if s.monitored.Load() {
		<-s.done
	}
	s.syncWG.Wait()
	if err := app.saveStartupResume(s); err != nil {
		return err
	}
	// Reads and capture input remain available during metadata preparation.
	app.prepareMetadataCheckpoint(logger)
	flush := app.startStopFlush()
	flush.settle(preCloseFlushTimeout)
	h := &inputHandover{receivers: app.Receivers, logger: logger, started: started, path: s.path, capture: s}
	app.Receivers = nil
	if app.storeInputHandoffPossible() && app.Config.Dump.StoreInputHandoff && handoff.StoreInputSupported(app.readSuccessor) && app.readRelease != nil {
		app.handOffStoreInput(logger, h)
	} else {
		h.stop("capture queue stop without an input-capable successor")
	}
	h.stop("capture handoff complete")
	if err := s.close(); err != nil {
		return err
	}
	app.startupInput = nil
	app.closeInputLog()
	app.markHeartbeatClean()
	logger.Info("capture replay snapshot and cursors saved for restart", zap.Int64("captured_points", s.points.Load()), zap.Int64("replayed_points", s.replayedPoints.Load()))
	if s.sealErr != nil {
		return errors.Join(s.closeErr, s.sealErr)
	}
	return nil
}
