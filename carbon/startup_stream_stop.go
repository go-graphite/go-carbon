package carbon

import (
	"errors"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Do not publish a newer cache checkpoint ahead of an unreplayed older suffix:
// ordinary chronological recovery would then replay that suffix too early.
// Every external point is already either persisted or in an authoritative
// capture segment, so preserve those sources and hand over the sockets directly.
func (app *App) stopStreamingInputForHandoff(started time.Time) error {
	s := app.startupInput
	s.pauseReplay()
	app.stopPendingRecovery()
	app.resolveSuccessor()
	logger := zapwriter.Logger("dump")
	app.stopExpiration()
	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}
	flush := app.startStopFlush()
	flush.settle(preCloseFlushTimeout)
	h := &inputHandover{receivers: app.Receivers, logger: logger, started: started, path: s.path, capture: s}
	app.Receivers = nil
	if app.storeInputHandoffPossible() && app.Config.Dump.StoreInputHandoff && handoff.StoreInputSupported(app.readSuccessor) && app.readRelease != nil {
		app.handOffStoreInput(logger, h)
	} else {
		h.stop("capture queue stop without an input-capable successor")
	}
	if err := s.close(); err != nil {
		return err
	}
	app.startupInput = nil
	app.closeInputLog()
	app.markHeartbeatClean()
	logger.Info("capture sources retained across restart without checkpoint reordering", zap.Int64("captured_points", s.points.Load()), zap.Int64("replayed_points", s.replayedPoints.Load()))
	if s.sealErr != nil {
		return errors.Join(s.closeErr, s.sealErr)
	}
	return nil
}
