package carbon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const InputLogHandoffProtocol = "input-log-v1"

// InputLogHandoffSupported also permits a capable successor to return to dump
// mode. Its stop setting may be off while it recovers a journal-only handoff.
func InputLogHandoffSupported(c *Config) bool {
	probe := *c
	probe.Dump.InputLogOnly = true
	return validateInputLogOnlyConfig(&probe) == nil
}

func validateInputLogOnlyConfig(c *Config) error {
	if !c.Dump.InputLogOnly {
		return nil
	}
	if c.Dump.Path == "" {
		return errors.New("dump.path is required for input-log-only socket offers and restart timings")
	}
	if !c.InputLog.Enabled || !c.InputLog.BackgroundReplay || !c.Dump.HandoffSupervisor || !c.Dump.StoreInputHandoff ||
		c.Whisper.StorageBackend != "pebble-chunk" || !c.Whisper.Enabled || c.Whisper.StoreMirrorFiles ||
		c.Tags.Enabled || !c.Carbonserver.Enabled || !c.Tcp.Enabled || c.Tcp.BufferSize != 0 || c.Tcp.Compression != "" ||
		c.Udp.Enabled || c.Pickle.Enabled || len(c.Receiver) != 0 {
		return errors.New("dump.input-log-only requires background input-log replay, supervised shared-store TCP handoff, carbonserver, and no tags or other receivers")
	}
	if hasDump(c.Dump.Path) {
		return errors.New("dump.input-log-only requires earlier dump recovery to finish first")
	}
	return nil
}

func (app *App) inputLogSuccessorSupported() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, app.readSuccessor, "-input-log-handoff-protocol", "-config", app.ConfigFilename).Output()
	return err == nil && strings.TrimSpace(string(out)) == InputLogHandoffProtocol
}

// stopWithInputLog keeps accepted input in the cache and journal throughout
// store teardown. The handover closes/syncs the journal only after detaching
// every socket, before allowing the successor to open that journal.
func (app *App) stopWithInputLog(started time.Time) error {
	logger := zapwriter.Logger("dump")
	if r := app.inputReplay; r != nil && r.started {
		// Relog only after the older history has been confirmed to storage.
		// Receivers and persistence stay active while this replay finishes.
		<-r.done
		if r.err != nil {
			return r.err
		}
	}
	if app.startupInput != nil || app.Cache.HasPendingRecovery() {
		return errors.New("input-log-only stop requires previous dump/capture recovery to finish")
	}
	app.resolveSuccessor()
	if !app.storeInputHandoffPossible() || !app.inputHandoffPossible() || !app.inputLogSuccessorSupported() {
		return errors.New("input-log-only stop requires a compatible successor and socket handoff")
	}
	log := app.inputLog.Load()
	if log == nil {
		return errors.New("input-log-only stop has no open input log")
	}
	app.MetricStore.StopCompactionPacing()
	background := app.stopStoreBackground()
	flush := app.startStopFlush()
	<-background
	app.stopExpiration()
	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}
	if app.Carbonserver != nil {
		app.Carbonserver.PauseIndexUpdates()
	}
	app.prepareMetadataCheckpoint(logger)
	flush.again()
	// Store durability must precede the newer journal boundary: otherwise a
	// checkpoint could omit points confirmed while still in a store memtable.
	if err := app.MetricStore.Sync(); err != nil {
		return fmt.Errorf("sync store before input log snapshot: %w", err)
	}
	cached := app.Cache.Size()
	before := log.Metrics().PointsWritten
	if err := log.Relog(); err != nil {
		return fmt.Errorf("relog remaining cache: %w", err)
	}
	logger.Info("cache relogged without dump", zap.Int64("cached_points", int64(cached)),
		zap.Int64("logged_points", log.Metrics().PointsWritten-before))
	flush.settle(preCloseFlushTimeout)
	app.restart.mu.Lock()
	app.restart.out = &restartRecord{Version: 1, PID: os.Getpid(), InputLogOnly: true, StopStarted: started}
	app.restart.mu.Unlock()
	app.recordStop(func(*restartRecord) {})
	h := &inputHandover{receivers: app.Receivers, flushTraces: app.FlushTraces, logger: logger, started: time.Now(), journal: log, cachePoints: app.Cache.Size}
	app.Receivers = nil
	h.timer = time.AfterFunc(inputHandoffTimeout, func() { h.stop("successor did not take input in time") })
	readsStopped := app.handOffStoreInput(logger, h)
	h.stop("successor did not take input")
	app.inputLog.Store(nil) // the handover synced and closed it
	app.recordStop(func(r *restartRecord) { r.InputClosed, r.ReadsStopped = h.closedAt(), readsStopped })
	logger.Info("input log stop finished", zap.Bool("input_handoff", h.transferred), zap.Duration("runtime", time.Since(started)))
	if h.journalErr != nil {
		return h.journalErr
	}
	if !h.transferred {
		return errors.New("input-log-only successor did not take input")
	}
	return nil
}

// The journal handoff has no dump manifest or capture spool. Claim the
// sockets first, then start their ordinary cache/log sinks before Pebble opens.
func (app *App) takeInputLogOnlySockets() error {
	app.inputLogHandoff = true
	if err := os.MkdirAll(app.Config.Dump.Path, 0750); err != nil {
		return err
	}
	claim := app.handoffClaim
	if claim == nil {
		return nil
	}
	app.handoffClaim = nil
	ln, _, err := claim.Take(&net.TCPAddr{})
	if err != nil {
		_ = claim.Close()
		return err
	}
	if ln != nil {
		_ = ln.Close()
		_ = claim.Close()
		return errors.New("input-log-only requires an input-only predecessor")
	}
	app.inheritedInput, err = claim.TakeInput()
	return err
}
