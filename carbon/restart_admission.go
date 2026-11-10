package carbon

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/go-graphite/go-carbon/internal/handoff"
)

// ErrRestartRefused leaves the incumbent serving and persisting. It is only
// returned before stop changes any component; other stop failures are distinct.
var ErrRestartRefused = errors.New("restart refused before stopping the incumbent")

// CheckStartupCaptureAdmission is a read-only probe run by the successor before
// the incumbent starts stopping. It opens no database, listener or recovery file.
// It checks the same configuration and resource fallback conditions as early
// capture. A successful probe is not a throughput or convergence certificate.
func CheckStartupCaptureAdmission(c *Config) error {
	if !c.Dump.Enabled || !c.Dump.HandoffSupervisor || !c.Dump.StoreInputHandoff || !c.InputLog.Enabled {
		return errors.New("durable startup capture requires dump, supervisor, store input handoff and input log")
	}
	if !c.Whisper.Enabled || c.Whisper.StorageBackend != "pebble-chunk" || c.Whisper.StoreMirrorFiles || c.Tags.Enabled ||
		!c.Tcp.Enabled || c.Tcp.BufferSize != 0 || c.Tcp.Compression != "" ||
		c.Udp.Enabled || c.Pickle.Enabled || len(c.Receiver) != 0 {
		return errors.New("configuration cannot take early shared-store TCP capture")
	}
	if !c.Carbonserver.Enabled || !c.Carbonserver.TrieIndex || !c.Carbonserver.ConcurrentIndex ||
		c.Carbonserver.FileListCache == "" || c.Carbonserver.FileListCacheVersion != 2 {
		return errors.New("startup capture requires a checkpoint-capable read index")
	}
	if c.Dump.Path == "" || !filepath.IsAbs(c.Dump.Path) {
		return errors.New("startup capture requires an absolute dump path")
	}
	policy := startupPolicy(filepath.Join(c.Dump.Path, "restart-admission"))
	return checkStartupCaptureHeadroom(policy)
}

func checkStartupCaptureHeadroom(policy startupCapturePolicy) error {
	if policy.available < 64<<20 || policy.spoolBytes < 64<<20 {
		return fmt.Errorf("startup capture lacks safe headroom: memory=%d spool=%d", policy.available, policy.spoolBytes)
	}
	return nil
}

// Called under the lifecycle and app locks, before cancellation, flushes,
// checkpoint freezing, replay changes or writer shutdown.
func (app *App) checkRestartAdmission() error {
	if !app.Config.Dump.RequireStartupCapture {
		return nil
	}
	refuse := func(err error) error { return fmt.Errorf("%w: %w", ErrRestartRefused, err) }
	if err := CheckStartupCaptureAdmission(app.Config); err != nil {
		return refuse(err)
	}
	if app.Cache == nil || app.Cache.Backlogged() ||
		!restartRecoveryReady(app.Cache, app.storeRestoreDone, app.metadataPrewarmGate, app.startupCaptureState.Load()) {
		return refuse(errors.New("incumbent recovery/capture is unfinished or cache capacity is exhausted"))
	}
	if !app.storeInputHandoffPossible() || !app.inputHandoffPossible() || !app.pendingReadsCompatible() {
		return refuse(errors.New("incumbent does not have an input/checkpoint handoff path"))
	}
	if app.Carbonserver.CurrentFileIndex() == nil ||
		!app.Carbonserver.HasMappedIndex() && !app.Carbonserver.UsesSharedCatalogIndex() {
		return refuse(errors.New("incumbent read checkpoint is not ready"))
	}
	if !app.resolveSuccessor() {
		return refuse(errors.New("cannot determine the actual successor executable"))
	}
	if !handoff.StartupCaptureSupported(app.readSuccessor, app.ConfigFilename) {
		return refuse(errors.New("successor rejected startup capture configuration/capacity admission"))
	}
	if !handoff.StartupGenerationSupported(app.readSuccessor) {
		return refuse(errors.New("successor cannot capture input if the optional read checkpoint fails"))
	}
	if !handoff.AsyncTailSealSupported(app.readSuccessor) {
		return refuse(errors.New("successor cannot capture input while the predecessor seals its tail"))
	}
	return nil
}
