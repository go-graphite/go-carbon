package carbon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// PrepareSharedStoreForHandoff runs the operator's verified finalizer after a
// stopping predecessor offered its handoff. At this boundary its persister is
// stopped and input is retained by its ordinary dump/tail protocol. Call before
// Start: no successor writer or storage handle may compete with the finalizer.
func (app *App) PrepareSharedStoreForHandoff(ctx context.Context, program string) error {
	if program == "" {
		return nil
	}
	app.Lock()
	defer app.Unlock()
	cfg := app.Config
	if cfg != nil && cfg.Whisper.StoreLiveMigration {
		return errors.New("live storage migration must not run fenced storage preparation")
	}
	if cfg == nil || cfg.Whisper.StorageBackend != "pebble-chunk" || cfg.Whisper.StoreEncoding != "exact" || !cfg.Whisper.StoreMirrorFiles || !cfg.Dump.Enabled || cfg.Tags.Enabled {
		return errors.New("storage preparation requires exact mirrored storage, enabled dumps and untagged input")
	}
	if err := validateStorageConfig(cfg); err != nil {
		return err
	}
	if app.handoffClaim == nil {
		return errors.New("storage preparation requires a claimed predecessor handoff; never assert source quiescence on a live writer")
	}
	if !filepath.IsAbs(program) {
		return errors.New("storage preparation executable must be an absolute path")
	}
	command := exec.CommandContext(ctx, program)
	command.Env = append(os.Environ(), "GO_CARBON_PREPARE_SOURCE="+cfg.Whisper.DataDir, "GO_CARBON_PREPARE_STORE="+sharedStorePath(cfg))
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("prepare shared storage: %w", err)
	}
	if err := checkStoreConversion(sharedStorePath(cfg)); err != nil {
		return fmt.Errorf("storage preparation left a serving guard: %w", err)
	}
	return nil
}
