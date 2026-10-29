package carbon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/persister"
)

func TestPrepareSharedStoreRequiresHandoffAndClearedGuards(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	_, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMirrorFiles = true
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Buckyd.Enabled = false
	cfg.Dump.Enabled = true
	cfg.Tags.Enabled = false
	if err := os.MkdirAll(cfg.Whisper.StoreDir, 0700); err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(t.TempDir(), "prepare")
	if err := os.WriteFile(program, []byte("#!/bin/sh\nset -eu\nprintf 'executed' > \"$GO_CARBON_PREPARE_STORE/HOOK-EXECUTED\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	app := New("")
	app.Config = cfg
	if err := app.PrepareSharedStoreForHandoff(context.Background(), program); err == nil || !strings.Contains(err.Error(), "claimed") {
		t.Fatalf("unfenced preparation accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Whisper.StoreDir, "HOOK-EXECUTED")); !os.IsNotExist(err) {
		t.Fatal("program ran before the handoff fence")
	}
	// The gate test supplies a claim sentinel. Real socket and input-tail
	// inheritance is covered by the separate application/native handoff tests.
	app.handoffClaim = &handoff.Claim{}
	if err := app.PrepareSharedStoreForHandoff(context.Background(), "relative"); err == nil {
		t.Fatal("relative executable accepted")
	}
	marker := filepath.Join(cfg.Whisper.StoreDir, "NEEDS-CATCHUP")
	if err := os.WriteFile(marker, []byte("retain"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := app.PrepareSharedStoreForHandoff(context.Background(), program); err == nil || !strings.Contains(err.Error(), "guard") {
		t.Fatalf("incomplete finalizer accepted: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("preparation removed a serving guard")
	}
	// A different ready fixture exercises successful execution without clearing
	// or weakening the guarded fixture above.
	cfg.Whisper.StoreDir = t.TempDir()
	if err := app.PrepareSharedStoreForHandoff(context.Background(), program); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Whisper.StoreDir, "HOOK-EXECUTED")); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.PrepareSharedStoreForHandoff(cancelled, program); err == nil {
		t.Fatal("cancelled preparation accepted")
	}
}

func TestMirroredFileIndexKeepsPhysicalQuotas(t *testing.T) {
	_, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMirrorFiles = true
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Buckyd.Enabled = false
	cfg.Whisper.Quotas = make(persister.WhisperQuotas, 1)
	cfg.Whisper.Quotas[0].PhysicalSize = 12345
	cfg.Whisper.PebbleChunkIgnorePhysicalQuotas = true
	if err := validateStorageConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.getCarbonserverQuotas(time.Minute)[0].PhysicalSize; got != 12345 {
		t.Fatalf("mirrored original-file quota lost: %d", got)
	}
	cfg.Whisper.PebbleChunkIgnorePhysicalQuotas = false
	if err := validateStorageConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.StoreMirrorFiles = false
	if err := validateStorageConfig(cfg); err == nil {
		t.Fatal("shared-only physical quota silently accepted")
	}
}
