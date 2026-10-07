package carbon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExpirationReloadAndIndexWithoutBuckyd(t *testing.T) {
	for _, trie := range []bool{false, true} {
		name := "trigram"
		if trie {
			name = "trie"
		}
		t.Run(name, func(t *testing.T) {
			app, policyPath := startExpirationIndexApp(t, trie)
			enableExpirationAndCheckIndex(t, app, policyPath)
			previous := app.expirer
			writeExpirationPolicy(t, policyPath, "[bad]\npattern = [\nexpiration = 1s\n")
			if err := app.ReloadConfig(); err == nil || app.expirer != previous {
				t.Fatal("invalid reload replaced live expiration policy")
			}
			writeExpirationPolicy(t, policyPath, "[jobs]\npattern = ^jobs\\.\nexpiration = 0s\n")
			if err := app.ReloadConfig(); err != nil {
				t.Fatal(err)
			}
			if app.expirer != nil {
				t.Fatal("disabled policy kept worker alive")
			}
			select {
			case <-previous.done:
			default:
				t.Fatal("reload did not join previous worker")
			}
		})
	}
}

func startExpirationIndexApp(t *testing.T, trie bool) (*App, string) {
	t.Helper()
	path, cfg := sharedAppConfig(t)
	cfg.Buckyd.Enabled = false
	cfg.Whisper.Enabled = false
	cfg.Carbonserver.TrieIndex, cfg.Carbonserver.TrigramIndex = trie, !trie
	cfg.Carbonserver.ScanFrequency = &Duration{time.Hour}
	cfg.Whisper.StoreExpirationFilename = filepath.Join(t.TempDir(), "expiration.conf")
	writeExpirationPolicy(t, cfg.Whisper.StoreExpirationFilename, "[jobs]\npattern = ^jobs\\.\nexpiration = 0s\n")
	writeSharedConfig(t, path, cfg)
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{Now: func() time.Time { return time.Now().Add(-48 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	createExpirationMetric(t, db, "jobs.old")
	createExpirationMetric(t, db, "permanent")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	if app.Buckyd != nil || app.expirer != nil || app.metricStoreIndex == nil {
		t.Fatal("unexpected startup workers")
	}
	// Exercise the same coalescing worker with a short test cadence.
	app.Lock()
	app.metricStoreIndex.close()
	app.metricStoreIndex = startMetricIndexRefresher(app.Carbonserver, 5*time.Millisecond)
	app.Unlock()
	if err := app.Carbonserver.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Carbonserver.Find(context.Background(), &protov2.GlobRequest{Query: "jobs.old"}); err != nil {
		t.Fatalf("initial index missing metric: %v", err)
	}
	return app, cfg.Whisper.StoreExpirationFilename
}

func enableExpirationAndCheckIndex(t *testing.T, app *App, policyPath string) {
	t.Helper()
	writeExpirationPolicy(t, policyPath, "[jobs]\npattern = ^jobs\\.\nexpiration = 24h\n")
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.expirer == nil || app.Config.Whisper.StoreExpiration.Value() != 0 {
		t.Fatal("override-only expiration did not start")
	}
	waitExpiration(t, func() bool {
		_, err := app.Carbonserver.Find(context.Background(), &protov2.GlobRequest{Query: "jobs.old"})
		return status.Code(err) == codes.NotFound
	})
	if _, err := app.MetricStore.Metadata(context.Background(), "jobs.old"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired metric still in catalog: %v", err)
	}
	if _, err := app.MetricStore.Metadata(context.Background(), "permanent"); err != nil {
		t.Fatalf("unmatched metric deleted: %v", err)
	}
}

func TestExpirationConfigLoadsWithGlobalDisabled(t *testing.T) {
	cfg := NewConfig()
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.Enabled = false
	cfg.Whisper.StoreExpirationFilename = filepath.Join(t.TempDir(), "missing")
	if err := loadExpirationConfig(cfg); err == nil {
		t.Fatal("configured missing file accepted with global expiration and persister disabled")
	}
	cfg.Whisper.StorageBackend = "files"
	if err := loadExpirationConfig(cfg); err != nil {
		t.Fatalf("file backend read expiration policy: %v", err)
	}
}

func TestExpirationStartupWaitsForPendingDump(t *testing.T) {
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.Enabled = false
	cfg.Buckyd.Enabled, cfg.Carbonserver.Enabled = false, false
	cfg.Whisper.StoreExpiration = &Duration{time.Hour}
	cfg.Dump.Enabled = true
	cfg.Dump.Path = t.TempDir()
	cfg.Dump.RestorePerSecond = 1
	writeSharedConfig(t, path, cfg)
	now := time.Now()
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{Now: func() time.Time { return now.Add(-48 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	original := createExpirationMetric(t, db, "jobs.restored")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	dump := filepath.Join(cfg.Dump.Path, "input.1.1")
	if err := os.WriteFile(dump, []byte(fmt.Sprintf("jobs.restored 42 %d\n", now.Unix())), 0600); err != nil {
		t.Fatal(err)
	}
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	if app.storeRestoreDone == nil || app.expirer == nil || app.expirer.restored != app.storeRestoreDone {
		t.Fatal("expiration is not waiting on startup restore")
	}
	waitExpiration(t, func() bool { return app.expirationStats.lastSuccess.Load() != 0 })
	got, err := app.MetricStore.Metadata(context.Background(), original.Name)
	if err != nil || got.ID != original.ID {
		t.Fatalf("cleanup removed metric while dump was pending: %+v, %v", got, err)
	}
	queued := app.Cache.Get(original.Name)
	if len(queued) != 1 || queued[0].Value != 42 || app.expirationStats.pending.Load() != 1 {
		t.Fatalf("restored points not protected: points=%v pending=%d", queued, app.expirationStats.pending.Load())
	}
}

func writeExpirationPolicy(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func waitExpiration(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for expiration")
		}
		time.Sleep(time.Millisecond)
	}
}
