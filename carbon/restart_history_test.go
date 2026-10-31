package carbon

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestRestartCPUAllowanceLogging(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, fresh := range []bool{false, true} {
		t.Run(strconv.FormatBool(fresh), func(t *testing.T) {
			defer zapwriter.Test()()
			path, cfg := sharedAppConfig(t)
			cfg.Whisper.StoreMetadataCachePrewarm = true
			cfg.Whisper.StoreMetadataCacheSize = 1 << 20
			cfg.Whisper.StoreMetadataCachePrewarmCPUCores = 48
			if fresh {
				db, err := store.Open(cfg.Whisper.StoreDir, store.Options{CacheSize: 1 << 20, MemTableSize: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				host, _ := os.Hostname()
				data, err := json.Marshal(map[string]any{
					"Host":   host,
					"Steady": map[string]any{"At": time.Now().Add(-time.Minute), "Cores": 16},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(cfg.Whisper.StoreDir, "restart-cpu.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			writeSharedConfig(t, path, cfg)
			app := New(path)
			if err := app.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			defer app.Stop()
			if _, err := app.startStorage(); err != nil {
				t.Fatal(err)
			}
			source, cores := `"fallback"`, "48"
			if fresh {
				source, cores = `"history"`, "16.8"
			}
			logs := zapwriter.TestString()
			if !strings.Contains(logs, "restart CPU allowance") || !logHas(logs, "source", source) || !logHas(logs, "cores", cores) {
				t.Fatal("allowance log omitted selection", logs)
			}
			if strings.Contains(logs, "using conservative allowance") {
				t.Fatal("configured fallback was reported as the host-derived default", logs)
			}
		})
	}
}

func TestMetadataPrewarmCompletesRestartBudget(t *testing.T) {
	db, err := store.Open(t.TempDir(), store.Options{MetadataCacheSize: 1 << 20, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	synctest.Test(t, func(t *testing.T) {
		defer db.StopBackground()
		budget := restartcpu.NewWithCPU(48, func() float64 { return 0 })
		ready, gate := make(chan struct{}), make(chan struct{})
		done := startMetadataPrewarm(db, whisperConfig{StoreMetadataCachePrewarm: true}, "test", nil, ready, nil, func() { close(gate) }, budget)
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("prewarm finished before input readiness")
		default:
		}
		close(ready)
		<-done
		select {
		case <-gate:
		default:
			t.Fatal("prewarm did not release the completion gate")
		}
		// Active remains true after 30 minutes unless production prewarm
		// completion called Warmed, including without a metadata checkpoint.
		time.Sleep(31 * time.Minute)
		if budget.Active() {
			t.Fatal("completed prewarm did not mark the restart budget warmed")
		}
		if err := budget.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRestartHistoryState(t *testing.T) {
	core := cache.New()
	defer core.Stop()
	core.SetMaxSize(100)
	if err := core.SetWriteoutBatching(100, 960*time.Second); err != nil {
		t.Fatal(err)
	}
	restored := make(chan struct{})
	state := restartHistoryState(core, restored)
	core.Add(points.OnePoint("live", 1, time.Now().Unix()))
	if _, ready := state(); ready {
		t.Fatal("history admitted unfinished dump loading")
	}
	close(restored)
	core.Add(points.OnePoint("live", 2, time.Now().Unix()))
	if size, ready := state(); size != 2 || !ready || core.WriteoutReady("live") {
		t.Fatal("ordinary batching was mistaken for restoration", size, ready)
	}
	for range 75 {
		core.Add(points.OnePoint("live", 3, time.Now().Unix()))
	}
	if _, ready := state(); ready {
		t.Fatal("cache pressure was recorded as steady")
	}
}

func TestRestartHistoryWaitsForPendingConfirmation(t *testing.T) {
	_, path := intakeTailFixture(t, 2, 2, true, 1, 0)
	dir := filepath.Dir(path)
	bundle, err := recovery.OpenBundle(dir, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	core := cache.New()
	defer core.Stop()
	if err := core.AttachPendingRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	if err := core.SetWriteoutBatching(100, 1800*time.Second); err != nil {
		t.Fatal(err)
	}
	core.PauseStartupWriteout()
	core.ResumeStartupWriteout(time.Now())
	state := restartHistoryState(core, nil)
	if _, ready := state(); ready {
		t.Fatal("unclaimed restoration was recorded as steady")
	}
	if err := core.RecoverPending(nil, 0); err != nil {
		t.Fatal(err)
	}
	for name := range core.MetricNames() {
		p, ok := core.PopNotConfirmed(name)
		if !ok {
			t.Fatal("restored metric missing", name)
		}
		if _, ready := state(); ready {
			t.Fatal("claimed or in-flight restoration was recorded as steady")
		}
		core.Confirm(p)
	}
	if _, ready := state(); !ready {
		t.Fatal("confirmed restoration still blocked history")
	}
}
