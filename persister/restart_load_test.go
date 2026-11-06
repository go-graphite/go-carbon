package persister

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

func TestRestartWorkersCatchUpAfterTripledLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart-cpu.json")
	synctest.Test(t, func(t *testing.T) {
		host, _ := os.Hostname()
		data, err := json.Marshal(map[string]any{
			"Host": host, "Steady": map[string]any{"At": time.Now().Add(-time.Minute), "Cores": 10}, "Updates": 1000,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		selected := restartcpu.Start(context.Background(), path, 120, 1.5)
		defer selected.Stop()
		// Keep actual history selection; replace the OS meter with deterministic
		// mandatory ingest/read CPU plus the confirmed writes' cost.
		started := time.Now()
		var writes atomic.Uint64
		budget := restartcpu.NewWithCPU(selected.Cores(), func() float64 {
			return 30*time.Since(started).Seconds() + float64(writes.Load())*.005
		})
		budget.SetSteadyRate(selected.SteadyRate())
		time.Sleep(time.Second)
		p := &Whisper{recv: func(chan bool) string { return "survivor.metric" }}
		p.SetWorkers(96)
		p.SetMaxUpdatesPerSecond(-1)
		p.SetRestartBudget(budget, nil)
		p.SetAutoUpdateRate(func() cache.WriteoutState {
			return cache.WriteoutState{Metrics: 3_000_000, MaxDelay: 1000 * time.Second, MeanDelay: 1000 * time.Second}
		}, 1, 0)
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(string) { time.Sleep(time.Millisecond); writes.Add(1); p.recordUpdate() }, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		previous := writes.Load()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		rate := float64(writes.Load()-previous) / 5
		t.Logf("workers=96 history=10 configured=120 mandatory=30 allowance=%.0f actual=%.1f/s current_input=3000/s floor=1000/s", budget.Cores(), rate)
		if rate < 3000 || budget.State().FloorGrants != 0 {
			t.Fatal("tripled input still outpaced real workers with configured headroom", rate, budget.State())
		}
	})
}
