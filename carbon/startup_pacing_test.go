package carbon

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

// Exercise the receiver-gating driver, queue and real rate tokens. There is no
// live input to hide an exponentially shrinking demand behind growing batches.
func TestReceiverRestoreDrainUsesAutoMax(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		app := New("")
		app.Config = NewConfig()
		app.Config.Dump.Path = t.TempDir()
		core := cache.New()
		app.Cache = core
		core.SetMaxSize(0)
		core.SetWriteoutPacing(true)
		if err := core.SetWriteoutBatching(960, 1800*time.Second); err != nil {
			t.Fatal(err)
		}
		const metrics, ceiling = 1600, 800
		bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x3ff0000000000001}
		for i := range metrics {
			core.AddRestoredBeforeInput(points.OnePoint(fmt.Sprintf("restore.%d", i), math.Float64frombits(bits[i%len(bits)]), int64(1700000000+i)))
		}
		p := persister.NewWhisper("", nil, nil, core.WriteoutQueue().Get, core.PopNotConfirmed, core.Confirm, core.Pop)
		p.SetWorkers(8)
		p.SetMaxUpdatesPerSecond(-1)
		p.SetAutoUpdateRate(core.WriteoutState, 1, ceiling)
		var mu sync.Mutex
		receipts := make(map[string]points.Point)
		p.SetMockStore(func() (persister.StoreFunc, func()) {
			return func(name string) {
				batch, ok := core.PopNotConfirmed(name)
				if !ok || len(batch.Data) != 1 {
					t.Error("missing restore receipt", name)
					return
				}
				mu.Lock()
				if _, duplicate := receipts[name]; duplicate {
					t.Error("duplicate restore receipt", name)
				}
				receipts[name] = batch.Data[0]
				mu.Unlock()
				core.Confirm(batch)
			}, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		app.Persister = p
		started := time.Now()
		done := make(chan struct{})
		go func() { app.restoreBeforeReceivers(core); close(done) }()
		// Include the five-second controller polling interval plus 1600/800s
		// of paced work. The steady 1800s deadline must not determine startup.
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			// Let the driver finish even on the unfixed implementation.
			p.Stop()
			for i := range metrics {
				if batch, ok := core.PopNotConfirmed(fmt.Sprintf("restore.%d", i)); ok {
					core.Confirm(batch)
				}
			}
			<-done
			t.Fatal("zero-input restore did not drain at auto-max pace")
		}
		mu.Lock()
		defer mu.Unlock()
		if len(receipts) != metrics {
			t.Fatal("lost restore receipts", len(receipts))
		}
		for i := range metrics {
			point := receipts[fmt.Sprintf("restore.%d", i)]
			if point.Timestamp != int64(1700000000+i) || math.Float64bits(point.Value) != bits[i%len(bits)] {
				t.Fatal("restore point bits changed", i)
			}
		}
		t.Logf("zero-input restore: %d bit-exact receipts in %v (auto-max %d/s, poll 5s)", len(receipts), time.Since(started), ceiling)
	})
}
