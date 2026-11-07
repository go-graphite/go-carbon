//go:build unix

package carbon

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

// A nonempty earlier journal is the canary's slow path: the monitor must drain
// its sources rather than waiting for input to fill steady-state batches.
func TestStartupDirectDrainsEarlierSourcesAtZeroInput(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, _ := directCaptureFixture(t)
	const metrics = 128
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x3ff0000000000001}
	for i := range metrics {
		core.Add(points.OnePoint(fmt.Sprintf("earlier.%d", i), math.Float64frombits(bits[i%len(bits)]), 1700000000))
	}
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	app.closeInputLog()
	core = cache.New()
	core.SetMaxSize(0)
	core.SetWriteoutPacing(true)
	if err := core.SetWriteoutBatching(960, 1800*time.Second); err != nil {
		t.Fatal(err)
	}
	app.Cache = core
	if err := app.openInputLog(core); err != nil {
		t.Fatal(err)
	}
	app.Config.Whisper.MaxUpdatesPerSecond = -1
	app.Config.Whisper.MaxUpdatesPerSecondAutoMin = 1
	app.Config.Whisper.MaxUpdatesPerSecondAutoMax = metrics
	app.Config.Whisper.Workers = 4
	p, err := app.newPersister()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	receipts := make(map[string]points.Point)
	p.SetMockStore(func() (persister.StoreFunc, func()) {
		return func(name string) {
			batch, ok := core.PopNotConfirmed(name)
			if !ok || len(batch.Data) != 1 {
				t.Error("missing journal receipt", name)
				return
			}
			mu.Lock()
			if _, duplicate := receipts[name]; duplicate {
				t.Error("duplicate journal receipt", name)
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
	app.finishStartupInput(core)
	s.startMonitor(1 << 30)
	for !s.direct.Load() && time.Since(started) < 25*time.Second {
		time.Sleep(10 * time.Millisecond)
	}
	if !s.direct.Load() {
		t.Fatal("zero-input direct switch did not finish within 25s", zapwriter.TestString())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(receipts) != metrics {
		t.Fatal("direct switch passed unpersisted earlier journal", len(receipts))
	}
	for i := range metrics {
		point := receipts[fmt.Sprintf("earlier.%d", i)]
		if point.Timestamp != 1700000000 || math.Float64bits(point.Value) != bits[i%len(bits)] {
			t.Fatal("journal receipt bits changed", i)
		}
	}
	t.Logf("zero-input direct switch in %v; %d bit-exact earlier-journal receipts", time.Since(started), len(receipts))
}
