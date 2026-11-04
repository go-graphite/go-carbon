package persister

import (
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRestartWorkersProgressBelowMandatoryCPU(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const floor = 569.4254382153856
		started := time.Now()
		var micros atomic.Uint64
		budget := restartcpu.NewWithCPU(.12365959574006986*1.5, func() float64 {
			return .28*time.Since(started).Seconds() + float64(micros.Load())/1e6
		})
		budget.SetSteadyRate(floor)
		time.Sleep(time.Second)
		p := &Whisper{recv: func(exit chan bool) string {
			select {
			case <-exit:
				return ""
			default:
				return "metric"
			}
		}}
		p.SetWorkers(8)
		p.SetMaxUpdatesPerSecond(50000)
		p.SetRestartBudget(budget, func() float64 { return floor })
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(string) { micros.Add(100); p.recordUpdate() }, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		rate := float64(budget.Updates()) / 10
		t.Logf("workers=8 writeout=%.2f/s floor=%.2f/s allowance=%.6f process=%.3f floor_grants=%d", rate, floor, budget.Cores(), budget.State().Process, budget.State().FloorGrants)
		if rate < .99*floor || rate > 1.05*floor || budget.State().FloorGrants == 0 {
			t.Fatal("real workers did not maintain the mandatory floor", rate, budget.State())
		}
	})
}

func TestRestartWriteoutErrorOncePerEscalation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := make(startupLogWriter, 32)
		logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(logs), zap.WarnLevel))
		budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
		backlog := false
		p := &Whisper{logger: logger}
		p.SetRestartBudget(budget, func() float64 { return 100 })
		p.SetRestartBacklog(func() bool { return backlog })
		// A nonempty but deferred batch may be silent for the full max delay.
		p.observeRestartWriteout(0)
		time.Sleep(16 * time.Minute)
		p.observeRestartWriteout(0)
		if len(logs) != 0 {
			t.Fatal("warned during ordinary deferred batching")
		}
		backlog = true
		for second := 0; second <= 600; second++ {
			p.observeRestartWriteout(0)
			if second == 200 {
				// A structural reload retains the incident and its counter.
				next := &Whisper{logger: logger}
				next.InheritRuntime(p)
				if next.restartBacklog == nil {
					t.Fatal("reload lost eligible-backlog sampler")
				}
				p = next
			}
			time.Sleep(time.Second)
		}
		warnings, errors := 0, 0
		for len(logs) > 0 {
			line := <-logs
			switch {
			case strings.Contains(line, `"level":"error"`) && strings.Contains(line, "restart writeout stalled"):
				errors++
			case strings.Contains(line, `"level":"warn"`) && strings.Contains(line, "restart writeout below floor"):
				warnings++
			default:
				t.Fatal("unexpected diagnostic", line)
			}
		}
		if warnings != 2 || errors != 1 || budget.State().Stalls != 1 {
			t.Fatal("incorrect warning/escalation logging", warnings, errors, budget.State())
		}
		p.observeRestartWriteout(100)
		if budget.State().Stalled {
			t.Fatal("successful writes did not clear stalled gauge")
		}
	})
}

func TestRestartColdQueueProgressBelowMandatoryCPU(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const floor = 569.4254382153856
		core := cache.New()
		defer core.Stop()
		core.SetMaxSize(0)
		for i := range 12000 {
			core.Add(points.OnePoint("cold."+strconv.Itoa(i), 1, 1))
		}
		started := time.Now()
		var micros atomic.Uint64
		budget := restartcpu.NewWithCPU(.12365959574006986*1.5, func() float64 {
			return .28*time.Since(started).Seconds() + float64(micros.Load())/1e6
		})
		budget.SetSteadyRate(floor)
		warmer := &slowColdWarmer{cpu: &micros, entered: make(chan struct{})}
		core.SetRestartOrder(budget.Active)
		core.SetRestartWarmFirst(warmer.MetadataCached, budget.WaitPrefetch, budget.EndPrefetch)
		p := &Whisper{recv: core.WriteoutQueue().Get}
		p.SetWriteoutBatchReceiver(core.WriteoutQueue().TryGet)
		p.SetWorkers(8)
		p.SetMaxUpdatesPerSecond(50000)
		p.SetMetricStore(warmer)
		p.SetWarmFirst(true)
		p.SetRestartBudget(budget, func() float64 { return floor })
		p.SetRestartBacklog(func() bool { return core.WriteoutState().Eligible > 0 })
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(name string) {
				if _, ok := core.Pop(name); ok {
					micros.Add(100)
					p.recordUpdate()
				}
			}, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(time.Second)
		synctest.Wait()
		previous := budget.Updates()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		rate := float64(budget.Updates()-previous) / 5
		t.Logf("cold queue writeout=%.1f/s floor=%.1f/s prefetched=%d", rate, floor, warmer.filled.Load())
		if rate < .95*floor || warmer.filled.Load() == 0 {
			t.Fatal("required preparation starved mandatory writeout", rate, budget.State())
		}
	})
}
