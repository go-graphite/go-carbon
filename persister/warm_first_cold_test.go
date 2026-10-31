package persister

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
)

type slowColdWarmer struct {
	controlledWarmer
	cpu     *atomic.Uint64
	entered chan struct{}
	once    sync.Once
	calls   atomic.Uint64
	filled  atomic.Uint64
}

func (w *slowColdWarmer) PrewarmWriteout(ctx context.Context, names []string) {
	w.calls.Add(1)
	w.once.Do(func() { close(w.entered) })
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		w.cpu.Add(uint64(len(names)) * 1000)
		w.filled.Add(uint64(len(names)))
	case <-ctx.Done():
	}
}

func TestWarmFirstColdBacklogMaintainsSteadyWritesAndPrefetchCPU(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := cache.New()
		defer core.Stop()
		core.SetMaxSize(0)
		var workMicros, coldWrites atomic.Uint64
		started := time.Now()
		used := func() float64 { return .5*time.Since(started).Seconds() + float64(workMicros.Load())/1e6 }
		budget := restartcpu.NewWithCPU(2, used)
		budget.SetSteadyRate(100)
		warmer := &slowColdWarmer{cpu: &workMicros, entered: make(chan struct{})}
		core.SetRestartOrder(budget.Active)
		core.SetRestartWarmFirst(warmer.MetadataCached, budget.WaitPrefetch, budget.EndPrefetch)
		add := func(i int) {
			prefix := "cold"
			// The initial backlog is entirely cold; subsequent input is 15/16
			// cold. Start prefetch before launching its 96 consumers.
			if i >= 1200 && i%16 == 0 {
				prefix = "warm"
			}
			core.Add(points.OnePoint(fmt.Sprintf("%s.%d", prefix, i), float64(i), int64(i)))
		}
		for i := range 1200 {
			add(i)
		}
		p := &Whisper{recv: core.WriteoutQueue().Get}
		p.SetWriteoutBatchReceiver(core.WriteoutQueue().TryGet)
		p.SetMetricStore(warmer)
		p.SetRestartBudget(budget, func() float64 { return 100 })
		p.SetWarmFirst(true)
		p.workersCount = 0 // Launch exactly 96 workers after prefetch has entered.
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(name string) {
				if _, ok := core.Pop(name); ok {
					workMicros.Add(3000) // Includes the ordinary cold read-through.
					if strings.HasPrefix(name, "cold") {
						coldWrites.Add(1)
					}
					p.recordUpdate()
				}
			}, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		<-warmer.entered
		if state := budget.State(); state.Warming <= 0 || state.WarmGrants == 0 {
			t.Fatal("required prefetch did not receive available CPU", state)
		}
		for range 96 {
			p.Go(p.worker)
		}
		stopInput, inputDone := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(inputDone)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for i := 1200; ; i++ {
				select {
				case <-stopInput:
					return
				case <-ticker.C:
					add(i)
				}
			}
		}()
		defer func() { close(stopInput); <-inputDone }()
		for second := 1; second <= 5; second++ {
			before := budget.Updates()
			time.Sleep(time.Second)
			synctest.Wait()
			if written := budget.Updates() - before; written < 100 {
				t.Fatalf("cold prefetch starved steady writeout at second %d: %d < 100 writes/s", second, written)
			}
			if cpu := used(); cpu > 2*time.Since(started).Seconds()+.25 {
				t.Fatal("read-through bypassed shared CPU allowance", cpu)
			}
		}
		if core.Size() >= 1200 || warmer.calls.Load() < 2 || coldWrites.Load() <= warmer.filled.Load() {
			t.Fatalf("backlog did not shrink alongside warming and cold read-through: points=%d calls=%d cold_writes=%d prefetched=%d", core.Size(), warmer.calls.Load(), coldWrites.Load(), warmer.filled.Load())
		}
	})
}
