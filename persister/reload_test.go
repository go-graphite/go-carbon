package persister

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	whisper "github.com/go-graphite/go-whisper"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

func TestReloadPreservesRestartAndAutomaticRate(t *testing.T) {
	for _, mode := range []int{200, -1} {
		synctest.Test(t, func(t *testing.T) {
			budget := restartcpu.NewWithCPU(12, func() float64 { return 0 })
			budget.SetSteadyRate(100)
			p := &Whisper{recv: func(exit chan bool) string {
				select {
				case <-exit:
					return ""
				case <-time.After(time.Millisecond):
					return "metric"
				}
			}}
			p.SetWorkers(4)
			p.SetMetricStore(&autoReloadWarmStore{})
			p.SetWarmFirst(true)
			p.SetRestartBudget(budget, func() float64 { return 50 })
			p.SetMaxUpdatesPerSecond(mode)
			state := func() cache.WriteoutState {
				return cache.WriteoutState{Metrics: 1000, MeanDelay: time.Second, MaxDelay: time.Minute}
			}
			p.SetAutoUpdateRate(state, 100, 2000)
			var written atomic.Uint64
			p.SetMockStore(func() (StoreFunc, func()) {
				return func(string) { written.Add(1); p.recordUpdate() }, nil
			})
			if err := p.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { p.Stop() }()
			time.Sleep(time.Second)
			synctest.Wait()
			pending, ceiling, updates := p.warmPending, budget.State().Ceiling, written.Load()
			p.autoUpdate.mu.Lock()
			controller := p.autoUpdate.controller
			p.autoUpdate.mu.Unlock()
			next := &Whisper{}
			next.SetMaxUpdatesPerSecond(mode)
			next.SetAutoUpdateRate(state, 100, 2000)
			next.SetSparse(true)
			next.SetStoreUpdateBatchSize(8)
			p.ReloadSettings(next)
			if p.warmPending != pending || !budget.Active() || budget.State().Ceiling != ceiling {
				t.Fatal("reload reset warm-first pipeline or restart allowance")
			}
			if mode == -1 && !p.autoUpdate.controller.started.Equal(controller.started) {
				t.Fatal("automatic controller restarted")
			}
			time.Sleep(time.Second)
			synctest.Wait()
			if written.Load() == updates {
				t.Fatal("reload stopped writeout")
			}

			p.StopForReload()
			if budget.State().Ceiling != ceiling {
				t.Fatal("rebuild cleared restart allowance")
			}
			next.InheritRuntime(p)
			next.recv = func(exit chan bool) string { <-exit; return "" }
			next.SetMetricStore(p.metricStore)
			if next.restartBudget != budget || !next.warmFirst || next.autoUpdate.controller.started != p.autoUpdate.controller.started {
				t.Fatal("rebuild lost runtime progress")
			}
			if err := next.Start(); err != nil {
				t.Fatal(err)
			}
			next.Stop()
		})
	}
}

func TestRebuildDoesNotReenterCompletedStartup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := restartcpu.NewWithCPU(12, func() float64 { return 0 })
		budget.Warmed()
		time.Sleep(31 * time.Minute)
		if budget.Active() {
			t.Fatal("fixture did not leave restart pacing")
		}
		ready, gate := make(chan struct{}), make(chan struct{})
		close(ready)
		p := &Whisper{recv: func(exit chan bool) string { <-exit; return "" }}
		p.SetWarmFirst(true)
		p.SetMetricStore(&autoReloadWarmStore{})
		p.SetRestartBudget(budget, nil)
		p.SetStartupGate(ready, gate, time.Minute, func() bool { return true })
		var released atomic.Int64
		p.SetStartupRelease(func(time.Time) { released.Add(1) })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		p.StopForReload()
		next := &Whisper{recv: p.warmOriginalRecv}
		next.SetMetricStore(p.metricStore)
		next.InheritRuntime(p)
		if err := next.Start(); err != nil {
			t.Fatal(err)
		}
		defer next.Stop()
		synctest.Wait()
		if released.Load() != 1 || next.startupGate != nil || next.warmPending != nil || budget.Active() {
			t.Fatal("rebuild reentered completed startup")
		}
	})
}

func TestReloadConversionAndReclaimSettings(t *testing.T) {
	p := &Whisper{rootPath: t.TempDir()}
	p.SetConversionIOSlots(2)
	old := p.conversionSlots()
	old.Acquire()
	p.SetConversionIOSlots(3)
	if used, slots := p.ConversionIOSlots(); used != 1 || slots != 3 {
		t.Fatalf("reload lost active conversion: %d/%d", used, slots)
	}
	old.Release()
	if used, slots := p.ConversionIOSlots(); used != 0 || slots != 3 {
		t.Fatalf("conversion slots = %d/%d", used, slots)
	}
	p.SetDeferredReclaim(time.Hour)
	defer p.StopDeferredReclaim()
	r := p.reclaimer()
	if !r.push(reclaimJob{name: "pending", path: "pending.wsp"}) {
		t.Fatal("queue rejected job")
	}
	p.SetDeferredReclaim(2 * time.Hour)
	if p.reclaimer() != r || p.DeferredReclaimQueued() != 1 || r.delay != 2*time.Hour {
		t.Fatal("reload lost deferred cleanup")
	}
	p.SetDeferredReclaim(0)
	if r.push(reclaimJob{name: "inline"}) || p.DeferredReclaimQueued() != 1 {
		t.Fatal("disabled reclaimer accepted work or discarded pending cleanup")
	}
}

// Exercise the real file path as well as the settings snapshot: a newly created
// metric and an existing metric's online migration both use the reloaded policy.
func TestReloadAppliesFileAndMigrationPolicies(t *testing.T) {
	root := t.TempDir()
	cache := &fakeCache{}
	schemas := func(def string) WhisperSchemas {
		retentions, err := ParseRetentionDefs(def)
		if err != nil {
			t.Fatal(err)
		}
		return WhisperSchemas{{Name: "all", Pattern: regexp.MustCompile(".*"), Retentions: retentions}}
	}
	p := NewWhisper(root, schemas("1s:2h"), NewWhisperAggregation(), func(exit chan bool) string { <-exit; return "" }, cache.pop, cache.confirm, cache.pop)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	now := time.Now().Unix() - 10
	cache.add("reload.existing", now, 7)
	p.store("reload.existing")
	aggregation := NewWhisperAggregation()
	aggregation.Default.aggregationMethod = whisper.Sum
	aggregation.Default.xFilesFactor = .25
	next := NewWhisper(root, schemas("2s:2h"), aggregation, nil, nil, nil, nil)
	next.SetSparse(true)
	next.SetFLock(true)
	next.SetCompressed(true)
	next.SetRemoveEmptyFile(true)
	next.EnableOnlineMigration(100, nil)
	next.EnableOutOfOrder(100, 65536)
	next.SetOutOfOrderCompactionPolicy(10, time.Minute, time.Hour)
	p.ReloadSettings(next)
	// Online migration takes a throttle token without blocking. Wait for the
	// reloaded ticker to produce capacity so a loaded runner cannot skip it.
	deadline := time.Now().Add(5 * time.Second)
	for len(p.settings().onlineMigration.ticker.C) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("online migration ticker produced no capacity after reload")
		}
		time.Sleep(time.Millisecond)
	}
	for _, name := range []string{"reload.created", "reload.existing"} {
		cache.add(name, now+2, 9)
		p.store(name)
		path := filepath.Join(root, "reload", strings.TrimPrefix(name, "reload.")+".wsp")
		w, err := whisper.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if w.Retentions()[0].SecondsPerPoint() != 2 || w.AggregationMethod() != whisper.Sum || w.XFilesFactor() != .25 {
			t.Errorf("%s retained old schema/aggregation", name)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Disabling policies must also take effect without stale enabled flags.
	plain := NewWhisper(root, next.schemas, aggregation, nil, nil, nil, nil)
	p.ReloadSettings(plain)
	if p.settings().onlineMigration.enabled || p.settings().outOfOrder.enabled || p.settings().compressed {
		t.Fatal("disabled policies remained enabled")
	}
}

func TestReloadRateModesRepeatedly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var written atomic.Uint64
		p := &Whisper{recv: func(exit chan bool) string {
			select {
			case <-exit:
				return ""
			case <-time.After(time.Millisecond):
				return "metric"
			}
		}}
		p.SetWorkers(1)
		p.SetMaxUpdatesPerSecond(200)
		state := func() cache.WriteoutState { return cache.WriteoutState{MaxDelay: time.Minute} }
		p.SetAutoUpdateRate(state, 80, 80)
		p.SetMockStore(func() (StoreFunc, func()) { return func(string) { written.Add(1) }, nil })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		for _, mode := range []int{200, -1, 200, 0, -1, 0} {
			next := &Whisper{}
			next.SetMaxUpdatesPerSecond(mode)
			next.SetAutoUpdateRate(state, 80, 80)
			p.ReloadSettings(next)
			time.Sleep(time.Second)
			synctest.Wait()
			before := written.Load()
			time.Sleep(time.Second)
			synctest.Wait()
			rate := written.Load() - before
			want := uint64(200)
			if mode == -1 {
				want = 80
			}
			if mode == 0 {
				want = 1000
			}
			if rate < want*95/100 || rate > want*105/100 {
				t.Fatalf("mode=%d rate=%d want=%d", mode, rate, want)
			}
		}
	})
}

func TestRebuildRetainsStartupDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ready, done := make(chan struct{}), make(chan struct{})
		close(ready)
		p := &Whisper{recv: func(exit chan bool) string { <-exit; return "" }}
		p.SetStartupGate(ready, done, time.Minute, nil)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(45 * time.Second)
		p.StopForReload()
		next := &Whisper{recv: p.recv}
		next.InheritRuntime(p)
		if next.startupWait != 15*time.Second {
			t.Fatalf("rebuild reset deadline: %v", next.startupWait)
		}
		if err := next.Start(); err != nil {
			t.Fatal(err)
		}
		defer next.Stop()
		time.Sleep(16 * time.Second)
		synctest.Wait()
		select {
		case <-next.startupGate:
		default:
			t.Fatal("rebuild extended startup gate")
		}
	})
}

func TestReloadEnforcesSmallAutomaticLimitChanges(t *testing.T) {
	p := &Whisper{recv: func(exit chan bool) string { <-exit; return "" }}
	state := func() cache.WriteoutState { return cache.WriteoutState{MaxDelay: time.Minute} }
	p.SetMaxUpdatesPerSecond(-1)
	p.SetAutoUpdateRate(state, 1000, 1000)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	for _, bounds := range [][2]int{{1005, 1005}, {1, 999}} {
		next := &Whisper{}
		next.SetMaxUpdatesPerSecond(-1)
		next.SetAutoUpdateRate(state, bounds[0], bounds[1])
		p.ReloadSettings(next)
		rate, _, _ := p.autoUpdateObservation()
		if rate < float64(bounds[0]) || rate > float64(bounds[1]) {
			t.Fatalf("rate=%g outside reloaded bounds %v", rate, bounds)
		}
	}
}

func TestReloadConversionSlotsRetainsActiveGrants(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &Whisper{}
		p.SetConversionIOSlots(2)
		slots := p.conversionSlots()
		slots.Acquire()
		slots.Acquire()
		p.SetConversionIOSlots(1)
		acquired := make(chan struct{})
		go func() { slots.Acquire(); close(acquired) }()
		synctest.Wait()
		slots.Release()
		synctest.Wait()
		select {
		case <-acquired:
			t.Fatal("lower limit admitted work before active holders drained")
		default:
		}
		slots.Release()
		<-acquired
		if used, limit := p.ConversionIOSlots(); used != 1 || limit != 1 {
			t.Fatalf("slots=%d/%d", used, limit)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := slots.acquire(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter: %v", err)
		}
		p.SetConversionIOSlots(0)
		slots.Acquire()
		p.SetConversionIOSlots(1)
		if used, limit := p.ConversionIOSlots(); used != 2 || limit != 1 {
			t.Fatalf("unlimited switch lost accounting: %d/%d", used, limit)
		}
		slots.Release()
		slots.Release()
	})
}
