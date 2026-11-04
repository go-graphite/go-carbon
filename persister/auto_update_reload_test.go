package persister

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
)

// A shard scan (for example the auto sampler followed by an input-log writer)
// must not stop every writer behind one warm-first queue reader. Reproduce the
// contention after replacing a fixed persister and retiring restart ordering.
func TestAutoUpdateReloadRestartEndContendedShard(t *testing.T) {
	core := cache.New()
	defer core.Stop()
	core.SetMaxSize(0)
	core.EnableInputLog()
	if err := core.SetWriteoutBatching(960, 1800*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := core.SetWriteStrategy("lexical"); err != nil {
		t.Fatal(err)
	}
	// Age only the budget in fake time. The contention below uses real time:
	// mutex waits are not durable blocks that let synctest advance its clock.
	var budget *restartcpu.Budget
	synctest.Test(t, func(t *testing.T) {
		budget = restartcpu.NewWithCPU(20, func() float64 { return 0 })
		budget.SetSteadyRate(1000)
		budget.Warmed()
		time.Sleep(31 * time.Minute)
	})
	var drained atomic.Bool
	budget.SetDrain(drained.Load)
	db := new(autoReloadWarmStore)
	core.SetRestartOrder(budget.Active)
	core.SetRestartWarmFirst(db.MetadataCached, budget.WaitPrefetch, budget.EndPrefetch)
	var p *Whisper
	defer func() { p.Stop() }()
	start := func(mode int, receive <-chan struct{}) {
		core.SetWriteoutPacing(mode == -1)
		if p != nil {
			p.Stop()
		}
		p = NewWhisper("", nil, nil, func(exit chan bool) string {
			if receive != nil {
				select {
				case <-receive:
				case <-exit:
					return ""
				}
			}
			return core.WriteoutQueue().Get(exit)
		}, core.PopNotConfirmed, core.Confirm, core.Pop)
		p.SetWriteoutBatchReceiver(core.WriteoutQueue().TryGet)
		p.SetStoreUpdateBatchSize(8)
		p.SetRequeue(core.Requeue)
		p.SetWriteoutReady(core.WriteoutReady)
		p.SetMetricStore(db)
		p.SetMaxUpdatesPerSecond(mode)
		p.SetAutoUpdateRate(core.WriteoutState, 1000, 1000)
		p.SetWorkers(96)
		p.SetRestartBudget(budget, nil)
		p.SetWarmFirst(true)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
	}
	start(50000, nil)
	receive := make(chan struct{})
	start(-1, receive)
	drained.Store(true)
	restartDeadline := time.Now().Add(time.Minute)
	for budget.Active() && time.Now().Before(restartDeadline) {
		time.Sleep(time.Millisecond)
	}
	if budget.Active() {
		t.Fatal("restart pacing did not end")
	}

	// Populate and build the steady-state queue while its consumers wait.
	// The first queued name belongs to the shard we will hold; all other
	// names belong to unrelated shards and can be persisted immediately.
	const blocked = "a.blocked"
	data := make([]points.Point, 960)
	for i := range data {
		data[i] = points.Point{Value: 1, Timestamp: time.Now().Unix()}
	}
	core.Add(&points.Points{Metric: "0.prepare", Data: data})
	core.Add(&points.Points{Metric: blocked, Data: data})
	const available = 256
	for i, added := 0, 0; added < available; i++ {
		name := fmt.Sprintf("b.ready.%04d", i)
		if core.GetShard(name) != core.GetShard(blocked) {
			core.Add(&points.Points{Metric: name, Data: data})
			added++
		}
	}
	if name := core.WriteoutQueue().Get(make(chan bool)); name != "0.prepare" {
		t.Fatal("unexpected queue order", name)
	}
	core.Pop("0.prepare")

	// The throttle runs under the real shard write lock. Hold that lock
	// without production-scale data or timing assumptions about the scan.
	locked, release, added := make(chan struct{}), make(chan struct{}), make(chan struct{})
	core.SetThrottle(func(p *points.Points, _ bool) bool {
		if p.Metric == blocked {
			close(locked)
			<-release
		}
		return false
	})
	go func() {
		core.Add(points.OnePoint(blocked, 2, time.Now().Unix()))
		close(added)
	}()
	<-locked
	close(receive)
	deadline := time.Now().Add(time.Minute)
	for db.written.Load() < available && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	written := db.written.Load()
	close(release)
	<-added
	if written != available {
		t.Fatalf("restart ended: wrote %d/%d unrelated metrics while one shard was locked", written, available)
	}
	t.Logf("restart ended: wrote %d/%d unrelated metrics while one shard was locked", written, available)
	deadline = time.Now().Add(time.Minute)
	for (db.written.Load() != available+1 || core.Len() != 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if db.written.Load() != available+1 || core.Len() != 0 {
		t.Fatal("blocked metric was lost or duplicated after the shard unlocked")
	}
}

type autoReloadStore struct {
	failedSharedStore
	written, batches atomic.Uint64
}

type autoReloadWarmStore struct{ autoReloadStore }

func (*autoReloadWarmStore) MetadataCached(string) bool                { return true }
func (*autoReloadWarmStore) PrewarmWriteout(context.Context, []string) {}

func (s *autoReloadStore) UpdateMany(context.Context, string, []points.Point) error {
	s.written.Add(1)
	return nil
}

func (s *autoReloadStore) UpdateBatch(_ context.Context, updates []store.MetricUpdate, _ *store.ContinuationMaintenance) error {
	s.written.Add(uint64(len(updates)))
	s.batches.Add(1)
	return nil
}

// ReloadConfig stops the old persister, then configures and starts a new one
// against the same restart CPU budget. Keep queue supply unlimited here so a
// shortfall can only come from admission, not eligibility or storage latency.
func TestAutoUpdateLiveReload(t *testing.T) {
	for _, tc := range []struct {
		name        string
		active      bool
		saved, cost float64
	}{
		{"restart_active", true, 1200, .0001},
		{"restart_no_history", true, 0, .0001},
		{"restart_cpu_bound", true, 1200, .0006},
		{"restart_complete", false, 1200, .0001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const allowance = 15.9
				const target = 26500
				started := time.Now()
				db := new(autoReloadStore)
				used := func() float64 { return 5*time.Since(started).Seconds() + float64(db.written.Load())*tc.cost }
				budget := restartcpu.NewWithCPU(allowance, used)
				// A quiet predecessor batching window can leave low throughput
				// history. Fixed restart pacing compensates using the queue floor.
				budget.SetSteadyRate(tc.saved)
				if !tc.active {
					budget.Warmed()
					time.Sleep(31 * time.Minute)
				}
				var p *Whisper
				var sequence atomic.Uint64
				next := func() string { return "reload.metric." + strconv.FormatUint(sequence.Add(1), 10) }
				defer func() {
					if p != nil {
						p.Stop()
					}
				}()
				for _, mode := range []int{50000, -1, 50000, -1} {
					if p != nil {
						p.Stop()
					}
					p = &Whisper{recv: func(exit chan bool) string {
						select {
						case <-exit:
							return ""
						default:
							return next()
						}
					}, pop: func(name string) (*points.Points, bool) { return points.OnePoint(name, 1, 1), true }}
					// Setter order matches carbon.App.startPersister.
					p.SetWriteoutBatchReceiver(next)
					p.SetStoreUpdateBatchSize(8)
					p.SetRequeue(func(*points.Points) { t.Error("unexpected failed write") })
					p.SetWriteoutReady(func(string) bool { return true })
					p.SetMetricStore(db)
					p.SetMaxUpdatesPerSecond(mode)
					p.SetAutoUpdateRate(func() cache.WriteoutState {
						return cache.WriteoutState{Metrics: 29_700_000, Points: 400_000_000,
							Eligible: 7_000_000, OldestAge: 1807 * time.Second,
							MaxDelay: 1800 * time.Second, MeanDelay: 1800 * time.Second}
					}, 1, target)
					p.SetWorkers(4)
					p.SetRestartBudget(budget, func() float64 { return 20144 })
					if err := p.Start(); err != nil {
						t.Fatal(err)
					}
					// Let fixed ticker handover and any inherited CPU debt settle.
					time.Sleep(time.Second)
					synctest.Wait()
					before, cpu := db.written.Load(), used()
					time.Sleep(2 * time.Second)
					synctest.Wait()
					rate := float64(db.written.Load()-before) / 2
					cores := (used() - cpu) / 2
					reported, _, _ := p.autoUpdateObservation()
					t.Logf("mode=%d restart=%v reported=%.0f/s actual=%.0f/s cpu=%.2f allowance=%.2f reservation_ceiling=%.0f/s",
						mode, budget.Active(), reported, rate, cores, allowance, budget.State().Ceiling)
					cpuBound := tc.active && 5+target*tc.cost > allowance
					if mode == -1 && (reported != target || !cpuBound && rate < target || rate > target*1.01) {
						t.Errorf("automatic admission starved or exceeded target: actual=%.0f/s target=%d/s", rate, target)
					}
					if mode == 50000 && !cpuBound {
						want := 50000.
						if tc.active {
							want = 3 * 20144
						}
						if rate < want*.99 || rate > want*1.01 {
							t.Errorf("fixed admission regressed: %.0f/s want %.0f/s", rate, want)
						}
					}
					// Only the mandatory floor can exceed a stale CPU allowance.
					floorCPU := 5 + 20144*tc.cost
					if tc.active && cores > max(allowance, floorCPU)+.2 {
						t.Errorf("restart catch-up exceeded allowance and mandatory floor: %.2f", cores)
					}
					if cpuBound && floorCPU > allowance && (rate < 20144*.99 || rate > 20144*1.01) {
						t.Errorf("CPU overload must admit the floor while withholding catch-up: %.0f/s", rate)
					}
					if cpuBound && rate < 16000 {
						t.Errorf("restart CPU headroom left idle: %.0f/s", rate)
					}
				}
				if db.batches.Load() == 0 {
					t.Fatal("writeout batching was not exercised")
				}
			})
		})
	}
}

// The same persister must keep its automatic admission when the shared budget
// ends, including when that persister replaced a fixed-rate one during restart.
func TestAutoUpdateReloadThroughRestartEnd(t *testing.T) {
	for _, tc := range []struct{ initial, workers int }{{50000, 1}, {50000, 96}, {-1, 1}, {-1, 96}} {
		t.Run(strconv.Itoa(tc.initial)+"/workers="+strconv.Itoa(tc.workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const target = 26500
				db := new(autoReloadStore)
				budget := restartcpu.NewWithCPU(20.46, func() float64 { return 0 })
				budget.SetSteadyRate(31150)
				budget.Warmed()
				var drained atomic.Bool
				budget.SetDrain(drained.Load)
				// Skip the minimum startup window before starting workers; an
				// unfinished checkpoint drain keeps restart pacing active.
				time.Sleep(31 * time.Minute)
				var sequence atomic.Uint64
				next := func() string { return "restart.end." + strconv.FormatUint(sequence.Add(1), 10) }
				var p *Whisper
				defer func() {
					if p != nil {
						p.Stop()
					}
				}()
				start := func(mode int) {
					if p != nil {
						p.Stop()
					}
					p = &Whisper{recv: func(exit chan bool) string {
						select {
						case <-exit:
							return ""
						default:
							return next()
						}
					}, pop: func(name string) (*points.Points, bool) { return points.OnePoint(name, 1, 1), true }}
					p.SetWriteoutBatchReceiver(next)
					p.SetStoreUpdateBatchSize(8)
					p.SetRequeue(func(*points.Points) { t.Error("unexpected failed write") })
					p.SetWriteoutReady(func(string) bool { return true })
					p.SetMetricStore(db)
					p.SetMaxUpdatesPerSecond(mode)
					p.SetAutoUpdateRate(func() cache.WriteoutState {
						return cache.WriteoutState{Metrics: 29_700_000, Eligible: 7_000_000,
							Points: 400_000_000, MaxDelay: 1800 * time.Second,
							MeanDelay: 1800 * time.Second, OldestAge: 1807 * time.Second}
					}, 1, target)
					p.SetWorkers(tc.workers)
					p.SetRestartBudget(budget, func() float64 { return 31150 })
					if err := p.Start(); err != nil {
						t.Fatal(err)
					}
				}
				check := func(stage string, want float64) {
					t.Helper()
					time.Sleep(time.Second)
					synctest.Wait()
					before := db.written.Load()
					time.Sleep(2 * time.Second)
					synctest.Wait()
					rate := float64(db.written.Load()-before) / 2
					reported, targetRate, _ := p.autoUpdateObservation()
					t.Logf("%s: active=%v reported=%.0f/s actual=%.0f/s", stage, budget.Active(), reported, rate)
					if rate < want*.99 || rate > want*1.01 {
						t.Errorf("%s: actual=%.0f/s want=%.0f/s", stage, rate, want)
					}
					if p.maxUpdatesPerSecond == -1 && (reported != want || targetRate != want) {
						t.Errorf("%s: reported=%g target=%g want=%d", stage, reported, targetRate, target)
					}
				}
				start(tc.initial)
				if tc.initial != -1 {
					check("fixed restart", 3*31150)
					start(-1)
				}
				check("auto restart", 31150)
				drained.Store(true)
				check("auto restart ended", target)
				if budget.Active() || !p.restartStopping {
					t.Fatal("restart observer did not finish")
				}
				check("auto after sampler", target)
				start(50000)
				check("fixed after restart", 50000)
			})
		})
	}
}

// Keep the real cache queue, eligibility phases and warm-first pump across a
// reload and restart end. Scale the host's population and rates by 1000.
func TestAutoUpdateCacheReloadThroughRestartEnd(t *testing.T) {
	t.Parallel()
	for _, initial := range []int{50, -1} {
		t.Run(strconv.Itoa(initial), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				const metrics = 29700
				const delay = 1800 * time.Second
				core := cache.New()
				defer core.Stop()
				core.SetMaxSize(0)
				core.EnableInputLog()
				core.SetWriteoutRestoreSpread(true)
				core.SetWriteoutSparseSpread(true)
				core.SetWriteoutPacing(initial == -1)
				if err := core.SetWriteoutBatching(960, delay); err != nil {
					t.Fatal(err)
				}
				if err := core.SetWriteoutSpread(.25); err != nil {
					t.Fatal(err)
				}
				if err := core.SetWriteStrategy("noop"); err != nil {
					t.Fatal(err)
				}
				budget := restartcpu.NewWithCPU(20.46, func() float64 { return 0 })
				budget.SetSteadyRate(31.15)
				budget.Warmed()
				var drained atomic.Bool
				budget.SetDrain(drained.Load)
				db := new(autoReloadWarmStore)
				core.SetRestartOrder(budget.Active)
				core.SetRestartWarmFirst(db.MetadataCached, budget.WaitPrefetch, budget.EndPrefetch)
				var names [60][][]byte
				for i := range metrics {
					name := "cache.reload." + strconv.Itoa(i)
					names[i%60] = append(names[i%60], []byte(name))
					core.AddRestored(points.OnePoint(name, 1, time.Now().Unix()))
				}
				var p *Whisper
				defer func() {
					if p != nil {
						p.Stop()
					}
				}()
				start := func(mode int) {
					core.SetWriteoutPacing(mode == -1)
					if p != nil {
						p.Stop()
					}
					p = NewWhisper("", nil, nil, core.WriteoutQueue().Get, core.PopNotConfirmed, core.Confirm, core.Pop)
					p.SetWriteoutBatchReceiver(core.WriteoutQueue().TryGet)
					p.SetStoreUpdateBatchSize(8)
					p.SetRequeue(core.Requeue)
					p.SetBacklogged(core.Backlogged)
					// Model the pending source retiring together with restart
					// pacing; recovery may keep the sampled target at its ceiling.
					p.SetRecovering(func() bool { return !drained.Load() })
					p.SetWriteoutReady(core.WriteoutReady)
					p.SetMetricStore(db)
					p.SetMaxUpdatesPerSecond(mode)
					p.SetAutoUpdateRate(core.WriteoutState, 1, 27)
					p.SetWorkers(4)
					p.SetRestartBudget(budget, func() float64 { return float64(core.Len()) / delay.Seconds() })
					p.SetWarmFirst(true)
					if err := p.Start(); err != nil {
						t.Fatal(err)
					}
				}
				start(initial)
				var bins [90]uint64
				for second := range 90 * 60 {
					if second == 13*60 && initial != -1 {
						start(-1)
					}
					if second == 45*60 {
						drained.Store(true)
					}
					if second%60 == 0 {
						for shard := range cache.ShardCount {
							core.TakeLogEntries(shard, uint64(second+1), nil)
						}
					}
					for _, name := range names[second%60] {
						core.AddPoint(name, 1, time.Now().Unix())
					}
					before := db.written.Load()
					time.Sleep(time.Second)
					synctest.Wait()
					bins[second/60] += db.written.Load() - before
				}
				t.Logf("minute writes: %v", bins)
				if budget.Active() {
					t.Fatal("restart did not end")
				}
				for minute := 45; minute < len(bins); minute++ {
					if bins[minute] < uint64(metrics/30)*9/10 {
						t.Errorf("minute %d: %d writes, need %d", minute, bins[minute], metrics/30)
					}
				}
			})
		})
	}
}

// The restart observer may still be waiting for startup gates when automatic
// demand changes. Sampling must update CPU reservations without that observer,
// and a stopped persister must not republish its ceiling on the shared budget.
func TestAutoUpdateRestartReservationFollowsSampler(t *testing.T) {
	for _, saved := range []float64{0, 1, 1000} {
		t.Run(strconv.FormatFloat(saved, 'f', 0, 64), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
				budget.SetSteadyRate(saved)
				var metrics atomic.Int64
				metrics.Store(1000)
				p := &Whisper{recv: func(exit chan bool) string { <-exit; return "" }}
				p.SetMaxUpdatesPerSecond(-1)
				p.SetAutoUpdateRate(func() cache.WriteoutState {
					return cache.WriteoutState{Metrics: metrics.Load(), MaxDelay: time.Minute, MeanDelay: time.Minute}
				}, 1, 100)
				p.SetStartupGate(make(chan struct{}), make(chan struct{}), time.Hour, nil)
				p.SetRestartBudget(budget, nil)
				check := func() {
					t.Helper()
					rate, _, _ := p.autoUpdateObservation()
					want := 0.
					if saved > 0 {
						want = 3 * max(rate, saved)
					}
					if ceiling := budget.State().Ceiling; ceiling != want {
						t.Fatalf("restart reservation did not follow auto sampler: ceiling=%g auto=%g", ceiling, rate)
					}
					if p.restartTicker.Load() != nil || p.updateTokens() != p.autoUpdate.ticker.Load().C {
						t.Fatal("restart installed competing rate tokens")
					}
				}
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				defer p.Stop()
				check()
				metrics.Store(10000)
				time.Sleep(autoUpdateInterval)
				synctest.Wait()
				check()
				if rate, _, _ := p.autoUpdateObservation(); rate != max(100, saved) {
					t.Fatal("sample did not apply auto rate and mandatory floor", rate)
				}
				p.Stop()
				p.sampleAutoUpdateRate()
				p.setRestartRate()
				if budget.State().Ceiling != 0 {
					t.Fatal("stopped persister republished its CPU reservation ceiling")
				}
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				check()
			})
		})
	}
}
