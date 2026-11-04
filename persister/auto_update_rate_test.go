package persister

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
)

func steadyUpdateState() cache.WriteoutState {
	return cache.WriteoutState{Metrics: 1_000_000, Points: 8_000_000,
		MaxDelay: 960 * time.Second, MeanDelay: 960 * time.Second}
}

func TestAutoUpdateController(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := steadyUpdateState()
	want := float64(s.Metrics) / s.MeanDelay.Seconds() * autoUpdateMargin
	t.Run("steady convergence", func(t *testing.T) {
		c := autoUpdateController{at: now, started: now, rate: want * 3, need: want * 3 / autoUpdateMargin, equilibrium: float64(s.Points)}
		var rate int
		for i := 1; i <= 84; i++ {
			rate = c.step(now.Add(time.Duration(i)*autoUpdateInterval), s, 1, 0)
		}
		if math.Abs(float64(rate)/want-1) > .02 {
			t.Fatalf("after 84 intervals: rate=%d want=%g", rate, want)
		}
	})
	t.Run("input step", func(t *testing.T) {
		c := autoUpdateController{}
		before := c.step(now, s, 1, 0)
		s := s
		s.Metrics *= 2
		if rate := c.step(now.Add(autoUpdateInterval), s, 1, 0); rate < 2*before-1 {
			t.Fatalf("input growth waited for an age violation: before=%d after=%d", before, rate)
		}
	})
	t.Run("cache growth", func(t *testing.T) {
		c := autoUpdateController{}
		before := c.step(now, s, 1, 0)
		s := s
		s.Points *= 2
		rate := c.step(now.Add(s.MaxDelay), s, 1, 0)
		if rate <= before || c.reason != "growth" {
			t.Fatalf("growth guard: %d -> %d (%s)", before, rate, c.reason)
		}
	})
	t.Run("capacity", func(t *testing.T) {
		c := autoUpdateController{}
		s := s
		s.MaxPoints = 10_000_000
		if rate := c.step(now, s, 1, 0); rate < 5000 || c.reason != "pressure" {
			t.Fatalf("capacity guard: %d (%s)", rate, c.reason)
		}
	})
	t.Run("byte capacity", func(t *testing.T) {
		c := autoUpdateController{}
		s := s
		s.Pressure = true
		if rate := c.step(now, s, 1, 0); rate < 20_000 || c.reason != "pressure" {
			t.Fatalf("byte capacity guard: %d (%s)", rate, c.reason)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		c := autoUpdateController{}
		s := s
		s.Eligible, s.OldestAge = 500_000, s.MaxDelay-10*time.Second
		if rate := c.step(now, s, 1, 0); rate < 1900 || c.reason != "deadline" {
			t.Fatalf("deadline guard did not act before max delay: %d (%s)", rate, c.reason)
		}
		s.OldestAge = s.MaxDelay + autoUpdateSlack(s.MaxDelay)
		if rate := c.step(now.Add(autoUpdateInterval), s, 1, 0); rate < 100_000 {
			t.Fatalf("overdue queue not released within one interval: %d", rate)
		}
	})
	t.Run("limits", func(t *testing.T) {
		c := autoUpdateController{}
		if rate := c.step(now, s, 1, 100); rate != 100 || c.reason != "ceiling" {
			t.Fatalf("ceiling=%d (%s)", rate, c.reason)
		}
		c = autoUpdateController{}
		if rate := c.step(now.Add(autoUpdateInterval), cache.WriteoutState{}, 200, 0); rate != 200 || c.reason != "floor" {
			t.Fatalf("floor=%d (%s)", rate, c.reason)
		}
	})
}

func TestAutoUpdateRateModes(t *testing.T) {
	for _, fixed := range []int{0, 13700, 23000} {
		p := &Whisper{}
		p.SetMaxUpdatesPerSecond(fixed)
		p.SetAutoUpdateRate(func() cache.WriteoutState { t.Fatal("fixed mode sampled auto state"); return cache.WriteoutState{} }, 1, 10)
		p.SetBacklogged(func() bool { return true })
		p.recv = func(exit chan bool) string { <-exit; return "" }
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		if p.autoUpdate.ticker.Load() != nil || p.maxUpdatesPerSecond != fixed {
			t.Fatal("fixed mode changed")
		}
		if p.throttleUpdates() {
			t.Fatal("fixed mode lost its backlog bypass")
		}
		p.Stop()
	}
	t.Run("recovery remains bounded", func(t *testing.T) {
		var recovering atomic.Bool
		p := &Whisper{}
		p.SetMaxUpdatesPerSecond(-1)
		p.SetBacklogged(func() bool { return true })
		p.SetRecovering(recovering.Load)
		if !p.throttleUpdates() {
			t.Fatal("auto pressure escaped its ceiling")
		}
		recovering.Store(true)
		if !p.throttleUpdates() {
			t.Fatal("recovery bypassed auto admission")
		}
		recovering.Store(false)
		if !p.throttleUpdates() {
			t.Fatal("recovery bypass outlived recovery")
		}
	})
	t.Run("restart allowance", func(t *testing.T) {
		for _, predecessor := range []float64{0, 100} {
			synctest.Test(t, func(t *testing.T) {
				p := &Whisper{recv: func(exit chan bool) string { <-exit; return "" }}
				p.SetMaxUpdatesPerSecond(-1)
				p.SetAutoUpdateRate(steadyUpdateState, 1, 10)
				b := restartcpu.NewWithCPU(2, func() float64 { return 0 })
				b.SetSteadyRate(predecessor)
				p.SetRestartBudget(b, nil)
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				defer p.Stop()
				if rate, _, _ := p.autoUpdateObservation(); rate != max(10, predecessor) {
					t.Fatalf("restart did not apply mandatory floor: %g", rate)
				}
				if p.updateTokens() != p.autoUpdate.ticker.Load().C || p.restartTicker.Load() != nil {
					t.Fatal("restart installed a competing auto admission budget")
				}
			})
		}
	})
}

// A full cache scan can take seconds: /metrics, the stats collector and the
// restart pacer must not wait for it. 1 Nov 2026: 15-17 s scrapes on a canary.
func TestAutoUpdateScanDoesNotBlockReaders(t *testing.T) {
	for _, reload := range []bool{false, true} {
		t.Run(fmt.Sprintf("reload=%v", reload), func(t *testing.T) {
			p := &Whisper{}
			p.SetMaxUpdatesPerSecond(-1)
			p.liveSettings.Store(&whisperSettings{maxUpdatesPerSecond: -1, updateTicker: helper.NewThrottleTicker(0)})
			entered, release := make(chan struct{}), make(chan struct{})
			scan := func() cache.WriteoutState {
				close(entered)
				<-release
				return steadyUpdateState()
			}
			p.SetAutoUpdateRate(scan, 1, 10)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if reload {
					next := &Whisper{}
					next.liveSettings.Store(&whisperSettings{maxUpdatesPerSecond: -1, updateTicker: helper.NewThrottleTicker(0)})
					next.SetAutoUpdateRate(scan, 1, 10)
					p.ReloadSettings(next)
				} else {
					p.sampleAutoUpdateRate()
				}
			}()
			<-entered
			observed := make(chan struct{})
			go func() {
				p.autoUpdateObservation()
				p.setRestartRate()
				close(observed)
			}()
			select {
			case <-observed:
			case <-time.After(time.Minute):
				t.Fatal("autoUpdate.mu held during the cache scan")
			}
			close(release)
			<-done
			if rate, _, _ := p.autoUpdateObservation(); rate != 10 {
				t.Fatalf("sample after the scan not applied: %g", rate)
			}
			if ticker := p.autoUpdate.ticker.Load(); ticker != nil {
				ticker.Stop()
			}
		})
	}
}

func TestAutoUpdateLimiterFollowsState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var state atomic.Pointer[cache.WriteoutState]
		initial := steadyUpdateState()
		state.Store(&initial)
		var written atomic.Uint64
		p := &Whisper{recv: func(chan bool) string { return "metric" }}
		p.SetWorkers(1)
		p.SetMaxUpdatesPerSecond(-1)
		p.SetAutoUpdateRate(func() cache.WriteoutState { return *state.Load() }, 1, 0)
		p.SetMockStore(func() (StoreFunc, func()) { return func(string) { written.Add(1) }, nil })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(time.Second)
		synctest.Wait()
		first, _, _ := p.autoUpdateObservation()
		if n := written.Load(); n < uint64(first*.9) || n > uint64(first*1.1) {
			t.Fatalf("effective auto rate not enforced: %d vs %g", n, first)
		}
		next := initial
		next.Metrics *= 2
		state.Store(&next)
		time.Sleep(autoUpdateInterval)
		synctest.Wait()
		if rate, _, _ := p.autoUpdateObservation(); rate < 2*first-1 {
			t.Fatalf("sampling did not raise the real limiter: %g -> %g", first, rate)
		}
	})
}

func TestFixedUpdateLimiterIgnoresAutoLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var written atomic.Uint64
		p := &Whisper{recv: func(chan bool) string { return "metric" }}
		p.SetWorkers(1)
		p.SetMaxUpdatesPerSecond(100)
		p.SetAutoUpdateRate(func() cache.WriteoutState { t.Fatal("sampled auto state in fixed mode"); return cache.WriteoutState{} }, 1, 2)
		p.SetMockStore(func() (StoreFunc, func()) { return func(string) { written.Add(1) }, nil })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(time.Second)
		synctest.Wait()
		if n := written.Load(); n < 99 || n > 101 {
			t.Fatalf("fixed limiter changed: %d updates/s", n)
		}
	})
}

func TestAutoUpdateMetricsReload(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	for i, mode := range []int{-1, 42, -1} {
		p := &Whisper{recv: func(exit chan bool) string { <-exit; return "" }}
		p.SetMaxUpdatesPerSecond(mode)
		p.SetAutoUpdateRate(steadyUpdateState, 1, 0)
		p.InitPrometheus(reg)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		families, err := reg.Gather()
		p.Stop()
		if err != nil {
			t.Fatal(err)
		}
		seen := false
		for _, f := range families {
			if f.GetName() == "persister_auto_update_rate_adjustments_total" {
				seen = true
				for _, metric := range f.Metric {
					if metric.Label[0].GetValue() == "steady" && metric.Counter.GetValue() != float64(i/2+1) {
						t.Fatal("adjustment counts reset across reload")
					}
				}
			}
		}
		if seen != (mode == -1) {
			t.Fatal("metrics still refer to a replaced persister")
		}
	}
}

// Run the real cache, eligibility checks and queue on virtual time. The fake
// persister spends at most one second's rate credit and pops real point batches;
// no modeled queue, age distribution or writeout deadlines replace cache logic.
// Scaling population and cap together keeps this inexpensive under -race.
func TestAutoUpdateSimulation(t *testing.T) {
	t.Parallel()
	const metrics = 5_000
	const referenceMetrics = 1_000_000
	const delay = 960 * time.Second
	const horizon = 12 * delay
	const measureFrom = 8 * delay
	type result struct {
		cv, equilibrium float64
		age             time.Duration
	}
	run := func(auto, step bool) (r result) {
		synctest.Test(t, func(t *testing.T) {
			c := cache.New()
			c.SetMaxSize(0)
			c.SetWriteoutPacing(auto)
			if err := c.SetWriteoutBatching(960, delay); err != nil {
				t.Fatal(err)
			}
			if err := c.SetWriteoutSpread(.25); err != nil {
				t.Fatal(err)
			}
			if err := c.SetWriteStrategy("max"); err != nil {
				t.Fatal(err)
			}
			var names [60][][]byte
			for i := 0; i < metrics; i++ {
				names[i%60] = append(names[i%60], []byte(fmt.Sprintf("simulation.metric.%08d", i)))
			}
			abort, request, next := make(chan bool), make(chan struct{}, 1), make(chan string, 1)
			defer close(abort)
			go func() {
				for {
					select {
					case <-abort:
						return
					case <-request:
						metric := c.WriteoutQueue().Get(abort)
						if metric == "" {
							return
						}
						next <- metric
					}
				}
			}()
			started := time.Now()
			controller := autoUpdateController{}
			pending := false
			var bins []float64
			minute, samples, cacheSum := 0, 0, int64(0)
			rate := 23000 * metrics / referenceMetrics
			for second := 0; time.Since(started) < horizon; second++ {
				arrivals := names[second%60]
				if step && time.Since(started) < 3*delay {
					arrivals = arrivals[:len(arrivals)/2]
				}
				for _, name := range arrivals {
					c.AddPoint(name, 1, time.Now().Unix())
				}
				if auto && second%int(autoUpdateInterval/time.Second) == 0 {
					rate = controller.step(time.Now(), c.WriteoutState(), 1, 0)
				}
				if !pending {
					request <- struct{}{}
					pending = true
				}
				synctest.Wait()
				write := func(metric string) {
					p, exists := c.Pop(metric)
					if !exists {
						t.Fatal("queued absent metric")
					}
					for _, point := range p.Data {
						r.age = max(r.age, time.Since(time.Unix(point.Timestamp, 0)))
					}
					minute++
				}
				select {
				case metric := <-next:
					pending = false
					write(metric)
					for i := 1; i < rate; i++ {
						metric := c.WriteoutQueue().TryGet()
						if metric == "" {
							break
						}
						write(metric)
					}
				default:
				}
				if time.Since(started) >= measureFrom {
					cacheSum += c.Size()
					samples++
				}
				time.Sleep(time.Second)
				if (second+1)%60 == 0 {
					if time.Since(started) > measureFrom {
						bins = append(bins, float64(minute)/60)
					}
					minute = 0
				}
			}
			var sum, squares float64
			// Include unpersisted points at the end, so a point left behind by
			// the fake persister cannot evade the age bound.
			r.age = max(r.age, c.WriteoutState().OldestAge)
			for _, bin := range bins {
				sum += bin
				squares += bin * bin
			}
			mean := sum / float64(len(bins))
			r.cv = math.Sqrt(max(0, squares/float64(len(bins))-mean*mean)) / mean
			r.equilibrium = float64(cacheSum) / float64(samples) * referenceMetrics / metrics
		})
		return
	}
	fixed, auto := run(false, false), run(true, false)
	t.Logf("1M metric equivalent, 1 point/min, delay=960s spread=.25 fixed=23000: cv=%.4f cache=%.0f max_age=%s; auto: cv=%.4f cache=%.0f max_age=%s", fixed.cv, fixed.equilibrium, fixed.age, auto.cv, auto.equilibrium, auto.age)
	if auto.cv > .15 {
		t.Fatalf("automatic writeout remains bursty: cv=%.4f", auto.cv)
	}
	if auto.equilibrium > 1.3*fixed.equilibrium {
		t.Fatalf("cache grew too far: fixed=%g auto=%g", fixed.equilibrium, auto.equilibrium)
	}
	if auto.age > delay+autoUpdateSlack(delay) {
		t.Fatalf("point exceeded bounded slack: %s", auto.age)
	}
	step := run(true, true)
	t.Logf("auto with 2x input step: cv=%.4f cache=%.0f max_age=%s", step.cv, step.equilibrium, step.age)
	if step.age > delay+autoUpdateSlack(delay) || step.cv > .15 || step.equilibrium > 1.3*fixed.equilibrium {
		t.Fatalf("input step failed to settle safely: %+v", step)
	}
}

// A restart observer can still be waiting for readiness after CPU pacing has
// ended. Its installed ticker must not override a steady auto observation.
func TestAutoUpdateExpiredRestartLimiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
		budget.SetSteadyRate(7666)
		budget.Warmed()
		time.Sleep(31 * time.Minute)
		var written atomic.Uint64
		p := &Whisper{recv: func(chan bool) string { return "metric" }}
		p.SetWorkers(96)
		p.SetMaxUpdatesPerSecond(-1)
		state := steadyUpdateState()
		state.Metrics = 11_650_000
		p.SetAutoUpdateRate(func() cache.WriteoutState { return state }, 1, 23000)
		p.SetStartupGate(make(chan struct{}), make(chan struct{}), time.Hour, nil)
		p.SetRestartBudget(budget, nil)
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(string) {
				written.Add(1)
				time.Sleep(time.Millisecond)
			}, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(100 * time.Second)
		synctest.Wait()
		rate, _, reason := p.autoUpdateObservation()
		actual := float64(written.Load()) / 100
		t.Logf("reported %.0f/s (%s), actual %.0f/s, restart active %v, ticker installed %v", rate, reason, actual, budget.Active(), p.restartTicker.Load() != nil)
		if reason != "steady" || actual > rate*1.02 {
			t.Fatalf("steady auto cap escaped: actual %.0f/s reported %.0f/s (%s)", actual, rate, reason)
		}
	})
}

// The default population scales demand and capacity together for race runs.
// Set AUTO_CAP_SIM_METRICS=1000000 for the full restart experiment.
func TestAutoUpdateRestartSimulation(t *testing.T) {
	t.Parallel()
	metrics := 5000
	if v := os.Getenv("AUTO_CAP_SIM_METRICS"); v != "" {
		var err error
		metrics, err = strconv.Atoi(v)
		if err != nil || metrics < 1000 {
			t.Fatal("invalid AUTO_CAP_SIM_METRICS")
		}
	}
	const delay = 960 * time.Second
	type result struct {
		drain time.Duration
		cv    float64
	}
	results := map[string]result{}
	for _, mode := range []string{"fixed", "auto", "auto_floor_before", "auto_floor"} {
		auto := mode != "fixed"
		floorCase := mode == "auto_floor" || mode == "auto_floor_before"
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := cache.New()
				c.SetMaxSize(0)
				c.EnableInputLog()
				c.SetWriteoutRestoreSpread(!floorCase)
				c.SetWriteoutSparseSpread(true)
				c.SetWriteoutPacing(auto)
				if err := c.SetWriteoutBatching(960, delay); err != nil {
					t.Fatal(err)
				}
				if err := c.SetWriteoutSpread(.25); err != nil {
					t.Fatal(err)
				}
				if err := c.SetWriteStrategy("max"); err != nil {
					t.Fatal(err)
				}
				var names [60][][]byte
				start := time.Now()
				remaining := make(map[string]bool)
				var drain time.Duration
				for i := 0; i < metrics; i++ {
					name := []byte(fmt.Sprintf("restart.metric.%08d", i))
					names[i%60] = append(names[i%60], name)
					remaining[string(name)] = true
					c.AddRestored(points.OnePoint(string(name), 1, start.Unix()))
				}
				abort, request, next := make(chan bool), make(chan struct{}, 1), make(chan string, 1)
				done := make(chan struct{})
				defer func() { close(abort); <-done }()
				go func() {
					defer close(done)
					for {
						select {
						case <-abort:
							return
						case <-request:
							name := c.WriteoutQueue().Get(abort)
							if name == "" {
								return
							}
							next <- name
						}
					}
				}()
				controller := autoUpdateController{}
				p := &Whisper{}
				p.SetMaxUpdatesPerSecond(-1)
				ceiling := int(math.Ceil(26500 * float64(metrics) / 11500000))
				p.SetAutoUpdateRate(c.WriteoutState, 1, ceiling)
				if mode == "auto_floor" {
					budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
					budget.SetSteadyRate(37900 * float64(metrics) / 11500000)
					budget.Warmed()
					p.SetRestartBudget(budget, nil)
				}
				defer p.stopAutoUpdateRate()
				pending := false
				var bins [120]float64
				var eligible [120]int64
				var maxAge time.Duration
				rate := int(math.Ceil(1.6 * float64(metrics) / delay.Seconds()))
				for second := 0; second < 7200; second++ {
					if second%60 == 0 {
						for shard := range cache.ShardCount {
							c.TakeLogEntries(shard, uint64(second+1), nil)
						}
					}
					for _, name := range names[second%60] {
						c.AddPoint(name, 1, time.Now().Unix())
					}
					if auto && second%5 == 0 {
						if floorCase {
							p.sampleAutoUpdateRate()
							rate = p.autoUpdate.applied
						} else {
							rate = controller.step(time.Now(), c.WriteoutState(), 1, int(math.Ceil(23000*float64(metrics)/11500000)))
						}
					}
					if !pending {
						request <- struct{}{}
						pending = true
					}
					synctest.Wait()
					write := func(name string) {
						p, ok := c.PopNotConfirmed(name)
						if !ok {
							t.Fatal("queued absent metric")
						}
						for _, point := range p.Data {
							maxAge = max(maxAge, time.Since(time.Unix(point.Timestamp, 0)))
						}
						c.Confirm(p)
						delete(remaining, name)
						if len(remaining) == 0 && drain == 0 {
							drain = time.Since(start)
						}
						bins[second/60]++
					}
					select {
					case name := <-next:
						pending = false
						write(name)
						for i := 1; i < rate; i++ {
							name := c.WriteoutQueue().TryGet()
							if name == "" {
								break
							}
							write(name)
						}
					default:
					}
					if second%60 == 59 {
						eligible[second/60] = c.WriteoutState().Eligible
					}
					time.Sleep(time.Second)
				}
				maxAge = max(maxAge, c.WriteoutState().OldestAge)
				var cvs, means [8]float64
				for window := range cvs {
					var sum, squares float64
					for _, n := range bins[window*15 : (window+1)*15] {
						sum += n
						squares += n * n
					}
					mean := sum / 15
					means[window] = mean / 60
					cvs[window] = math.Sqrt(max(0, squares/15-mean*mean)) / mean
				}
				results[mode] = result{drain: drain, cv: cvs[7]}
				t.Logf("restored drain=%s", drain)
				t.Logf("metrics=%d cv_15m=%0.4f mean_updates_per_second=%0.2f max_point_age=%s", metrics, cvs, means, maxAge)
				trough := 30
				for i := 31; i < len(bins); i++ {
					if bins[i] < bins[trough] {
						trough = i
					}
				}
				t.Logf("lowest minute after 30m: minute=%d updates=%.0f eligible_at_end=%d", trough, bins[trough], eligible[trough])
				if auto && os.Getenv("AUTO_CAP_SIM_BASELINE") == "" {
					for w, cv := range cvs {
						need := float64(metrics) / delay.Seconds()
						if w >= 2 && (means[w] < need*.95 || means[w] > need*1.10) {
							t.Errorf("window %d-%d min rate=%g need=%g", w*15, (w+1)*15, means[w], need)
						}
						if w >= 2 && cv > .15 {
							t.Errorf("window %d-%d min cv=%g exceeds .15", w*15, (w+1)*15, cv)
						}
					}
					if maxAge > delay+autoUpdateSlack(delay) {
						t.Errorf("max point age %s exceeds delay + slack", maxAge)
					}
				}
			})
		})
	}
	before, after := results["auto_floor_before"], results["auto_floor"]
	t.Logf("automatic ceiling below floor: drain %s -> %s; steady cv %.4f -> %.4f", before.drain, after.drain, before.cv, after.cv)
	if after.drain == 0 || after.drain >= before.drain*85/100 {
		t.Fatal("restart floor did not shorten drain", before, after)
	}
	if math.Abs(before.cv-after.cv) > .02 {
		t.Fatal("temporary floor changed steady variability", before, after)
	}

}

func TestAutoUpdateRecoveryLimiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var written atomic.Uint64
		p := &Whisper{recv: func(chan bool) string { return "recovery.metric" }}
		p.SetWorkers(96)
		p.SetMaxUpdatesPerSecond(-1)
		p.SetRecovering(func() bool { return true })
		state := steadyUpdateState()
		state.Metrics = 96000
		p.SetAutoUpdateRate(func() cache.WriteoutState { return state }, 1, 0)
		p.SetMockStore(func() (StoreFunc, func()) { return func(string) { written.Add(1); time.Sleep(time.Millisecond) }, nil })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		rate, _, reason := p.autoUpdateObservation()
		if n := written.Load(); float64(n) > rate*10*1.02 {
			t.Fatalf("pending recovery bypassed %.0f/s cap (%s): %d writes", rate, reason, n)
		}
	})
}

type countedUpdateStore struct {
	updates, batches atomic.Uint64
	fallback         bool
}

func (s *countedUpdateStore) Metadata(context.Context, string) (store.Metadata, error) {
	return store.Metadata{}, nil
}
func (s *countedUpdateStore) Create(context.Context, store.MetricConfig) (store.Metadata, error) {
	return store.Metadata{}, nil
}
func (s *countedUpdateStore) UpdateMany(context.Context, string, []points.Point) error {
	s.updates.Add(1)
	return nil
}
func (s *countedUpdateStore) UpdateBatch(_ context.Context, updates []store.MetricUpdate, _ *store.ContinuationMaintenance) error {
	s.batches.Add(1)
	if s.fallback {
		return store.ErrFormat
	}
	s.updates.Add(uint64(len(updates)))
	return nil
}

func TestAutoUpdateSharedBatchAccounting(t *testing.T) {
	for _, workers := range []int{1, 96} {
		for _, fallback := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/fallback=%v", workers, fallback), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var sequence atomic.Uint64
					next := func() string { return fmt.Sprintf("batch.metric.%d", sequence.Add(1)) }
					db := &countedUpdateStore{fallback: fallback}
					p := NewWhisper("", nil, nil, func(chan bool) string { return next() }, func(name string) (*points.Points, bool) { return points.OnePoint(name, 1, time.Now().Unix()), true }, nil, nil)
					p.SetMetricStore(db)
					p.SetRequeue(func(*points.Points) {})
					p.SetWriteoutBatchReceiver(next)
					p.SetStoreUpdateBatchSize(8)
					p.SetWorkers(workers)
					p.SetMaxUpdatesPerSecond(-1)
					p.SetRecovering(func() bool { return true })
					state := steadyUpdateState()
					state.Metrics = 1_200_000
					p.SetAutoUpdateRate(func() cache.WriteoutState { return state }, 1, 0)
					if err := p.Start(); err != nil {
						t.Fatal(err)
					}
					defer p.Stop()
					time.Sleep(20 * time.Second)
					synctest.Wait()
					p.Stop()
					rate, _, _ := p.autoUpdateObservation()
					actual := db.updates.Load()
					if float64(actual) > rate*20*1.01 || float64(actual) < rate*20*.9 {
						t.Fatalf("rate %.0f/s, %d updates in 20s", rate, actual)
					}
					if n := atomic.LoadUint32(&p.updateOperations); uint64(n) != actual {
						t.Fatalf("%d members counted as %d updates", actual, n)
					}
					if workers == 1 && db.batches.Load() == 0 {
						t.Fatal("shared batch path was not exercised")
					}
				})
			})
		}
	}
}
