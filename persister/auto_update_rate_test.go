package persister

import (
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/prometheus/client_golang/prometheus"
)

func steadyUpdateState() cache.WriteoutState {
	return cache.WriteoutState{Metrics: 1_000_000, Points: 8_000_000,
		MaxDelay: 960 * time.Second, MeanDelay: 840 * time.Second}
}

func TestAutoUpdateController(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := steadyUpdateState()
	want := float64(s.Metrics) / (s.MeanDelay + autoUpdateSlack(s.MaxDelay)/2).Seconds() * autoUpdateMargin
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
	t.Run("recovery bypass is immediate", func(t *testing.T) {
		var recovering atomic.Bool
		p := &Whisper{}
		p.SetMaxUpdatesPerSecond(-1)
		p.SetBacklogged(func() bool { return true })
		p.SetRecovering(recovering.Load)
		if !p.throttleUpdates() {
			t.Fatal("auto pressure escaped its ceiling")
		}
		recovering.Store(true)
		if p.throttleUpdates() {
			t.Fatal("recovery waited for an auto sample")
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
				if rate, _, reason := p.autoUpdateObservation(); rate != 3*predecessor || reason != "restart" {
					t.Fatalf("auto ceiling throttled restart: rate=%g reason=%s", rate, reason)
				}
				if predecessor == 0 && p.updateTokens() != p.updateTicker.C {
					t.Fatal("auto throttled unrestricted CPU allowance")
				}
				if predecessor > 0 && p.updateTokens() != p.restartTicker.Load().C {
					t.Fatal("auto replaced predecessor allowance")
				}
			})
		}
	})
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
