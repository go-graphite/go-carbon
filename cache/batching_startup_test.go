package cache

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func TestWriteoutStartupSpread(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := batchingCache(t, 960, 960*time.Second)
		c.SetMaxSize(0)
		c.SetWriteoutStartupSpread(true)
		if err := c.SetWriteoutSpread(.25); err != nil {
			t.Fatal(err)
		}
		// Startup work before the first batch must not consume its phase.
		time.Sleep(time.Hour)
		start := time.Now()
		name := sparseName(t, "startup", .5)
		c.AddPoint([]byte(name), 1, 1)
		d := deadlineOf(c, name)
		if d.Before(start.Add(360*time.Second)) || !d.Before(start.Add(960*time.Second)) {
			t.Fatalf("first deadline %v outside startup window", d.Sub(start))
		}
		for i := range 960 {
			c.Add(points.OnePoint(name, 1, int64(i+2)))
		}
		if c.WriteoutReady(name) || !deadlineOf(c, name).Equal(d) {
			t.Fatal("point threshold bypassed the startup phase")
		}
		q, next := c.makeQueueAt(start)
		if q != nil || !next.Equal(d) {
			t.Fatalf("queue disagrees with startup phase: %v %v", q, next)
		}
		abort := make(chan bool)
		defer close(abort)
		got := make(chan string, 1)
		go func() { got <- c.WriteoutQueue().Get(abort) }()
		synctest.Wait()
		time.Sleep(d.Sub(time.Now()) - time.Millisecond)
		select {
		case <-got:
			t.Fatal("queue returned before phase")
		default:
		}
		time.Sleep(time.Millisecond)
		queuedMetric(t, got, name)
		p, ok := c.PopNotConfirmed(name)
		if !ok {
			t.Fatal("missing batch")
		}
		c.Requeue(p)
		if !c.WriteoutReady(name) {
			t.Fatal("failed write delayed again")
		}
		writeOnce(t, c, name)
		c.AddPoint([]byte(name), 2, 2000)
		it, first := sparseItem(c, name)
		s := c.settings.Load().(*cacheSettings)
		if it.startup || !first.Equal(time.Now()) || !deadlineOf(c, name).Equal(first.Add(s.delayFor(it))) {
			t.Fatal("second batch received another shortened window")
		}
		for range s.minPointsFor(it) - 1 {
			c.AddPoint([]byte(name), 3, 2001)
		}
		if !c.WriteoutReady(name) {
			t.Fatal("second batch lost the ordinary point threshold")
		}
	})
}

func TestWriteoutStartupSpreadConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := batchingCache(t, 8, time.Hour)
		c.SetWriteoutStartupSpread(true)
		var writers sync.WaitGroup
		for worker := range 4 {
			writers.Go(func() {
				for i := range 100 {
					name := fmt.Sprintf("startup.concurrent.%d.%d", worker, i)
					c.Add(&points.Points{Metric: name, Data: make([]points.Point, 8)})
				}
			})
		}
		writers.Wait()
		epoch := c.startupAt.Load()
		if epoch == nil {
			t.Fatal("no startup epoch")
		}
		for _, shard := range c.data {
			for _, it := range shard.items.all {
				d := writeoutDeadline(it, c.settings.Load().(*cacheSettings))
				if d.Before(*epoch) || !d.Before(epoch.Add(time.Hour)) {
					t.Fatalf("%s outside shared startup window: %v", it.Metric, d)
				}
			}
		}
		// A first arrival that lost the epoch race keeps its delay bound.
		now := time.Now()
		later := now.Add(time.Second)
		c.startupAt.Store(&later)
		it := newItem("startup.earlier", 0)
		it.firstArrival = now
		c.spreadStartup(it, c.settings.Load().(*cacheSettings), now)
		if it.startup || !it.firstArrival.Equal(now) {
			t.Fatal("arrival older than the startup epoch changed its window")
		}
	})
}

func TestWriteoutStartupSpreadBypasses(t *testing.T) {
	for _, mode := range []string{"disabled", "batching off", "pressure", "unsafe dump", "replay", "late arrival", "disable"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := batchingCache(t, 8, time.Hour)
				c.SetWriteoutStartupSpread(mode != "disabled")
				name := sparseName(t, "startup.bypass", .5)
				switch mode {
				case "pressure":
					c.SetMaxSize(1)
				case "batching off":
					if err := c.SetWriteoutBatching(0, 0); err != nil {
						t.Fatal(err)
					}
				case "late arrival":
					c.AddPoint([]byte("epoch"), 1, 1)
					time.Sleep(2 * time.Hour)
				}
				data := &points.Points{Metric: name, Data: make([]points.Point, 8)}
				switch mode {
				case "unsafe dump":
					c.AddRestored(data)
				case "replay":
					c.AddRecovered(data)
				default:
					c.Add(data)
				}
				if mode == "disable" {
					c.SetWriteoutStartupSpread(false)
				}
				if !c.WriteoutReady(name) {
					t.Fatal("startup spread suppressed a batching bypass")
				}
				if mode == "replay" && c.startupAt.Load() != nil {
					t.Fatal("urgent replay started the live startup epoch")
				}
			})
		})
	}
}

func TestWriteoutStartupSpreadDurableRestore(t *testing.T) {
	c := restoreSpreadCache(t, true)
	c.SetWriteoutStartupSpread(true)
	dir := t.TempDir()
	opts := inputlog.Options{}
	l, _ := startInputLog(t, dir, c, opts)
	defer func() { _ = l.Close() }()
	name := sparseName(t, "startup.dump", .5)
	data := &points.Points{Metric: name, Data: make([]points.Point, 1100)}
	c.AddRestored(data)
	d := deadlineOf(c, name)
	if d.IsZero() || c.WriteoutReady(name) {
		t.Fatal("full restored batch bypassed its startup phase")
	}
	c.AddPoints([]byte(name), []points.Point{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}})
	if !deadlineOf(c, name).Equal(d) || len(c.Get(name)) != 1102 {
		t.Fatal("live points did not join the restored window")
	}
	syncLog(t, l)
	want := c.Get(name)
	l.Kill()
	restarted := restoreSpreadCache(t, true)
	restarted.SetWriteoutStartupSpread(true)
	l, _ = startInputLog(t, dir, restarted, opts)
	if got := restarted.Get(name); !reflect.DeepEqual(got, want) {
		t.Fatal("crash before startup deadline lost restored or live points")
	}
	if restarted.startupAt.Load() != nil {
		t.Fatal("input-log replay consumed the live startup phase")
	}
	writeOnce(t, restarted, name)
	restarted.AddPoint([]byte(name), 5, 5)
	if it, _ := sparseItem(restarted, name); !it.startup {
		t.Fatal("first live batch after replay was not phased")
	}
}

func TestWriteoutStartupSpreadPending(t *testing.T) {
	c := restoreSpreadCache(t, true)
	c.SetWriteoutStartupSpread(true)
	if err := c.AttachPendingRecoveryWithPreparedNames(pendingFixtureKnown(t)); err != nil {
		t.Fatal(err)
	}
	if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
		t.Fatal(err)
	}
	for range 1100 {
		c.AddPoint([]byte("metric"), 4, 4)
	}
	if c.WriteoutReady("metric") || !c.WriteoutReady("other") {
		t.Fatal("known or new saved metric lost its recovery policy")
	}
	if c.PendingUrgentOutstanding() != 1 || c.PendingOutstanding() != 2 {
		t.Fatal("startup changed recovery accounting")
	}
	persist(c, []string{"other"})
	if err := c.FinishPendingRecovery(); err == nil {
		t.Fatal("retired a source before the phased batch was persisted")
	}
	persist(c, []string{"metric"})
	if err := c.FinishPendingRecovery(); err != nil {
		t.Fatal(err)
	}
}

// Replayed input can claim older checkpoint history before live input opens.
// That urgent merged batch must not consume the live startup epoch either.
func TestWriteoutStartupSpreadReplayClaims(t *testing.T) {
	c := restoreSpreadCache(t, true)
	c.SetWriteoutStartupSpread(true)
	if err := c.AttachPendingRecoveryWithPreparedNames(pendingFixtureKnown(t)); err != nil {
		t.Fatal(err)
	}
	c.AddRecovered(points.OnePoint("metric", 9, 9))
	if c.startupAt.Load() != nil || !c.WriteoutReady("metric") {
		t.Fatal("urgent checkpoint claim consumed the live startup phase")
	}
	writeOnce(t, c, "metric")
	c.AddPoint([]byte("metric"), 10, 10)
	if it, _ := sparseItem(c, "metric"); !it.startup {
		t.Fatal("first live batch after a replay claim was not phased")
	}
}

// Model arrivals arithmetically and visit only batch boundaries. The clock is
// virtual and the production deadline functions decide eligibility; no point
// buffers, sleeping, per-second full-cache scans or background workers needed.
func TestWriteoutStartupSpreadSimulation(t *testing.T) {
	const metrics, cycle, horizon, interval = 100000, 960, 5 * 960, 180
	type result struct {
		writes, persisted [27]float64
		perCycle          [5]int
		cache             [horizon + 1]int64
		peak              int64
		metricWrites      [metrics]uint8
		steadyWrites      float64
	}
	simulate := func(t *testing.T, startup, restored bool) result {
		t.Helper()
		var r result
		var inputs [4][60]int64
		c := &Cache{}
		epoch := time.Unix(1_790_000_000, 0)
		c.startupAt.Store(&epoch)
		s := &cacheSettings{writeoutMinPoints: cycle, writeoutMaxDelay: cycle * time.Second,
			writeoutSpread: .25, writeoutStartupSpread: startup, writeoutSparseSpread: true}
		for i := range metrics {
			it := newItem(fmt.Sprintf("db.host-%d.metric_%d", i%97, i), 0)
			gap, burst := [...]int{1, 10, 60, 60}[i%4], 1
			if i%4 == 3 {
				burst = 6 // synchronized short bursts, average 1/10s
			}
			period := min(((s.minPointsFor(it)+burst-1)/burst)*gap, (int(s.delayFor(it)/time.Second)/gap+1)*gap)
			r.steadyWrites += float64(cycle) / float64(period)
			phase := i % 60
			inputs[i%4][phase]++
			first, saved := phase, 0
			if restored {
				// Half a cycle of pending history, followed by live input.
				saved = cycle / (2 * gap) * burst
				first = 0
				r.cache[0] += int64(saved)
			}
			startupWindows := 0
			for first < horizon {
				it.compact.n, it.startup = saved, false
				now := epoch.Add(time.Duration(first) * time.Second)
				it.firstArrival = now
				if saved > 0 {
					it.firstArrival = s.restoreArrival(it, now)
				}
				c.spreadStartup(it, s, now)
				if it.startup {
					startupWindows++
					if startupWindows > 1 {
						t.Fatal("metric received recurring shortened windows")
					}
				}
				due := writeoutDeadline(it, s)
				end := due.Sub(epoch).Seconds()
				nextArrival := phase
				if first > phase {
					nextArrival += (first - phase + gap - 1) / gap * gap
				}
				need := (max(0, s.minPointsFor(it)-saved) + burst - 1) / burst
				thresholdAt := float64(nextArrival + (need-1)*gap)
				if need == 0 {
					thresholdAt = float64(first)
				}
				if due.IsZero() {
					end = float64(first)
				} else if !it.startup {
					end = min(end, thresholdAt)
				}
				last := nextArrival - gap
				if end >= float64(nextArrival) {
					last = nextArrival + int((end-float64(nextArrival))/float64(gap))*gap
				}
				n := saved + (last-nextArrival+gap)/gap*burst
				if end >= horizon {
					break
				}
				it.compact.n = n
				ready := writeoutDeadline(it, s)
				at := epoch.Add(time.Duration(math.Ceil(end * float64(time.Second))))
				if !ready.IsZero() && at.Before(ready) {
					t.Fatalf("simulation wrote %s before its deadline", it.Metric)
				}
				r.cache[int(math.Ceil(end))] -= int64(n)
				bucket := int(end) / interval
				r.writes[bucket]++
				r.persisted[bucket] += float64(n)
				r.perCycle[int(end)/cycle]++
				r.metricWrites[i]++
				first, saved = last+gap, 0
			}
		}
		var cached int64
		for sec := range r.cache {
			for group, phases := range inputs {
				gap, burst := [...]int{1, 10, 60, 60}[group], int64(1)
				if group == 3 {
					burst = 6
				}
				for phase, count := range phases {
					if sec >= phase && (sec-phase)%gap == 0 {
						cached += count * burst
					}
				}
			}
			cached += r.cache[sec]
			r.cache[sec] = cached
			r.peak = max(r.peak, cached)
			if cached < 0 {
				t.Fatal("simulation persisted points that had not arrived")
			}
		}
		return r
	}
	stats := func(bins [27]float64, from int) (cv, low, high float64) {
		var sum, square float64
		low = math.Inf(1)
		for i := from; i < len(bins); i++ {
			rate := bins[i] / float64(min(interval, horizon-i*interval))
			sum += rate
			square += rate * rate
			low, high = min(low, rate), max(high, rate)
		}
		mean := sum / float64(len(bins)-from)
		return math.Sqrt(max(0, square/float64(len(bins)-from)-mean*mean)) / mean, low / mean, high / mean
	}
	for _, restored := range []bool{false, true} {
		t.Run(fmt.Sprintf("restored=%t", restored), func(t *testing.T) {
			before, after := simulate(t, false, restored), simulate(t, true, restored)
			bcv, _, _ := stats(before.writes, 0)
			acv, low, high := stats(after.writes, 0)
			bp, _, _ := stats(before.persisted, 0)
			ap, initialLow, initialHigh := stats(after.persisted, 0)
			_, plow, phigh := stats(after.persisted, 6) // after the empty-cache ramp
			t.Logf("100000 metrics, 180s intervals, five 960s cycles: write CV %.3f -> %.3f, points CV %.3f -> %.3f; writes min/max %.3f/%.3f mean; points after 1080s %.3f/%.3f mean; cache peak %d -> %d points",
				bcv, acv, bp, ap, low, high, plow, phigh, before.peak, after.peak)
			t.Logf("points including startup min/max %.3f/%.3f mean", initialLow, initialHigh)
			var cacheLow, cacheHigh, cacheSum float64
			cacheLow = math.Inf(1)
			for _, n := range after.cache[1080:] {
				cacheLow, cacheHigh = min(cacheLow, float64(n)), max(cacheHigh, float64(n))
				cacheSum += float64(n)
			}
			cacheMean := cacheSum / float64(len(after.cache)-1080)
			t.Logf("cache after 1080s min/max %.3f/%.3f mean; steady writes/metric/cycle %.4f -> %.4f",
				cacheLow/cacheMean, cacheHigh/cacheMean, before.steadyWrites/metrics, after.steadyWrites/metrics)
			if cacheLow/cacheMean < .8 || cacheHigh/cacheMean > 1.2 {
				t.Fatal("cache occupancy did not settle")
			}
			extra := uint8(1)
			if restored {
				// The old restore window spans max delay, which can exceed
				// one ordinary period: advancing it crosses two boundaries.
				extra++
			}
			for i, n := range after.metricWrites {
				if n > before.metricWrites[i]+extra {
					t.Fatalf("metric %d gained recurring writes: %d -> %d", i, before.metricWrites[i], n)
				}
			}
			var bw, aw int
			for i := range after.perCycle {
				bw += before.perCycle[i]
				aw += after.perCycle[i]
				t.Logf("cycle %d writes/metric %.4f -> %.4f", i+1, float64(before.perCycle[i])/metrics, float64(after.perCycle[i])/metrics)
			}
			if acv > .10 || acv > bcv*.5 || low < .8 || high > 1.2 {
				t.Fatalf("startup writes are not flat: CV %.3f, min/max %.3f/%.3f", acv, low, high)
			}
			if plow < .8 || phigh > 1.2 || after.peak >= before.peak {
				t.Fatal("points or cache peak did not settle")
			}
			// Advancing the first write can add one partial batch over this
			// finite horizon, but must not add a recurring write per cycle.
			if aw > bw+metrics || float64(aw)/metrics/5 > 1.2 {
				t.Fatal("startup spreading added recurring writes")
			}
		})
	}
	t.Logf("item size with startup flag: %d bytes", reflect.TypeFor[item]().Size())
}

// Compare append cost with startup phasing on/off. Forced pops bound memory;
// scheduling itself is checked by the simulation and queue tests above.
func BenchmarkWriteoutStartupSpreadAddPoint(b *testing.B) {
	for _, on := range []bool{false, true} {
		b.Run(fmt.Sprintf("startup=%t", on), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
				b.Fatal(err)
			}
			if err := c.SetWriteoutSpread(.25); err != nil {
				b.Fatal(err)
			}
			c.SetWriteoutStartupSpread(on)
			name := []byte("startup.append.metric")
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.AddPoint(name, float64(i), int64(i))
				if i%960 == 959 {
					p, _ := c.PopNotConfirmed(string(name))
					c.Confirm(p)
				}
			}
		})
	}
}
