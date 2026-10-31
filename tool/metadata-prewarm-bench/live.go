//go:build darwin || linux

package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

// runLive drives sustained input through the real cache and persister, including
// the entire final drain. Every arrival has unique timestamps within its metric.
func runLive(db *store.Store, metrics, writers, rate, maxUpdates, batchPoints int, startOffset int64, duration, gate time.Duration, budget *restartcpu.Budget, warm func(), warmFirst bool) {
	if rate <= 0 || batchPoints <= 0 || duration < time.Minute {
		panic("live needs positive rate/points and at least one minute")
	}
	core := cache.New()
	defer core.Stop()
	core.SetMaxSize(0)
	check(core.SetWriteStrategy("noop"))
	check(core.SetWriteoutBatching(batchPoints, max(time.Second, time.Duration(float64(metrics)/float64(rate)*float64(time.Second)))))
	if !warmFirst {
		core.PauseStartupWriteout()
	}
	ready, done := make(chan struct{}), make(chan struct{})
	close(ready)
	p := persister.NewWhisper("", nil, nil, core.WriteoutQueue().Get, core.PopNotConfirmed, func(batch *points.Points) { core.Confirm(batch); completed.Add(1) }, core.Pop)
	p.SetMetricStore(db)
	p.SetWriteoutBatchReceiver(core.WriteoutQueue().TryGet)
	p.SetWriteoutReady(core.WriteoutReady)
	p.SetRequeue(core.Requeue)
	p.SetWorkers(writers)
	if maxUpdates <= 0 {
		maxUpdates = rate
	}
	p.SetMaxUpdatesPerSecond(maxUpdates)
	if budget.Cores() > 0 {
		if ordered, ok := any(core).(interface{ SetRestartOrder(func() bool) }); ok {
			ordered.SetRestartOrder(budget.Active)
		}
		if warmFirst {
			if ordered, ok := any(core).(interface {
				SetRestartWarmFirst(func(string) bool, func(<-chan bool) bool, func())
			}); ok {
				if hint, ok := any(db).(interface{ MetadataCached(string) bool }); ok {
					begin, end := budget.WaitWriteout, budget.EndWriteout
					if prefetch, ok := any(budget).(interface {
						WaitPrefetch(<-chan bool) bool
						EndPrefetch()
					}); ok {
						begin, end = prefetch.WaitPrefetch, prefetch.EndPrefetch
					}
					ordered.SetRestartWarmFirst(hint.MetadataCached, begin, end)
				}
			}
		}
		p.SetRestartBudget(budget, func() float64 { return min(float64(rate), float64(core.Len())) })
	}
	// Keep identical workload code usable against a baseline without these APIs.
	if enabled, ok := any(p).(interface{ SetWarmFirst(bool) }); ok {
		enabled.SetWarmFirst(warmFirst)
	}
	if seeded, ok := any(budget).(interface{ SetSteadyRate(float64) }); ok {
		seeded.SetSteadyRate(float64(rate))
	}
	if !warmFirst {
		if gate <= 0 {
			panic("live gated mode needs a positive -gate")
		}
		p.SetStartupGate(ready, done, gate, nil)
		p.SetStartupRelease(func(at time.Time) {
			core.ResumeStartupWriteout(at)
			emit(map[string]any{"mode": "live-gate-release", "at": at})
		})
	}
	check(p.Start())
	defer p.Stop()
	go func() {
		defer close(done)
		defer budget.Warmed()
		if warm != nil {
			warm()
		}
	}()
	started, used := time.Now(), cpu()
	var peak atomic.Int64
	stop, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				count := core.Size()
				peak.Store(max(peak.Load(), count))
				emit(map[string]any{"mode": "live-backlog", "second": time.Since(started).Seconds(), "points": count, "metrics": core.Len()})
			}
		}
	}()
	defer func() { close(stop); <-sampled }()
	var expected [16][]points.Point
	total := int(duration.Seconds() * float64(rate))
	const chunk = 140
	for offset := 0; offset < total; offset += chunk {
		due := started.Add(time.Duration(float64(offset) / float64(rate) * float64(time.Second)))
		if delay := time.Until(due); delay > 0 {
			time.Sleep(delay)
		}
		for j := offset; j < min(offset+chunk, total); j++ {
			metric := int(int64(j) * 999983 % int64(metrics))
			batch := make([]points.Point, batchPoints)
			for k := range batch {
				batch[k] = points.Point{Timestamp: base + startOffset + int64(j/rate+k), Value: float64(j%1000 + k)}
			}
			core.Add(&points.Points{Metric: name(metric), Data: batch})
			if metric < len(expected) {
				expected[metric] = append(expected[metric], batch...)
			}
		}
	}
	intakeWall := time.Since(started).Seconds()
	for !core.IsEmpty() {
		if time.Since(started) > duration+4*time.Minute {
			panic("live backlog did not drain")
		}
		time.Sleep(10 * time.Millisecond)
	}
	drainedAt := time.Now()
	select {
	case <-done:
	case <-time.After(4 * time.Minute):
		panic("warming did not finish after drain")
	}
	p.Stop()
	for i, want := range expected {
		snapshot, err := db.Snapshot(context.Background(), name(i))
		check(err)
		got := make(map[int64]float64)
		for _, point := range snapshot.Archives[0].Points {
			got[point.Timestamp] = point.Value
		}
		for _, point := range want {
			value, ok := got[point.Timestamp]
			if !ok || value != point.Value {
				panic("live output differs from generated input")
			}
		}
	}
	emit(map[string]any{"mode": "live-drained", "wall": time.Since(started).Seconds(), "cpu": cpu() - used, "intake_wall": intakeWall, "drain_seconds": drainedAt.Sub(started).Seconds() - intakeWall, "peak_points": peak.Load(), "arrivals": total, "input_points": int64(total) * int64(batchPoints), "updates": completed.Load(), "cache": db.Stats().StateCache})
}
