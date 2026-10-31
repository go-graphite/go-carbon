//go:build darwin || linux

package main

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

type backlogConfig struct {
	minPoints, updates      int
	spread                  float64
	startupSpread           bool
	stopAfter               time.Duration
	pendingDir              string
	savedPoints, gatePoints int
}

// runBacklog drives the real cache queue, gate and rate-limited persister.
// The legacy callback models the old unconditional pending-recovery rate lift;
// source ownership itself is covered by the recovery tests.
func runBacklog(db *store.Store, metrics, writers int, window, gate time.Duration, adaptive, legacy bool, config backlogConfig, progress *store.PrewarmProgress, budget *restartcpu.Budget, warm func()) {
	if window <= 0 || gate <= 0 {
		panic("backlog needs positive -window and -gate")
	}
	core := cache.New()
	defer core.Stop()
	core.SetMaxSize(0)
	core.SetWriteoutStartupSpread(config.startupSpread)
	core.SetWriteoutRestoreSpread(config.startupSpread)
	check(core.SetWriteoutSpread(config.spread))
	check(core.SetWriteoutBatching(config.minPoints, window))
	if !legacy {
		core.PauseStartupWriteout()
	}
	var pending *pendingBacklog
	if config.pendingDir != "" {
		core.SetMaxSize(10_000_000_000)
		core.SetWriteStrategy("noop")
		core.SetWriteoutRestoreSpread(true)
		pending = openPending(core, db, config, metrics, window)
		defer pending.close()
	}
	input := make([]points.Point, 30)
	for k := range input {
		input[k] = points.Point{Timestamp: base + 30 + int64(k), Value: float64(k)}
	}
	started := time.Now()
	if pending == nil {
		for i := range metrics {
			core.Add(&points.Points{Metric: name(int(int64(i) * 999983 % int64(metrics))), Data: input})
		}
	}
	emit(map[string]any{"mode": "backlog-held", "points": core.Size(), "estimated_bytes": core.EstimatedBytes(), "ingest_wall": time.Since(started).Seconds(), "legacy": legacy, "adaptive": adaptive})
	ready, done, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
	close(ready)
	var released atomic.Pointer[time.Time]
	var firstWrite atomic.Int64
	var seconds [300]atomic.Int64
	p := persister.NewWhisper("", nil, nil, core.WriteoutQueue().Get, core.PopNotConfirmed,
		func(batch *points.Points) {
			core.Confirm(batch)
			if at := released.Load(); at != nil {
				elapsed := time.Since(*at)
				firstWrite.CompareAndSwap(0, int64(elapsed)+1)
				if second := int(elapsed / time.Second); second < len(seconds) {
					seconds[second].Add(1)
				}
			}
			if completed.Add(1) == int64(metrics) {
				close(drained)
			}
		}, core.Pop)
	p.SetMetricStore(db)
	if budget.Cores() > 0 {
		p.SetRestartBudget(budget, func() float64 { return float64(core.Len()) / window.Seconds() })
	}
	p.SetWriteoutReady(core.WriteoutReady)
	p.SetRequeue(core.Requeue)
	p.SetBacklogged(func() bool { return legacy || core.Backlogged() })
	p.SetWorkers(writers)
	steady := float64(metrics) / window.Seconds()
	updates := config.updates
	if updates == 0 {
		updates = max(1, int(steady))
	}
	p.SetMaxUpdatesPerSecond(updates)
	p.SetStartupGate(ready, done, gate, nil)
	if adaptive {
		p.SetStartupProgress(progress.Snapshot)
		p.SetStartupCheckpointLoading(progress.CheckpointLoading)
	}
	p.SetStartupRelease(func(at time.Time) {
		released.Store(&at)
		if !legacy {
			core.ResumeStartupWriteout(at)
		}
		emit(map[string]any{"mode": "gate-release", "second": time.Since(started).Seconds(), "points": core.Size()})
	})
	check(p.Start())
	defer p.Stop()
	if pending != nil {
		pending.start(gate)
		previous := warm
		warm = func() {
			if previous != nil {
				previous()
			}
			<-pending.done
		}
	}
	go func() {
		defer close(done)
		defer budget.Warmed()
		if warm != nil {
			warm()
		}
	}()
	var stop <-chan time.Time
	if config.stopAfter > 0 {
		timer := time.NewTimer(config.stopAfter)
		defer timer.Stop()
		stop = timer.C
	}
	select {
	case <-drained:
	case <-stop:
		emit(map[string]any{"mode": "backlog-window-ended", "metrics": completed.Load(), "remaining_points": core.Size(), "wall": time.Since(started).Seconds()})
		return
	case <-time.After(280 * time.Second):
		panic(fmt.Sprintf("backlog did not drain: %d metrics, %d points remain", completed.Load(), core.Size()))
	}
	<-done
	if !core.IsEmpty() {
		panic("confirmed backlog still cached")
	}
	if pending != nil {
		pending.verify(db)
	} else {
		// Probe stored output against every input point, including timestamps.
		for i := range min(metrics, 16) {
			snapshot, err := db.Snapshot(context.Background(), name(i))
			check(err)
			found := 0
			for _, point := range snapshot.Archives[0].Points {
				if point.Timestamp >= base+30 && point.Timestamp < base+60 {
					if point.Value != float64(point.Timestamp-base-30) {
						panic("backlog value changed")
					}
					found++
				}
			}
			if found != len(input) {
				panic("backlog points lost")
			}
		}
	}
	emit(map[string]any{"mode": "backlog-drained", "metrics": completed.Load(), "wall": time.Since(started).Seconds(), "cache": db.Stats().StateCache})
	var perSecond [300]int64
	minutes := make([]int64, int(math.Ceil(time.Since(*released.Load()).Minutes())))
	for i := range seconds {
		perSecond[i] = seconds[i].Load()
		if i/60 < len(minutes) {
			minutes[i/60] += perSecond[i]
		}
	}
	emit(map[string]any{"mode": "backlog-drain-profile", "first_write": float64(firstWrite.Load()-1) / float64(time.Second),
		"steady_updates_per_second": steady, "max_updates_per_second": updates,
		"writes_per_minute": minutes, "writes_per_second": perSecond})
}
