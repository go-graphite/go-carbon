package carbon

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

// One slow batch must not stop the remaining writer pool at SIGHUP. The same
// fixture reproduces the old Stop/start gap without needing a slow real disk.
func TestPersisterReloadWriteContinuity(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, modes := range [][2]int{{200, 200}, {200, -1}, {-1, 200}} {
		t.Run(fmt.Sprint(modes), func(t *testing.T) {
			path := testConfig(t, t.TempDir())
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Common.MetricInterval = &Duration{time.Hour}
			cfg.Whisper.Workers = 4
			cfg.Whisper.MaxUpdatesPerSecond = modes[0]
			cfg.Whisper.MaxUpdatesPerSecondAutoMin, cfg.Whisper.MaxUpdatesPerSecondAutoMax = 200, 200
			cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 1, Duration{time.Millisecond}
			writeBatchingConfig(t, path, cfg)
			synctest.Test(t, func(t *testing.T) {
				app := New(path)
				if err := app.ParseConfig(); err != nil {
					t.Fatal(err)
				}
				app.Cache = cache.New()
				app.Cache.SetMaxSize(0)
				defer app.Cache.Stop()
				if err := app.Cache.SetWriteoutBatching(1, time.Millisecond); err != nil {
					t.Fatal(err)
				}
				app.Cache.SetWriteoutPacing(modes[0] == -1)
				p := persister.NewWhisper(cfg.Whisper.DataDir, app.Config.Whisper.Schemas, app.Config.Whisper.Aggregation,
					app.Cache.WriteoutQueue().Get, app.Cache.PopNotConfirmed, app.Cache.Confirm, app.Cache.Pop)
				p.SetMaxUpdatesPerSecond(modes[0])
				p.SetAutoUpdateRate(app.Cache.WriteoutState, 200, 200)
				p.SetWorkers(4)
				var block atomic.Bool
				blocked, release := make(chan struct{}), make(chan struct{})
				var mu sync.Mutex
				var last time.Time
				var maxGap time.Duration
				var written int
				p.SetMockStore(func() (persister.StoreFunc, func()) {
					return func(name string) {
						if block.CompareAndSwap(true, false) {
							close(blocked)
							<-release
						}
						if batch, ok := app.Cache.PopNotConfirmed(name); ok {
							app.Cache.Confirm(batch)
							mu.Lock()
							if !last.IsZero() {
								maxGap = max(maxGap, time.Since(last))
							}
							last = time.Now()
							written++
							mu.Unlock()
						}
					}, nil
				})
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				app.Persister = p
				defer app.Stop()
				feedDone := make(chan struct{})
				go func() {
					defer close(feedDone)
					for i := range 500 {
						app.Cache.Add(points.OnePoint(fmt.Sprintf("reload.metric.%d", i), float64(i), time.Now().Unix()))
						time.Sleep(10 * time.Millisecond)
					}
				}()
				time.Sleep(500 * time.Millisecond)
				block.Store(true)
				<-blocked
				cfg.Whisper.MaxUpdatesPerSecond = modes[1]
				cfg.Whisper.Sparse = !cfg.Whisper.Sparse
				cfg.Whisper.StoreUpdateBatchSize = 8
				writeBatchingConfig(t, path, cfg)
				reloaded := make(chan error, 1)
				go func() { reloaded <- app.ReloadConfig() }()
				time.Sleep(2 * time.Second)
				close(release)
				if err := <-reloaded; err != nil {
					t.Fatal(err)
				}
				<-feedDone
				time.Sleep(200 * time.Millisecond)
				mu.Lock()
				defer mu.Unlock()
				t.Logf("modes=%v writes=%d maximum write gap=%v", modes, written, maxGap)
				if maxGap > time.Second {
					t.Errorf("reload paused store writes for %v", maxGap)
				}
				if app.Persister != p {
					t.Error("reload replaced running persister")
				}
				if written != 500 {
					t.Errorf("confirmed %d writes, want 500", written)
				}
			})
		})
	}
}

func TestPersisterWorkerReloadKeepsWriting(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	cfg.Carbonserver.Enabled, cfg.Buckyd.Enabled = false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Whisper.Workers, cfg.Whisper.MaxUpdatesPerSecond = 2, 200
	writeSharedConfig(t, path, cfg)
	app := startApp(t, path)
	defer app.Stop()
	feedDone, stopFeed := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(feedDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; ; i++ {
			select {
			case <-stopFeed:
				return
			case <-ticker.C:
				app.Cache.Add(points.OnePoint(fmt.Sprintf("reload.worker.%d", i%8), float64(i), time.Now().Unix()))
			}
		}
	}()
	defer func() { close(stopFeed); <-feedDone }()
	// Wait for observed writes rather than asserting a wall-clock write gap.
	// The virtual-time test above checks continuity with a blocked operation.
	previousUpdates := app.MetricStore.Stats().Operands
	waitForWrite := func() {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for {
			if now := app.MetricStore.Stats().Operands; now > previousUpdates {
				previousUpdates = now
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("persister made no write progress")
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitForWrite()
	previous := app.Persister
	cfg.Whisper.Workers = 5
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.Persister == previous {
		t.Fatal("worker count did not rebuild persister")
	}
	previousUpdates = app.MetricStore.Stats().Operands
	waitForWrite()
}
