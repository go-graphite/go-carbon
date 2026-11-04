package carbon

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
)

func TestAutoUpdateRateConfig(t *testing.T) {
	for _, tc := range []struct {
		mode, floor, ceiling int
		delay                time.Duration
		valid                bool
	}{
		{-1, 1, 0, time.Minute, true}, {-1, 10, 100, time.Minute, true},
		{23000, 1, 0, 0, true}, {0, 1, 0, 0, true},
		{-2, 1, 0, time.Minute, false}, {-1, 1, 0, 0, false},
		{-1, 0, 0, time.Minute, false}, {-1, 10, 9, time.Minute, false}, {-1, 1, -1, time.Minute, false},
	} {
		cfg := NewConfig()
		cfg.Whisper.MaxUpdatesPerSecond = tc.mode
		cfg.Whisper.MaxUpdatesPerSecondAutoMin, cfg.Whisper.MaxUpdatesPerSecondAutoMax = tc.floor, tc.ceiling
		if tc.delay > 0 {
			cfg.Cache.WriteoutMinPoints = 960
		}
		cfg.Cache.WriteoutMaxDelay = Duration{tc.delay}
		if err := validateCacheConfig(cfg); (err == nil) != tc.valid {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}

func TestAutoUpdateAppReloadThroughRestartEnd(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, initial := range []int{50, -1} {
		t.Run(strconv.Itoa(initial), func(t *testing.T) {
			path, cfg := sharedAppConfig(t)
			cfg.Carbonserver.Enabled, cfg.Buckyd.Enabled = false, false
			cfg.Common.MetricInterval = &Duration{time.Hour}
			cfg.Cache.MaxSize = 0
			cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 960, Duration{1800 * time.Second}
			cfg.Whisper.MaxUpdatesPerSecond = initial
			cfg.Whisper.MaxUpdatesPerSecondAutoMin, cfg.Whisper.MaxUpdatesPerSecondAutoMax = 27, 27
			cfg.Whisper.Workers = 4
			cfg.Whisper.StoreUpdateBatchSize = 8
			cfg.Whisper.StoreMetadataCacheSize = 1 << 20
			cfg.Whisper.StoreMetadataCachePrewarm = true
			writeSharedConfig(t, path, cfg)
			synctest.Test(t, func(t *testing.T) {
				budget := restartcpu.NewWithCPU(20.46, func() float64 { return 0 })
				budget.SetSteadyRate(31.15)
				budget.Warmed()
				var drained atomic.Bool
				budget.SetDrain(drained.Load)
				time.Sleep(31 * time.Minute)
				app := New(path)
				if err := app.ParseConfig(); err != nil {
					t.Fatal(err)
				}
				app.restartBudget = budget
				app.Cache = cache.New()
				app.Cache.SetMaxSize(0)
				if err := app.Cache.SetWriteoutBatching(960, 1800*time.Second); err != nil {
					t.Fatal(err)
				}
				app.Cache.SetWriteoutPacing(initial == -1)
				var err error
				app.MetricStore, err = store.Open(cfg.Whisper.StoreDir, store.Options{
					CacheSize: 1 << 20, MemTableSize: 1 << 20, MetadataCacheSize: 1 << 20,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer app.Stop()
				app.metadataPrewarmReady, app.metadataPrewarmGate = make(chan struct{}), make(chan struct{})
				close(app.metadataPrewarmReady)
				close(app.metadataPrewarmGate)
				data := make([]points.Point, 960)
				for i := range data {
					data[i] = points.Point{Value: 1, Timestamp: time.Now().Unix()}
				}
				// Keep an actual pending source attached until the transition.
				// Its oldest metric is written first; the source remains visible
				// to Backlogged/Recovering until FinishPendingRecovery retires it.
				bundle := autoUpdatePendingSource(t, cfg.Whisper.DataDir, time.Now().Unix()-1)
				t.Cleanup(func() { _ = bundle.Close() })
				if err := app.Cache.AttachPendingRecoveryWithPreparedNames(bundle); err != nil {
					t.Fatal(err)
				}
				for i := range 1000 {
					name := "app.reload." + strconv.Itoa(i)
					if _, err := app.MetricStore.Create(context.Background(), store.MetricConfig{
						Name: name, Retentions: []store.Retention{{Step: 1, Count: 120}}, AggregationMethod: store.Average,
					}); err != nil {
						t.Fatal(err)
					}
					app.Cache.Add(&points.Points{Metric: name, Data: data})
				}
				if err := app.startPersister(); err != nil {
					t.Fatal(err)
				}
				check := func(stage string, want float64) {
					t.Helper()
					time.Sleep(time.Second)
					synctest.Wait()
					before := budget.Updates()
					time.Sleep(2 * time.Second)
					synctest.Wait()
					rate := float64(budget.Updates()-before) / 2
					t.Logf("%s: active=%v actual=%.0f/s", stage, budget.Active(), rate)
					if rate < want*.95 || rate > want*1.05 {
						t.Errorf("%s: actual=%g want=%g", stage, rate, want)
					}
				}
				if initial != -1 {
					check("fixed restart", 94)
					cfg.Whisper.MaxUpdatesPerSecond = -1
					writeSharedConfig(t, path, cfg)
					if err := app.ReloadConfig(); err != nil {
						t.Fatal(err)
					}
				}
				check("auto restart", 32)
				if err := app.Cache.FinishPendingRecovery(); err != nil {
					t.Fatal(err)
				}
				drained.Store(true)
				check("auto restart ended", 27)
				if budget.Active() {
					t.Fatal("restart pacing did not end")
				}
				check("auto sampler after restart", 27)
				cfg.Whisper.MaxUpdatesPerSecond = 50
				writeSharedConfig(t, path, cfg)
				if err := app.ReloadConfig(); err != nil {
					t.Fatal(err)
				}
				check("fixed after restart", 50)
			})
		})
	}
}

func autoUpdatePendingSource(t *testing.T, root string, stamp int64) *recovery.Bundle {
	t.Helper()
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	builder := recovery.NewBuilder(func(string) bool { return true })
	dump, err := recovery.NewWriter(filepath.Join(dir, "cache.1.2.bin"), 0, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err := dump.WritePoints(points.OnePoint("app.reload.0", 1, stamp)); err != nil {
		t.Fatal(err)
	}
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	wal, err := recovery.NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	inputFile, err := wal.Close()
	if err != nil {
		t.Fatal(err)
	}
	indexFile, err := recovery.WriteIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Publish(dir, root, cacheFile, inputFile, indexFile, "auto-reload-index"); err != nil {
		t.Fatal(err)
	}
	bundle, err := recovery.OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestAutoUpdateRateReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path := testConfig(t, t.TempDir())
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tcp.Enabled, cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Carbonserver.Enabled, cfg.Buckyd.Enabled = false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 960, Duration{time.Hour}
	cfg.Cache.WriteoutSpread = .25
	cfg.Whisper.MaxUpdatesPerSecond = -1
	cfg.Whisper.MaxUpdatesPerSecondAutoMin = 7
	cfg.Whisper.MaxUpdatesPerSecondAutoMax = 13
	writeBatchingConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	core := app.Cache
	core.Add(points.OnePoint("reload.auto.metric", 1, 1))
	for _, mode := range []int{-1, 42, -1, 0} {
		cfg.Whisper.MaxUpdatesPerSecond = mode
		cfg.Whisper.MaxUpdatesPerSecondAutoMin++
		writeBatchingConfig(t, path, cfg)
		previous := app.Persister
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.Cache != core || app.Persister != previous {
			t.Fatal("reload replaced cache or persister")
		}
		stats := make(map[string]float64)
		app.Persister.Stat(func(name string, value float64) { stats[name] = value })
		if stats["maxUpdatesPerSecond"] != float64(mode) {
			t.Fatal("mode did not reload")
		}
		if mode == -1 && stats["autoUpdateRate"] != float64(cfg.Whisper.MaxUpdatesPerSecondAutoMin) {
			t.Fatalf("auto floor did not reload: %v", stats)
		}
		wantPeriod := time.Hour
		if mode != -1 {
			wantPeriod = time.Hour * 7 / 8
		}
		if state := core.WriteoutState(); state.MeanDelay != wantPeriod {
			t.Fatalf("mode %d retained wrong eligibility schedule: %+v", mode, state)
		}
		if got := core.Get("reload.auto.metric"); len(got) != 1 {
			t.Fatal("reload lost a deferred point")
		}
	}
}
