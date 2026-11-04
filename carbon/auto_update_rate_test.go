package carbon

import (
	"testing"
	"time"

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
		if app.Cache != core || app.Persister == previous {
			t.Fatal("reload lost cache or retained old persister")
		}
		stats := make(map[string]float64)
		app.Persister.Stat(func(name string, value float64) { stats[name] = value })
		if stats["maxUpdatesPerSecond"] != float64(mode) {
			t.Fatal("mode did not reload")
		}
		if mode == -1 && stats["autoUpdateRate"] != float64(cfg.Whisper.MaxUpdatesPerSecondAutoMin) {
			t.Fatalf("auto floor did not reload: %v", stats)
		}
		if got := core.Get("reload.auto.metric"); len(got) != 1 {
			t.Fatal("reload lost a deferred point")
		}
	}
}
