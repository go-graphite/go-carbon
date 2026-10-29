package carbon

import (
	"os"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/points"
)

func writeBatchingConfig(t *testing.T, path string, cfg *Config) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = toml.NewEncoder(f).Encode(cfg)
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

func TestWriteoutBatchingConfig(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		min     int
		delay   time.Duration
		invalid bool
	}{
		{"disabled", 0, 0, false},
		{"bounded", 8, 2 * time.Second, false},
		{"negative points", -1, time.Second, true},
		{"negative delay", 8, -time.Second, true},
		{"unbounded", 8, 0, true},
		{"missing threshold", 0, time.Second, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := testConfig(t, t.TempDir())
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = tt.min, Duration{tt.delay}
			writeBatchingConfig(t, path, cfg)
			app := New(path)
			err = app.ParseConfig()
			if (err != nil) != tt.invalid {
				t.Fatalf("ParseConfig=%v; invalid=%v", err, tt.invalid)
			}
			if err == nil && (app.Config.Cache.WriteoutMinPoints != tt.min || app.Config.Cache.WriteoutMaxDelay.Value() != tt.delay) {
				t.Fatal("batching config changed in round trip")
			}
		})
	}
}

func TestWriteoutBatchingReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path := testConfig(t, t.TempDir())
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.Enabled = false
	cfg.Tcp.Enabled, cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 8, Duration{time.Hour}
	writeBatchingConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	app.Cache.Add(points.OnePoint("reload.metric", 1, 1))
	abort := make(chan bool)
	defer close(abort)
	got := make(chan string, 1)
	go func() { got <- app.Cache.WriteoutQueue().Get(abort) }()
	select {
	case metric := <-got:
		t.Fatalf("batch flushed before reload: %s", metric)
	case <-time.After(150 * time.Millisecond):
	}
	cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 0, Duration{}
	writeBatchingConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	select {
	case metric := <-got:
		if metric != "reload.metric" {
			t.Fatalf("metric=%s", metric)
		}
	case <-time.After(time.Second):
		t.Fatal("reload did not release pending batch")
	}
}

func TestWriteoutSparseSpreadReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path := testConfig(t, t.TempDir())
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cache.WriteoutSparseSpread {
		t.Fatal("writeout-sparse-spread must default to off")
	}
	cfg.Whisper.Enabled = false
	cfg.Tcp.Enabled, cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 8, Duration{time.Hour}
	writeBatchingConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	if app.Cache.WriteoutSparseSpread() {
		t.Fatal("sparse spread enabled by default")
	}
	for _, on := range []bool{true, false, true} {
		cfg.Cache.WriteoutSparseSpread = on
		writeBatchingConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if got := app.Cache.WriteoutSparseSpread(); got != on {
			t.Fatalf("after reload to %v: WriteoutSparseSpread=%v", on, got)
		}
	}
}

func TestWriteoutStartupSpreadConfig(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path := testConfig(t, t.TempDir())
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cache.WriteoutStartupSpread {
		t.Fatal("startup spread must default to off: it holds the first point threshold")
	}
	cfg.Whisper.Enabled = false
	cfg.Tcp.Enabled, cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 8, Duration{time.Hour}
	cfg.Cache.WriteoutStartupSpread = true
	writeBatchingConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	if !app.Config.Cache.WriteoutStartupSpread {
		t.Fatal("startup spread did not round-trip")
	}
	for range 8 {
		app.Cache.AddPoint([]byte("startup.config.metric"), 1, 1)
	}
	if app.Cache.WriteoutReady("startup.config.metric") {
		t.Fatal("startup option was not applied at start")
	}
	cfg.Cache.WriteoutStartupSpread = false
	writeBatchingConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if !app.Cache.WriteoutReady("startup.config.metric") {
		t.Fatal("reload did not release the startup point threshold")
	}
}

func TestWriteoutStartupSpreadAfterReplay(t *testing.T) {
	config := inputLogTestConfig(t, t.TempDir(), func(cfg *Config) {
		cfg.Cache.WriteoutMinPoints = 960
		cfg.Cache.WriteoutMaxDelay = Duration{960 * time.Second}
		cfg.Cache.WriteoutStartupSpread = true
	})
	app := startApp(t, config)
	addInputLogTestPoints(app)
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	app.inputLog.Swap(nil).Kill()
	app.Stop()
	app = startApp(t, config)
	defer app.Stop()
	checkInputLogTestPoints(t, app)
	for _, name := range []string{"input.log.one", "input.log.two"} {
		if !app.Cache.WriteoutReady(name) {
			t.Fatal("startup spread deferred urgent replay")
		}
		p, ok := app.Cache.PopNotConfirmed(name)
		if !ok {
			t.Fatal("missing replayed points")
		}
		app.Cache.Confirm(p)
		for range 960 {
			app.Cache.AddPoint([]byte(name), 5, 1700000120)
		}
		if app.Cache.WriteoutReady(name) {
			t.Fatal("first live threshold after replay bypassed its phase")
		}
	}
}
