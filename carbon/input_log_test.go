package carbon

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// inputLogTestConfig writes a config with the input log enabled and every
// listener off. Without whisper there is no persister, so cached points stay.
func inputLogTestConfig(t *testing.T, root string, mutate func(*Config)) string {
	t.Helper()
	testConfig(t, root) // schemas
	cfg := NewConfig()
	cfg.Whisper.DataDir = root
	cfg.Whisper.SchemasFilename = filepath.Join(root, "schemas.conf")
	cfg.Whisper.Enabled = false
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Dump.Path = filepath.Join(root, "dump")
	cfg.InputLog.Enabled = true
	cfg.InputLog.Path = filepath.Join(root, "input-log")
	cfg.InputLog.SyncInterval = Duration{time.Hour} // cycles run on Sync only
	cfg.InputLog.MaxAge = Duration{1000 * time.Hour}
	if mutate != nil {
		mutate(cfg)
	}
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(cfg); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "go-carbon.conf")
	if err := os.WriteFile(file, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func startApp(t *testing.T, config string) *App {
	t.Helper()
	app := New(config)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	return app
}

var inputLogTestPoints = []*points.Points{
	points.OnePoint("input.log.one", 1, 1700000000),
	points.OnePoint("input.log.two", 2, 1700000000).Add(3, 1700000060),
	points.OnePoint("input.log.one", 4, 1700000060),
}

func addInputLogTestPoints(app *App) {
	for _, p := range inputLogTestPoints {
		app.Cache.Add(p.Copy())
	}
}

func checkInputLogTestPoints(t *testing.T, app *App) {
	t.Helper()
	want := map[string][]points.Point{
		"input.log.one": {{Value: 1, Timestamp: 1700000000}, {Value: 4, Timestamp: 1700000060}},
		"input.log.two": {{Value: 2, Timestamp: 1700000000}, {Value: 3, Timestamp: 1700000060}},
	}
	for metric, w := range want {
		if got := app.Cache.Get(metric); !slices.Equal(got, w) {
			t.Fatalf("%s: got %v, want %v", metric, got, w)
		}
	}
}

// A crashed instance's synced points are back in the cache before Start
// returns, that is before receivers open. A graceful stop without a dump
// keeps them for the next start too.
func TestInputLogReplayedOnStart(t *testing.T) {
	root := t.TempDir()
	config := inputLogTestConfig(t, root, nil)
	app := startApp(t, config)
	addInputLogTestPoints(app)
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	app.Cache.Add(points.OnePoint("input.log.unsynced", 1, 1))
	app.inputLog.Swap(nil).Kill() // the crash
	app.Stop()

	app = startApp(t, config)
	checkInputLogTestPoints(t, app)
	if err := app.DumpStop(); err != nil { // dump disabled: sync and close
		t.Fatal(err)
	}
	app.Stop()

	app = startApp(t, config)
	defer app.Stop()
	checkInputLogTestPoints(t, app)
}

// A graceful dump retires the log: its points are restored from the dump,
// once, and nothing is replayed.
func TestInputLogRetiredByDump(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, format := range []string{"legacy", "compact", "blocks"} {
		t.Run(format, func(t *testing.T) { testInputLogRetiredByDump(t, format) })
	}
}

func testInputLogRetiredByDump(t *testing.T, format string) {
	root := t.TempDir()
	config := inputLogTestConfig(t, root, func(cfg *Config) { cfg.Dump.Enabled, cfg.Dump.Format = true, format })
	if err := os.MkdirAll(filepath.Join(root, "dump"), 0755); err != nil {
		t.Fatal(err)
	}
	app := startApp(t, config)
	addInputLogTestPoints(app)
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	app.Stop()
	cacheFormat, err := (dumpConfig{Format: format}).cacheFormat()
	if err != nil {
		t.Fatal(err)
	}
	if dumps, _ := filepath.Glob(filepath.Join(root, "dump", recovery.CacheName(cacheFormat, "*"))); len(dumps) != 1 {
		t.Fatalf("%s stop wrote %v", format, dumps)
	}
	entries, err := os.ReadDir(filepath.Join(root, "input-log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			t.Fatalf("segment %s left after the dump", e.Name())
		}
	}

	app = startApp(t, config)
	defer app.Stop()
	if app.inputLog.Load().NeedsReplay() {
		t.Fatal("retired log needs replay")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		dumps, _ := filepath.Glob(filepath.Join(root, "dump", "*.bin"))
		if len(dumps) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dump files not restored: %v", dumps)
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkInputLogTestPoints(t, app)
}

// A dump that fails keeps the log: the process exits without a dump to
// restore, so the next start replays the log instead.
func TestInputLogKeptWhenDumpFails(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	dumpDir := filepath.Join(root, "dump")
	config := inputLogTestConfig(t, root, func(cfg *Config) { cfg.Dump.Enabled = true })
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		t.Fatal(err)
	}
	app := startApp(t, config)
	addInputLogTestPoints(app)
	// Not synced yet: the failed stop syncs what the log recorded.
	if err := os.Remove(dumpDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dumpDir, nil, 0644); err != nil { // dump files cannot be created
		t.Fatal(err)
	}
	if err := app.DumpStop(); err == nil {
		t.Fatal("dump into a file succeeded")
	}
	app.inputLog.Swap(nil).Kill() // the process exits after a failed dump
	app.Stop()
	if err := os.Remove(dumpDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		t.Fatal(err)
	}

	app = startApp(t, config)
	defer app.Stop()
	checkInputLogTestPoints(t, app)
}

// Replayed points reach the persister like any other, and its confirmations
// release their segments.
func TestInputLogReplayReleasedOncePersisted(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	// The crashed instance persisted nothing.
	app := startApp(t, inputLogTestConfig(t, root, nil))
	now := time.Now().Unix()
	app.Cache.Add(points.OnePoint("replayed.metric", 1, now-120).Add(2, now-60))
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	app.inputLog.Swap(nil).Kill()
	app.Stop()

	app = startApp(t, inputLogTestConfig(t, root, func(cfg *Config) {
		cfg.Whisper.Enabled = true
		cfg.Whisper.Compressed = true
		cfg.InputLog.TruncateLag = Duration{}
	}))
	defer app.Stop()
	deadline := time.Now().Add(10 * time.Second)
	for !app.Cache.IsEmpty() {
		if time.Now().After(deadline) {
			t.Fatal("replayed points not persisted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(root, "replayed", "metric.wsp")); err != nil {
		t.Fatal(err)
	}
	l := app.inputLog.Load()
	for range 2 {
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	segments, _ := filepath.Glob(filepath.Join(root, "input-log", "*.log"))
	if len(segments) != 0 {
		t.Fatalf("segments %v kept after their points were persisted", segments)
	}
}

func TestInputLogSettingsRequireRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	app := startApp(t, inputLogTestConfig(t, root, nil))
	defer app.Stop()
	inputLogTestConfig(t, root, func(cfg *Config) { cfg.InputLog.SyncInterval = Duration{time.Second} })
	if err := app.ReloadConfig(); err == nil || !strings.Contains(err.Error(), "input-log") {
		t.Fatalf("reload with changed input-log settings: %v", err)
	}
	inputLogTestConfig(t, root, nil)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
}

func TestInputLogConfigValidation(t *testing.T) {
	batching := func(cfg *Config) {
		cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 960, Duration{960 * time.Second}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{"path", func(c *Config) { c.InputLog.Path = "" }},
		{"dump path", func(c *Config) { c.InputLog.Path = "/var/lib/graphite/dump" }},
		{"sync interval", func(c *Config) { c.InputLog.SyncInterval = Duration{} }},
		{"segment size", func(c *Config) { c.InputLog.SegmentSize = 1 << 20 }},
		{"max age", func(c *Config) { c.InputLog.MaxAge = Duration{time.Second} }},
		{"negative max age", func(c *Config) { c.InputLog.MaxAge = Duration{-time.Second} }},
		{"max age within writeout delay", func(c *Config) { batching(c); c.InputLog.MaxAge = Duration{960 * time.Second} }},
		{"truncate lag", func(c *Config) { c.InputLog.TruncateLag = Duration{-time.Second} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.InputLog.Enabled = true
			if err := validateInputLogConfig(cfg); err != nil {
				t.Fatalf("defaults rejected: %v", err)
			}
			tc.mutate(cfg)
			if err := validateInputLogConfig(cfg); err == nil {
				t.Fatal("invalid setting accepted")
			}
		})
	}

	// By default max-age follows the writeout delay with a margin.
	cfg := NewConfig()
	cfg.InputLog.Enabled = true
	if age := inputLogMaxAge(cfg); age != 30*time.Minute {
		t.Fatalf("max-age without batching %v", age)
	}
	batching(cfg)
	if err := validateInputLogConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if age := inputLogMaxAge(cfg); age != 32*time.Minute {
		t.Fatalf("max-age with a 960s writeout delay %v", age)
	}
}

// A reload may not let points wait for writeout past the running log's
// max-age, which only a restart changes.
func TestInputLogWriteoutDelayReload(t *testing.T) {
	root := t.TempDir()
	delay := func(d time.Duration) func(*Config) {
		return func(cfg *Config) {
			cfg.InputLog.SyncInterval = Duration{time.Second}
			cfg.InputLog.MaxAge = Duration{} // default: 32m for 960s
			cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 960, Duration{d}
		}
	}
	app := startApp(t, inputLogTestConfig(t, root, delay(960*time.Second)))
	defer app.Stop()
	inputLogTestConfig(t, root, delay(32*time.Minute))
	if err := app.ReloadConfig(); err == nil || !strings.Contains(err.Error(), "max-age") {
		t.Fatalf("reload raising the writeout delay to max-age: %v", err)
	}
	inputLogTestConfig(t, root, delay(20*time.Minute))
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
}

// The input log's metrics are registered only with the log enabled, through
// the app registerer with its constant labels.
func TestInputLogMetrics(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, enabled := range []bool{true, false} {
		root := t.TempDir()
		config := inputLogTestConfig(t, root, func(cfg *Config) {
			cfg.InputLog.Enabled = enabled
			cfg.Prometheus.Enabled = true
			cfg.Prometheus.Labels = map[string]string{"host": "h1"}
		})
		app := startApp(t, config)
		addInputLogTestPoints(app)
		if enabled {
			if err := app.inputLog.Load().Sync(); err != nil {
				t.Fatal(err)
			}
		}
		labels := map[string]string{"host": "h1"}
		for _, name := range []string{"input_log_bytes_written_total", "input_log_points_written_total", "input_log_syncs_total",
			"input_log_sync_seconds_exp", "input_log_segments", "input_log_disk_bytes", "input_log_oldest_unpersisted_age_seconds",
			"input_log_relogged_points_total", "input_log_unlogged_points_total", "input_log_write_errors_total",
			"input_log_replayed_points_total", "input_log_replay_seconds", "input_log_cycles_total"} {
			v, ok := metricValue(t, app, name, labels)
			if ok != enabled {
				t.Fatalf("input log enabled %v: %s registered %v", enabled, name, ok)
			}
			if enabled && name == "input_log_points_written_total" && v != 4 {
				t.Fatalf("%s = %v, want 4", name, v)
			}
			if enabled && name == "input_log_sync_seconds_exp" && v != 1 {
				t.Fatalf("%s has %v observations, want 1", name, v)
			}
		}
		for _, stage := range []string{"collect", "encode", "compress", "write", "sync", "roll", "relog", "truncate"} {
			if _, ok := metricValue(t, app, "input_log_stage_seconds_total", map[string]string{"host": "h1", "stage": stage}); ok != enabled {
				t.Fatalf("input log enabled %v: stage %s registered %v", enabled, stage, ok)
			}
		}
		if v, _ := metricValue(t, app, "input_log_stage_seconds_total", map[string]string{"stage": "encode"}); enabled && v <= 0 {
			t.Fatal("encode stage not timed")
		}
		app.Stop()
	}
}
