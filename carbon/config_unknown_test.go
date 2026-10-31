package carbon

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lomik/zapwriter"
	dto "github.com/prometheus/client_model/go"
)

func captureConfigWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &output
}

func TestUnknownConfigKeys(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	output := captureConfigWarnings(t)
	for _, test := range []struct {
		name, body, key, suggestion string
	}{
		{"top level", "mystery = true\n", "mystery", ""},
		{"quoted key", "'mystery.key' = true\n", `"mystery.key"`, ""},
		{"section", "[carbonsrver]\n", "carbonsrver", "carbonserver"},
		{"nested", "[whisper]\nstore-compaction-workerz = 2\n", "whisper.store-compaction-workerz", "whisper.store-compaction-workers"},
		{"long prefix", "[carbonserver]\nmax-creates-per-interval = 5\n", "carbonserver.max-creates-per-interval", "carbonserver.max-creates-per-second"},
		{"deeply nested", "[carbonserver.grpc]\nlsiten = '127.0.0.1:0'\n", "carbonserver.grpc.lsiten", "carbonserver.grpc.listen"},
		{"tags", "[tags]\ntagdb-timout = '1s'\n", "tags.tagdb-timout", "tags.tagdb-timeout"},
		{"dump", "[dump]\nrestore-per-secon = 2\n", "dump.restore-per-secon", "dump.restore-per-second"},
		{"retired", "[carbonserver]\ngraphite-web-10-strict-mode = true\n", "carbonserver.graphite-web-10-strict-mode", ""},
		{"array of tables", "[[logging]]\nlevle = 'info'\n[[logging]]\nlevle = 'warn'\n", "logging.levle", "logging.level"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "go-carbon.conf")
			if err := os.WriteFile(path, []byte(test.body), 0600); err != nil {
				t.Fatal(err)
			}
			output.Reset()
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(cfg.unknownKeys) != 1 || cfg.unknownKeys[0].String() != test.key {
				t.Fatalf("unknown keys %v, want %s", cfg.unknownKeys, test.key)
			}
			if got, want := strings.Count(output.String(), "[WARN] unknown config key"), 1; got != want {
				t.Fatalf("warning count %d, want %d: %s", got, want, output)
			}
			if !strings.Contains(output.String(), fmt.Sprintf("unknown config key %q", test.key)) {
				t.Fatalf("missing dotted key: %s", output)
			}
			if test.suggestion != "" {
				if !strings.Contains(output.String(), fmt.Sprintf("did you mean %q?", test.suggestion)) {
					t.Fatalf("missing suggestion %q: %s", test.suggestion, output)
				}
			} else if strings.Contains(output.String(), "did you mean") {
				t.Fatalf("unrelated suggestion: %s", output)
			}
		})
	}
}

func TestConfigConsumedKeys(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	// Protocol-owned maps can contain tables and arrays of tables, whose
	// children TOML leaves undecoded behind interface{} values.
	body := `
[receiver.custom]
protocol = "custom"
listen = "127.0.0.1:0"
[receiver.custom.headers]
"X-Custom-Header" = "value"
[[receiver.custom.filters]]
pattern = "prefix.*"
[[receiver.custom.filters]]
pattern = "other.*"
[receiver.tcp2]
protocol = "tcp"
listen = "127.0.0.1:0"
buffer-size = 10
[prometheus.labels]
"custom.label" = "value"
[carbonserver.discovery-cache-ttl]
"custom.*" = "10s"
[tags]
tagdb-url = "http://127.0.0.1:8000"
tagdb-timeout = "1s"
[dump]
path = "dump"
restore-per-second = 10
[[logging]]
logger = "custom"
file = "stderr"
level = "info"
`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.unknownKeys) != 0 {
		t.Fatalf("consumed keys reported as unknown: %v", cfg.unknownKeys)
	}
	if cfg.Receiver["custom"]["headers"] == nil || cfg.Receiver["custom"]["filters"] == nil {
		t.Fatal("custom receiver options were not preserved")
	}
}

func TestExampleConfigUnknownKeysClean(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"../go-carbon.conf.example", "../go-carbon.docker.conf", testConfig(t, t.TempDir())} {
		cfg, err := ReadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.unknownKeys) != 0 {
			t.Fatalf("%s: unknown keys %v", path, cfg.unknownKeys)
		}
	}
	// The deploy configs use their own INI-style parsers, referenced by
	// known TOML options. Exercise those paths together with the example.
	cfg, err := ReadConfig("../go-carbon.conf.example")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.SchemasFilename = "../deploy/storage-schemas.conf"
	cfg.Whisper.AggregationFilename = "../deploy/storage-aggregation.conf"
	cfg.Whisper.QuotasFilename = "../deploy/storage-quota.conf"
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	writeBatchingConfig(t, path, cfg)
	cfg, err = loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.unknownKeys) != 0 || len(cfg.Whisper.Schemas) == 0 || cfg.Whisper.Aggregation == nil || len(cfg.Whisper.Quotas) == 0 {
		t.Fatalf("deploy configuration not fully loaded: unknown keys %v", cfg.unknownKeys)
	}
}

func TestConfigUnknownKeysReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	output := captureConfigWarnings(t)
	path := testConfig(t, t.TempDir())
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Pprof.Enabled, cfg.Carbonserver.Enabled, cfg.Whisper.Enabled = false, false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Prometheus.Enabled = true
	cfg.Prometheus.Labels["role"] = "test"
	write := func(extra string) {
		t.Helper()
		writeBatchingConfig(t, path, cfg)
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.WriteString(extra)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("write: %v, close: %v", writeErr, closeErr)
		}
		output.Reset()
	}
	// Startup warnings must work before zapwriter.ApplyConfig in main.
	write("\n[unexpected]\nretired = true\n")
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	assertUnknownKeysGauge(t, app, 2)
	if strings.Count(output.String(), "[WARN] unknown config key") != 2 {
		t.Fatalf("startup warnings: %s", output)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	for _, extra := range []string{"\n[unexpected]\nretired = true\n", "\n[unexpected]\nretired = true\n", ""} {
		write(extra)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		want := 0
		if extra != "" {
			want = 2
		}
		assertUnknownKeysGauge(t, app, float64(want))
		if got := strings.Count(output.String(), "[WARN] unknown config key"); got != want {
			t.Fatalf("reload warning count %d, want %d: %s", got, want, output)
		}
	}
}

func assertUnknownKeysGauge(t *testing.T, app *App, want float64) {
	t.Helper()
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "config_unknown_keys" {
			continue
		}
		if family.GetType() != dto.MetricType_GAUGE || len(family.Metric) != 1 || family.Metric[0].GetGauge().GetValue() != want {
			t.Fatalf("unknown key gauge %v, want %v", family, want)
		}
		labels := family.Metric[0].GetLabel()
		if len(labels) != 1 || labels[0].GetName() != "role" || labels[0].GetValue() != "test" {
			t.Fatalf("configured metric labels missing: %v", labels)
		}
		return
	}
	t.Fatal("config_unknown_keys missing")
}
