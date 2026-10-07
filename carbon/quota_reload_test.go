package carbon

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestReloadConfigUpdatesQuotasWithoutRestart(t *testing.T) {
	defer zapwriter.Test()()
	app, quota := newQuotaReloadApp(t)
	defer app.Stop()
	server := app.Carbonserver
	cache := app.Cache
	cache.Add(points.OnePoint("namespace.existing", 1, time.Now().Unix()-2))
	deadline := time.Now().Add(5 * time.Second)
	for !server.MetricExists("namespace.existing") {
		if time.Now().After(deadline) {
			t.Fatal("metric did not appear")
		}
		time.Sleep(time.Millisecond)
	}
	defer checkQuotaConcurrently(server)()
	for _, limit := range []int{10, 1, 20, 1, 0} {
		if limit == 0 {
			quota("")
		} else {
			quota(fmt.Sprintf("[/]\nmetrics=%d\n", limit))
		}
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.Carbonserver != server || app.Cache != cache {
			t.Fatal("reload replaced serving components")
		}
		deadline = time.Now().Add(5 * time.Second)
		for server.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) != (limit == 1) {
			if time.Now().After(deadline) {
				t.Fatalf("limit %d did not apply", limit)
			}
			time.Sleep(time.Millisecond)
		}
	}
	quota("[/]\nmetrics=invalid\n")
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("malformed quota accepted")
	}
	if server.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("failed reload changed limits")
	}
}

// newQuotaReloadApp starts a real trie-backed application and returns a writer
// for its quota file so reload tests exercise the normal config parsing path.
func newQuotaReloadApp(t *testing.T, configure ...func(*Config)) (*App, func(string)) {
	t.Helper()
	root := t.TempDir()
	path := TestConfig(root)
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Pprof.Enabled, cfg.Prometheus.Enabled = false, true
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Carbonserver.Enabled = true
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Carbonserver.Listen = l.Addr().String()
	l.Close()
	cfg.Carbonserver.TrieIndex = true
	cfg.Carbonserver.ConcurrentIndex = true
	cfg.Carbonserver.RealtimeIndex = 100
	cfg.Carbonserver.ScanFrequency = &Duration{time.Hour}
	cfg.Whisper.QuotasFilename = filepath.Join(root, "quotas.conf")
	quota := func(body string) {
		t.Helper()
		if err := os.WriteFile(cfg.Whisper.QuotasFilename+".tmp", []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(cfg.Whisper.QuotasFilename+".tmp", cfg.Whisper.QuotasFilename); err != nil {
			t.Fatal(err)
		}
	}
	quota("[/]\nmetrics=1\n")
	for _, apply := range configure {
		apply(cfg)
	}
	writeBatchingConfig(t, path, cfg)
	app := New(path)
	if err = app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
	return app, quota
}

// checkQuotaConcurrently exercises ingestion's quota path while configuration
// reloads publish new rules and estimator snapshots.
func checkQuotaConcurrently(server *carbonserver.CarbonserverListener) func() {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			server.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, time.Now().Unix()), false)
			time.Sleep(time.Microsecond)
		}
	}()
	return func() { close(stop); wg.Wait() }
}

func TestQuotaReloadIntervalConfig(t *testing.T) {
	if got := NewConfig().Whisper.QuotasReloadInterval.Value(); got != 0 {
		t.Fatalf("default interval = %v; want disabled", got)
	}
	for _, interval := range []time.Duration{0, time.Minute, -time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			path := TestConfig(t.TempDir())
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.QuotasReloadInterval = Duration{interval}
			writeBatchingConfig(t, path, cfg)
			app := New(path)
			err = app.ParseConfig()
			if interval < 0 {
				if err == nil || !strings.Contains(err.Error(), "quotas-reload-interval") {
					t.Fatalf("negative interval accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if app.Config.Whisper.QuotasReloadInterval.Value() != interval {
				t.Fatal("interval changed in config round trip")
			}
		})
	}
}

func waitQuotaThrottle(t *testing.T, server *carbonserver.CarbonserverListener, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for server.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) != want {
		if time.Now().After(deadline) {
			t.Fatalf("quota throttle did not become %t", want)
		}
		time.Sleep(time.Millisecond)
	}
}

func addQuotaExistingMetric(t *testing.T, app *App) {
	t.Helper()
	app.Cache.Add(points.OnePoint("namespace.existing", 1, time.Now().Unix()-2))
	deadline := time.Now().Add(5 * time.Second)
	for !app.Carbonserver.MetricExists("namespace.existing") {
		if time.Now().After(deadline) {
			t.Fatal("metric did not appear")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestQuotaReloadIntervalUpdatesOnlyQuotas(t *testing.T) {
	defer zapwriter.Test()()
	app, quota := newQuotaReloadApp(t, func(cfg *Config) {
		cfg.Whisper.QuotasReloadInterval = Duration{10 * time.Millisecond}
	})
	defer app.Stop()
	server, cache, persister, collector := app.Carbonserver, app.Cache, app.Persister, app.Collector
	addQuotaExistingMetric(t, app)
	defer checkQuotaConcurrently(server)()
	for _, limit := range []int{20, 1, 30, 1, 0, 1} {
		if limit == 0 {
			quota("")
		} else {
			quota(fmt.Sprintf("[/]\nmetrics=%d\n", limit))
		}
		waitQuotaThrottle(t, server, limit == 1)
	}
	if app.Carbonserver != server || app.Cache != cache || app.Persister != persister || app.Collector != collector {
		t.Fatal("periodic reload replaced application components")
	}

	for _, body := range []string{"[/]\nmetrics=invalid\n", "[[]\nmetrics=20\n"} {
		quota(body)
		if err := app.quotaReloader.reload(); err == nil {
			t.Fatalf("invalid quota accepted: %q", body)
		}
		if !server.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
			t.Fatal("invalid reload changed active limits")
		}
	}
	if err := os.Remove(app.Config.Whisper.QuotasFilename); err != nil {
		t.Fatal(err)
	}
	if err := app.quotaReloader.reload(); err == nil {
		t.Fatal("missing quota file accepted")
	}
	if !server.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("missing file removed active limits")
	}
	quota("[/]\nmetrics=20\n")
	waitQuotaThrottle(t, server, false)
}

func TestQuotaReloadIntervalReconfigurationAndStop(t *testing.T) {
	defer zapwriter.Test()()
	app, quota := newQuotaReloadApp(t, func(cfg *Config) {
		cfg.Whisper.QuotasReloadInterval = Duration{time.Hour}
	})
	defer app.Stop()
	addQuotaExistingMetric(t, app)
	r := app.quotaReloader
	setInterval := func(interval time.Duration) {
		t.Helper()
		cfg, err := ReadConfig(app.ConfigFilename)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Whisper.QuotasReloadInterval = Duration{interval}
		writeBatchingConfig(t, app.ConfigFilename, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
	}
	setInterval(10 * time.Millisecond)
	quota("[/]\nmetrics=20\n")
	waitQuotaThrottle(t, app.Carbonserver, false)
	setInterval(0)
	quota("[/]\nmetrics=1\n")
	time.Sleep(50 * time.Millisecond)
	if app.Carbonserver.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("polling continued after disabling interval")
	}
	setInterval(10 * time.Millisecond)
	waitQuotaThrottle(t, app.Carbonserver, true)
	quota("[/]\nmetrics=20\n")
	waitQuotaThrottle(t, app.Carbonserver, false)

	// A path change must prevent the old file from publishing rules afterwards.
	cfg, err := ReadConfig(app.ConfigFilename)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.QuotasFilename += ".new"
	if err := os.WriteFile(cfg.Whisper.QuotasFilename, []byte("[/]\nmetrics=1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeBatchingConfig(t, app.ConfigFilename, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	waitQuotaThrottle(t, app.Carbonserver, true)
	quota("")
	time.Sleep(50 * time.Millisecond)
	if !app.Carbonserver.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("old quota file overwrote the new file's rules")
	}
	// A rejected main-config reload must leave the running poller intact.
	cfg.Whisper.QuotasReloadInterval = Duration{-time.Second}
	writeBatchingConfig(t, app.ConfigFilename, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("negative reload interval accepted")
	}
	if err := os.WriteFile(cfg.Whisper.QuotasFilename+".tmp", []byte("[/]\nmetrics=20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(cfg.Whisper.QuotasFilename+".tmp", cfg.Whisper.QuotasFilename); err != nil {
		t.Fatal(err)
	}
	waitQuotaThrottle(t, app.Carbonserver, false)

	stopped := make(chan struct{})
	go func() {
		app.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown deadlocked with quota polling")
	}
	select {
	case <-r.done:
	default:
		t.Fatal("shutdown left the quota poller running")
	}
}

func TestQuotaReloadRetainsSharedStorageValidation(t *testing.T) {
	defer zapwriter.Test()()
	app, quota := newQuotaReloadApp(t, func(cfg *Config) {
		cfg.Whisper.StorageBackend = "pebble-chunk"
		cfg.Whisper.QuotasReloadInterval = Duration{time.Hour}
	})
	defer app.Stop()
	quota("[/]\nphysical-size=100\n")
	if err := app.quotaReloader.reload(); err == nil || !strings.Contains(err.Error(), "physical-size") {
		t.Fatalf("shared storage accepted physical-size quota: %v", err)
	}
}

func TestPebbleChunkIgnorePhysicalQuotasStartupAndReload(t *testing.T) {
	defer zapwriter.Test()()
	for _, backend := range []string{"files", "pebble-chunk"} {
		t.Run(backend, func(t *testing.T) {
			app, quota := newQuotaReloadApp(t, func(cfg *Config) {
				cfg.Whisper.StorageBackend = backend
				cfg.Whisper.PebbleChunkIgnorePhysicalQuotas = true
				cfg.Whisper.QuotasReloadInterval = Duration{time.Hour}
				if err := os.WriteFile(cfg.Whisper.QuotasFilename, []byte("[/]\nmetrics=20\nphysical-size=1\n"), 0600); err != nil {
					t.Fatal(err)
				}
			})
			defer app.Stop()
			server := app.Carbonserver
			if backend == "files" {
				waitQuotaThrottle(t, server, true)
				defer checkQuotaConcurrently(server)()
				for _, reload := range []struct {
					name string
					run  func() error
				}{
					{name: "poll", run: app.quotaReloader.reload},
					{name: "SIGHUP", run: app.ReloadConfig},
				} {
					t.Run(reload.name, func(t *testing.T) {
						for _, size := range []string{"max", "2"} {
							quota(fmt.Sprintf("[/]\nmetrics=20\nphysical-size=%s\n", size))
							if err := reload.run(); err != nil {
								t.Fatal(err)
							}
							waitQuotaThrottle(t, server, size == "2")
						}
					})
				}
				if !app.Config.Whisper.PebbleChunkIgnorePhysicalQuotas || app.Carbonserver != server {
					t.Fatal("reload changed the configured flag or the running server")
				}
				return
			}
			addQuotaExistingMetric(t, app)
			if app.MetricStore != nil {
				// Shared scans rebuild from the catalog, so the metric must be
				// persisted before it can count towards the reloaded quota.
				deadline := time.Now().Add(5 * time.Second)
				for {
					_, err := app.MetricStore.Metadata(context.Background(), "namespace.existing")
					if err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("metric not persisted: %v", err)
					}
					time.Sleep(time.Millisecond)
				}
				if err := server.RefreshMetricStoreIndex(); err != nil {
					t.Fatal(err)
				}
			}
			waitQuotaThrottle(t, server, false)
			if app.Config.Whisper.Quotas[0].PhysicalSize != 1 {
				t.Fatal("startup changed the configured physical quota")
			}
			defer checkQuotaConcurrently(server)()
			for _, limit := range []int{1, 20} {
				quota(fmt.Sprintf("[/]\nmetrics=%d\nphysical-size=2\n", limit))
				if err := app.quotaReloader.reload(); err != nil {
					t.Fatal(err)
				}
				waitQuotaThrottle(t, server, limit == 1)
			}

			cfg, err := ReadConfig(app.ConfigFilename)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.PebbleChunkIgnorePhysicalQuotas = false
			writeBatchingConfig(t, app.ConfigFilename, cfg)
			err = app.ReloadConfig()
			if err == nil || !strings.Contains(err.Error(), "physical-size") {
				t.Fatalf("shared storage accepted physical quotas after disabling opt-in: %v", err)
			}
			if !app.Config.Whisper.PebbleChunkIgnorePhysicalQuotas {
				t.Fatal("rejected reload changed the ignore policy")
			}
			waitQuotaThrottle(t, server, false)

			quota("[/]\nmetrics=1\n")
			if err := app.ReloadConfig(); err != nil {
				t.Fatal(err)
			}
			if app.Config.Whisper.PebbleChunkIgnorePhysicalQuotas || app.Carbonserver != server {
				t.Fatal("SIGHUP did not update the ignore policy on the running server")
			}
			waitQuotaThrottle(t, server, true)
		})
	}
}
