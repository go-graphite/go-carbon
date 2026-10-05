package carbon

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestReloadConfigUpdatesQuotasWithoutRestart(t *testing.T) {
	defer zapwriter.Test()()
	root := t.TempDir()
	path := TestConfig(root)
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Pprof.Enabled, cfg.Prometheus.Enabled = false, false
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
		if err := os.WriteFile(cfg.Whisper.QuotasFilename, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	quota("[/]\nmetrics=1\n")
	writeBatchingConfig(t, path, cfg)
	app := New(path)
	if err = app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
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
	defer func() { close(stop); wg.Wait() }()
	for _, limit := range []int{10, 1, 20, 1, 0} {
		if limit == 0 {
			quota("")
		} else {
			quota(fmt.Sprintf("[/]\nmetrics=%d\n", limit))
		}
		if err = app.ReloadConfig(); err != nil {
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
	if err = app.ReloadConfig(); err == nil {
		t.Fatal("malformed quota accepted")
	}
	if server.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("failed reload changed limits")
	}
}
