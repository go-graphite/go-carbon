package carbon

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-graphite/go-carbon/helper/heartbeat"
	"github.com/go-graphite/go-carbon/helper/qa"
)

// noFixedPorts disables the listeners whose default ports other tests in the
// package may still hold.
func noFixedPorts(c *Config) {
	c.Udp.Enabled, c.Tcp.Enabled, c.Pickle.Enabled = false, false, false
	c.Grpc.Enabled, c.Carbonlink.Enabled, c.Carbonserver.Enabled = false, false, false
}

func TestHeartbeatCleanAndUncleanStops(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	qa.Root(t, func(root string) {
		hb := filepath.Join(root, "heartbeat.json")
		newApp := func() *App {
			app := New(testConfig(t, root))
			if err := app.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			noFixedPorts(app.Config)
			app.Config.Common.HeartbeatFile = hb
			app.Config.Dump.Enabled = true
			app.Config.Dump.Path = root
			app.Config.Cache.MaxSize = 12345
			app.PromRegisterer = prometheus.WrapRegistererWith(prometheus.Labels{"carbon_cluster": "c1"}, app.PromRegistry)
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			return app
		}

		first := newApp()
		if m := heartbeatMetrics(t, first); m["heartbeat_unclean_starts_total"] != 0 || m["heartbeat_gap_start_timestamp_seconds"] != 0 ||
			m["heartbeat_gap_end_timestamp_seconds"] != 0 || m["heartbeat_last_beat_timestamp_seconds"] <= 0 || m["heartbeat_write_errors_total"] != 0 {
			t.Fatalf("clean first start metrics %v", m)
		}
		s, err := heartbeat.Read(hb)
		if err != nil || s.Clean || s.Ready.IsZero() || s.Unclean != nil || s.CacheMaxSize != 12345 {
			t.Fatalf("running state %+v %v", s, err)
		}
		if err := first.DumpStop(); err != nil {
			t.Fatal(err)
		}
		first.Stop()
		if s, err = heartbeat.Read(hb); err != nil || !s.Clean {
			t.Fatalf("dump stop must mark clean: %+v %v", s, err)
		}

		// A stop without a dump drops the cache, so it is unclean.
		second := newApp()
		if second.heartbeat.Unclean() != nil {
			t.Fatal("clean dump stop reported as unclean")
		}
		second.Stop()
		stopped, err := heartbeat.Read(hb)
		if err != nil || stopped.Clean {
			t.Fatalf("plain stop must not mark clean: %+v %v", stopped, err)
		}

		third := newApp()
		defer third.Stop()
		s, err = heartbeat.Read(hb)
		if err != nil || s.Unclean == nil || !s.Unclean.LastAlive.Equal(stopped.Updated) ||
			s.Unclean.Resumed.IsZero() || s.Unclean.LossWindowSeconds != int64(third.Config.Cache.WriteoutMaxDelay.Value().Seconds()) {
			t.Fatalf("unclean window not recorded: %+v %v", s.Unclean, err)
		}
		m := heartbeatMetrics(t, third)
		gapStart := float64(s.Unclean.LastAlive.Unix() - s.Unclean.LossWindowSeconds)
		if m["heartbeat_unclean_starts_total"] != 1 || m["heartbeat_gap_start_timestamp_seconds"] != gapStart ||
			m["heartbeat_gap_end_timestamp_seconds"] != float64(s.Unclean.Resumed.Unix()) || m["heartbeat_last_beat_timestamp_seconds"] < float64(s.Unclean.Resumed.Unix()) {
			t.Fatalf("unclean start metrics %v, window %+v", m, s.Unclean)
		}
	})
}

func TestHeartbeatDefaultOff(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	qa.Root(t, func(root string) {
		app := New(testConfig(t, root))
		if err := app.ParseConfig(); err != nil {
			t.Fatal(err)
		}
		if app.Config.Common.HeartbeatFile != "" {
			t.Fatal("heartbeat must be off by default")
		}
		noFixedPorts(app.Config)
		if err := app.Start(); err != nil {
			t.Fatal(err)
		}
		defer app.Stop()
		if app.heartbeat != nil {
			t.Fatal("heartbeat started without a file")
		}
		families, err := app.PromRegistry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			if strings.HasPrefix(f.GetName(), "heartbeat_") {
				t.Fatalf("%s registered without a heartbeat file", f.GetName())
			}
		}
	})
}

// heartbeatMetrics gathers the heartbeat series, checking the constant label.
func heartbeatMetrics(t *testing.T, app *App) map[string]float64 {
	t.Helper()
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "heartbeat_") {
			continue
		}
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != 1 || m.GetLabel()[0].GetName() != "carbon_cluster" || m.GetLabel()[0].GetValue() != "c1" {
				t.Fatalf("%s labels %v", f.GetName(), m.GetLabel())
			}
			out[f.GetName()] = m.GetGauge().GetValue() + m.GetCounter().GetValue()
		}
	}
	if len(out) != 5 {
		t.Fatalf("heartbeat metrics %v", out)
	}
	return out
}
