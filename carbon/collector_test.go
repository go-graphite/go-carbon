package carbon

import (
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/prometheus/client_golang/prometheus"
)

func TestCollectorFirstInterval(t *testing.T) {
	core := cache.New()
	core.Add(points.OnePoint("input.metric", 1, 1))
	rcv := &tcp.TCP{}
	registry := prometheus.NewRegistry()
	rcv.InitPrometheus(registry)
	app := &App{
		Config: NewConfig(), Cache: core,
		Receivers: []*NamedReceiver{{Name: "tcp", Receiver: rcv}},
	}
	app.Config.Common.GraphPrefix = "test"
	// Drive collection explicitly so the test needs no sleeps or real-time ticks.
	app.Config.Common.MetricInterval = &Duration{time.Hour}
	const startup = uint64(81900000)
	const interval = uint64(1500000)
	for cycle := range 2 {
		// Model a long startup capture, then the same gap across a collector reload.
		rcv.SeedCapturedPoints(startup)
		for range 50 {
			core.Get("input.metric")
		}
		collector := NewCollector(app)
		collector.Stop()
		if len(collector.data) != 0 {
			t.Fatal("baseline published an incomplete interval")
		}

		rcv.SeedCapturedPoints(interval)
		for range 3 {
			core.Get("input.metric")
		}
		collector.collect(nil)
		got := make(map[string]float64)
		for len(collector.data) > 0 {
			p := <-collector.data
			got[p.Metric] = p.Data[0].Value
		}
		for metric, want := range map[string]float64{
			"test.tcp.metricsReceived": float64(interval),
			"test.cache.queries":       3,
			"test.cache.size":          1,
		} {
			if value, ok := got[metric]; !ok || value != want {
				t.Fatalf("cycle %d: %s = %v (present %v), want %v", cycle, metric, value, ok, want)
			}
		}
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, family := range families {
			if family.GetName() == "metrics_received_tcp_total" {
				found = true
				if got, want := family.Metric[0].GetCounter().GetValue(), float64(uint64(cycle+1)*(startup+interval)); got != want {
					t.Fatalf("cumulative received = %v, want %v", got, want)
				}
			}
		}
		if !found {
			t.Fatal("missing cumulative received counter")
		}
	}
}
