package carbon

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/prometheus/client_golang/prometheus"
)

func TestCollectorDroppedStats(t *testing.T) {
	registry := prometheus.NewRegistry()
	app := &App{Config: NewConfig(), Cache: cache.New(), PromRegisterer: registry}
	app.Config.Common.MetricInterval = &Duration{time.Hour}
	const count = 12830
	for cycle := range 2 {
		collector := NewCollector(app)
		collector.Stop() // Hold the sender to reproduce a report-sized burst.
		send := collector.statCallback("carbonserver")
		for i := range count {
			send(fmt.Sprintf("usage.metrics.namespace_%d", i), float64(i))
		}
		if got := len(collector.data); got != 4096 {
			t.Fatalf("queued %d points, want 4096", got)
		}
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		if len(families) != 1 || families[0].GetName() != "internal_stats_dropped_total" {
			t.Fatalf("missing dropped-stat counter: %v", families)
		}
		want := float64((cycle + 1) * (count - 4096))
		t.Logf("cycle %d: emitted %d; queued %d; cumulative drops %.0f", cycle, count, len(collector.data), want)
		if got := families[0].Metric[0].GetCounter().GetValue(); got != want {
			t.Fatalf("dropped %v points, want %v (cycle %d)", got, want, cycle)
		}
	}
}
