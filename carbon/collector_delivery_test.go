package carbon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

func TestCollectorDroppedStats(t *testing.T) {
	registry := prometheus.NewRegistry()
	app := &App{Config: NewConfig(), Cache: cache.New(), PromRegisterer: registry}
	app.Config.Common.MetricInterval = &Duration{time.Hour}
	const count = 12830
	for cycle := range 2 {
		collector := NewCollector(app)
		collector.Stop() // Hold the sender to reproduce a report-sized burst.
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Exhausted budget: preserve the original nonblocking fallback.
		send := collector.statCallback(ctx, nil, "carbonserver")
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

func newDeliveryTestCollector() *Collector {
	return &Collector{
		graphPrefix: "test", endpoint: MetricEndpointLocal,
		data: make(chan *points.Points, 4096), logger: zap.NewNop(),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{Name: "internal_stats_dropped_total", Help: "test"}),
	}
}

func TestCollectorLargeReport(t *testing.T) {
	const count = 12831 // More than 2,566 namespaces times five stat points.
	collector := newDeliveryTestCollector()
	core := cache.New()
	resume := make(chan struct{})
	full := make(chan struct{})
	stored := make(chan struct{})
	finished := make(chan struct{})
	collector.Start()
	defer collector.Stop()
	// Hold the real local sender until the report has filled its queue. This
	// makes the old nonblocking callback lose points regardless of scheduling.
	collector.startSender(func(p *points.Points) {
		<-resume
		core.Add(p)
		if p.Metric == "test.cache.existing_stat" {
			close(stored)
		}
	}, nil, collector.logger)
	collector.stats = []statFunc{func(ctx context.Context, exit <-chan bool) {
		send := collector.statCallback(ctx, exit, "carbonserver")
		for i := range count {
			if i == cap(collector.data)+1 {
				close(full)
			}
			send(fmt.Sprintf("usage.metrics.namespace_%05d", i), float64(i))
		}
		collector.statCallback(ctx, exit, "cache")("existing_stat", 42)
	}}
	collector.Go(func(exit chan bool) {
		collector.collect(exit)
		close(finished)
	})
	// Always release the sender before Stop, including when a test fails.
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	select {
	case <-full:
	case <-time.After(10 * time.Second):
		t.Fatal("report did not fill the queue")
	}
	close(resume)
	select {
	case <-stored:
	case <-time.After(10 * time.Second):
		t.Fatal("report did not reach storage")
	}
	<-finished
	for i := range count {
		name := fmt.Sprintf("test.carbonserver.usage.metrics.namespace_%05d", i)
		got := core.Get(name)
		if len(got) != 1 || got[0].Value != float64(i) || got[0].Timestamp == 0 {
			t.Fatalf("%s: stored %v", name, got)
		}
	}
	if got := core.Get("test.cache.existing_stat"); len(got) != 1 || got[0].Value != 42 {
		t.Fatalf("existing stat changed: %v", got)
	}
	if got := collectorDropped(t, collector); got != 0 {
		t.Fatalf("dropped %v points with a healthy sender", got)
	}
	t.Logf("stored all %d quota stats and existing stat through a 4096-entry queue", count)
}

func TestCollectorReportWaitBounded(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%t", shutdown), func(t *testing.T) {
			collector := newDeliveryTestCollector()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			exit := make(chan bool)
			if shutdown {
				// An unexpired report must still stop immediately on shutdown.
				cancel()
				ctx = context.Background()
				close(exit)
			}
			send := collector.statCallback(ctx, exit, "carbonserver")
			start := time.Now()
			const count = 12830
			for i := range count {
				send("usage.metrics.namespace", float64(i))
			}
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Fatalf("report took %s; wait budget must be shared across points", elapsed)
			}
			if got := collectorDropped(t, collector); got != count-4096 {
				t.Fatalf("dropped %v points, want %d", got, count-4096)
			}
		})
	}
}

func TestCollectorStopDuringReport(t *testing.T) {
	collector := newDeliveryTestCollector()
	full := make(chan struct{})
	collector.stats = []statFunc{func(ctx context.Context, exit <-chan bool) {
		send := collector.statCallback(ctx, exit, "carbonserver")
		for i := range 12830 {
			if i == cap(collector.data) {
				close(full)
			}
			send("usage.metrics.namespace", float64(i))
		}
	}}
	collector.Start()
	collector.Go(func(exit chan bool) { collector.collect(exit) })
	<-full
	stopped := make(chan struct{})
	go func() { collector.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not cancel the report's queue wait")
	}
}

func collectorDropped(t *testing.T, c *Collector) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := c.dropped.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}
