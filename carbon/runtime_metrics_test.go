package carbon

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestRuntimeMetricsDuringLifecycleLock(t *testing.T) {
	app := &App{}
	r := prometheus.NewPedanticRegistry()
	r.MustRegister(runtimeWaitCollector{}, &migrationCollector{app: app})
	app.Lock()
	done := make(chan struct{})
	response := httptest.NewRecorder()
	go func() {
		defer close(done)
		promhttp.HandlerFor(r, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	}()
	select {
	case <-done:
		app.Unlock()
	case <-time.After(time.Second):
		app.Unlock()
		<-done
		t.Fatal("scrape waited for app lifecycle lock")
	}
	for _, name := range []string{"# TYPE go_sync_mutex_wait_seconds_total counter", "go_sched_latency_seconds_bucket", "migration_metrics_skipped_total 1"} {
		if !strings.Contains(response.Body.String(), name) {
			t.Fatal("missing", name, response.Body.String())
		}
	}
	var got float64
	found, buckets := false, 0
	RuntimeStat(func(k string, v float64) {
		if k == "mutexWaitSecondsTotal" {
			got = v
			found = true
		}
		if strings.HasPrefix(k, "schedulerLatencyBuckets.le") {
			buckets++
		}
	})
	if !found || got < 0 || buckets < 10 || strings.Count(response.Body.String(), "go_sched_latency_seconds_bucket{") < 10 {
		t.Fatal(got)
	}
}

func BenchmarkRuntimeWaitTelemetry(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		runtimeWaitStat(func(string, float64) {})
	}
}
