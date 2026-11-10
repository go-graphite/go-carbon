package carbon

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMigrationMetricsScrapeDoesNotWaitForShard(t *testing.T) {
	for _, query := range []string{"", "?name[]=cache_points"} {
		t.Run(query, func(t *testing.T) {
			core := cache.New()
			core.Add(points.OnePoint("scrape.locked", 1, 1))
			locked, release, writerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			core.SetThrottle(func(*points.Points, bool) bool {
				close(locked)
				<-release // Throttles run with the shard write lock held.
				return false
			})
			go func() {
				defer close(writerDone)
				core.AddPoint([]byte("scrape.locked"), 2, 2)
			}()
			<-locked
			unlock := sync.OnceFunc(func() { close(release); <-writerDone })
			defer unlock()

			registry := prometheus.NewRegistry()
			registry.MustRegister(&migrationCollector{app: &App{Cache: core}})
			handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics"+query, nil))
			}()
			select {
			case <-done:
				if response.Code != 200 || !strings.Contains(response.Body.String(), "cache_points 1\n") {
					t.Fatalf("scrape status %d: %s", response.Code, response.Body.String())
				}
			case <-time.After(time.Second):
				// Release the writer and join both goroutines even on the old path.
				unlock()
				<-done
				t.Fatal("metrics scrape waited for a cache shard write lock")
			}
		})
	}
}

// Startup holds the lifecycle write lock across storage/checkpoint recovery.
// The actual HTTP handler must still complete, including startup telemetry.
func TestMigrationMetricsScrapeDuringStartup(t *testing.T) {
	app := &App{}
	s := &startupInput{sealed: make(chan struct{})}
	s.size.Store(123)
	s.fallback.Store(true)
	app.startupCaptureState.Store(s)
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(&migrationCollector{app: app})
	app.Lock()
	defer app.Unlock()
	done := make(chan struct{})
	response := httptest.NewRecorder()
	go func() {
		defer close(done)
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	}()
	select {
	case <-done:
		if response.Code != 200 || !strings.Contains(response.Body.String(), "migration_metrics_skipped_total 1\n") || !strings.Contains(response.Body.String(), "startup_capture_bytes 123\n") {
			t.Fatal(response.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("scrape blocked on startup lifecycle lock")
	}
}

func TestStartupHTTPMetricsAndPprof(t *testing.T) {
	app := &App{}
	reg := prometheus.NewRegistry()
	reg.MustRegister(&migrationCollector{app: app})
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	server := httptest.NewServer(mux)
	defer server.Close()
	app.Lock()
	defer app.Unlock()
	client := &http.Client{Timeout: time.Second}
	for _, path := range []string{"/metrics", "/debug/pprof/goroutine?debug=1"} {
		response, err := client.Get(server.URL + path)
		if err != nil {
			t.Fatal(path, err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 {
			t.Fatal(path, response.StatusCode, err)
		}
	}
}

func TestStartupCaptureDirectGauge(t *testing.T) {
	app := &App{}
	s := &startupInput{sealed: make(chan struct{})}
	app.startupCaptureState.Store(s)
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(&migrationCollector{app: app})
	for _, direct := range []bool{false, true} {
		s.direct.Store(direct)
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, family := range families {
			if family.GetName() == "startup_capture_direct" {
				want := float64(0)
				if direct {
					want = 1
				}
				if family.Metric[0].GetGauge().GetValue() != want || !strings.Contains(family.GetHelp(), "ordinary cache admission and input logging") {
					t.Fatal("direct gauge/help", family)
				}
				found = true
			}
		}
		if !found {
			t.Fatal("missing direct gauge")
		}
	}
}
