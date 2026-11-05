package carbon

import (
	"net/http/httptest"
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
