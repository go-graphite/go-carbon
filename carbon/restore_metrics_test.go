package carbon

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestRestoreMetricsDuringStartupLock(t *testing.T) {
	app := New("")
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(restoreCollector{app: app})
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	scrape := func(want ...string) {
		t.Helper()
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
			done <- response
		}()
		select {
		case response := <-done:
			if response.Code != http.StatusOK {
				t.Fatal(response.Code, response.Body.String())
			}
			for _, line := range want {
				if !strings.Contains(response.Body.String(), line+"\n") {
					t.Fatalf("missing %q in %s", line, response.Body.String())
				}
			}
		case <-time.After(time.Second):
			t.Fatal("scrape blocked on the startup lock")
		}
	}
	app.Lock()
	defer app.Unlock()
	scrape(`restore_loaded_points 0`, `restore_blocked{reason="load"} 0`, `restore_blocked{reason="drain"} 0`)
	core := cache.New()
	var committed atomic.Uint64
	committed.Store(100) // earlier input-log replay is not drain throughput
	now := time.Now()
	p := &restoreProgress{core: core, committed: committed.Load, started: now.Add(-4 * time.Second)}
	app.restoreProgress.Store(p)
	core.Add(points.OnePoint("metric", 1, 1).Add(2, 2))
	p.loaded.Store(2)
	scrape(`restore_loaded_points 2`, `restore_backlog_points 2`, `restore_blocked{reason="load"} 1`)
	batch, ok := core.PopNotConfirmed("metric")
	if !ok {
		t.Fatal("no in-flight batch")
	}
	scrape(`restore_backlog_points 2`) // popped is not persisted
	core.Add(points.OnePoint("metric", 3, 3))
	core.Requeue(batch)
	scrape(`restore_backlog_points 3`) // retry preserves the older prefix
	batch, _ = core.PopNotConfirmed("metric")
	p.loaded.Store(3)
	p.loadComplete(now.Add(-2 * time.Second))
	scrape(`restore_blocked{reason="drain"} 1`, `restore_blocked{reason="load"} 0`, `restore_backlog_points 3`)
	core.Confirm(batch)
	committed.Add(3)
	p.finish(now)
	scrape(`restore_loaded_points 3`, `restore_backlog_points 0`, `restore_persisted_points_per_second 1.5`, `restore_load_seconds 2`, `restore_drain_seconds 2`, `restore_blocked{reason="drain"} 0`)
	committed.Add(1000) // live writes and elapsed wall time cannot alter the final report
	scrape(`restore_persisted_points_per_second 1.5`, `restore_drain_seconds 2`)
}
