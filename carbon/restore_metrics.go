package carbon

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/prometheus/client_golang/prometheus"
)

// Published independently of App's lifecycle lock: Start holds that lock while
// legacy recovery waits for persistence. Scrapes must neither wait for Start nor
// call interval-resetting Stat methods or generate ordinary collector traffic.
type restoreProgress struct {
	core      *cache.Cache
	committed func() uint64
	loaded    atomic.Uint64
	mu        sync.Mutex
	started   time.Time
	loadedAt  time.Time
	finished  time.Time
	drainBase uint64
	drainEnd  uint64
}

func (p *restoreProgress) loadComplete(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drainBase = p.committed()
	p.loadedAt = now
}

func (p *restoreProgress) finish(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drainEnd = p.committed()
	p.finished = now
}

var (
	restoreLoadedDesc  = prometheus.NewDesc("restore_loaded_points", "Points admitted from legacy dump files in this startup.", nil, nil)
	restoreBacklogDesc = prometheus.NewDesc("restore_backlog_points", "Queued and in-flight cache points during legacy restore; zero after the gate completes.", nil, nil)
	restoreRateDesc    = prometheus.NewDesc("restore_persisted_points_per_second", "Average successful point writes per second since legacy dump loading finished; frozen at completion. Uses the configured store sync policy.", nil, nil)
	restoreLoadDesc    = prometheus.NewDesc("restore_load_seconds", "Elapsed legacy dump load time, including saved index warmup; frozen when loading finishes.", nil, nil)
	restoreDrainDesc   = prometheus.NewDesc("restore_drain_seconds", "Elapsed persistence wait after legacy dump loading; frozen at completion.", nil, nil)
	restoreBlockedDesc = prometheus.NewDesc("restore_blocked", "Whether legacy recovery blocks the receiver gate for this reason; all zero when inactive or complete.", []string{"reason"}, nil)
)

type restoreCollector struct{ app *App }

func (restoreCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{restoreLoadedDesc, restoreBacklogDesc, restoreRateDesc, restoreLoadDesc, restoreDrainDesc, restoreBlockedDesc} {
		ch <- desc
	}
}

func (c restoreCollector) Collect(ch chan<- prometheus.Metric) {
	var loaded, backlog, rate, loadSeconds, drainSeconds, loading, draining float64
	if p := c.app.restoreProgress.Load(); p != nil {
		p.mu.Lock()
		now, loadEnd := time.Now(), p.loadedAt
		if loadEnd.IsZero() {
			loading = 1
			loadEnd = now
		} else {
			drainEnd, committed := now, p.committed()
			if !p.finished.IsZero() {
				drainEnd, committed = p.finished, p.drainEnd
			} else {
				draining = 1
			}
			drainSeconds = drainEnd.Sub(p.loadedAt).Seconds()
			if drainSeconds > 0 {
				rate = float64(committed-p.drainBase) / drainSeconds
			}
		}
		loaded = float64(p.loaded.Load())
		loadSeconds = loadEnd.Sub(p.started).Seconds()
		if p.finished.IsZero() {
			backlog = float64(p.core.BacklogPoints())
		}
		p.mu.Unlock()
	}
	for _, metric := range []struct {
		desc  *prometheus.Desc
		value float64
	}{{restoreLoadedDesc, loaded}, {restoreBacklogDesc, backlog}, {restoreRateDesc, rate}, {restoreLoadDesc, loadSeconds}, {restoreDrainDesc, drainSeconds}} {
		ch <- prometheus.MustNewConstMetric(metric.desc, prometheus.GaugeValue, metric.value)
	}
	ch <- prometheus.MustNewConstMetric(restoreBlockedDesc, prometheus.GaugeValue, loading, "load")
	ch <- prometheus.MustNewConstMetric(restoreBlockedDesc, prometheus.GaugeValue, draining, "drain")
}
