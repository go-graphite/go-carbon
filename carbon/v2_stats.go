package carbon

import (
	"context"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/prometheus/client_golang/prometheus"
)

type v2Runtime struct {
	daemonstore.Backend
	app         *App
	failures    *prometheus.CounterVec
	readLatency prometheus.Histogram
	desc        []*prometheus.Desc
	stopErrors  chan struct{}
	errorsDone  chan struct{}
	statMu      sync.Mutex
	lastPoints  uint64
	lastStat    time.Time
	readMu      sync.Mutex
	readWG      sync.WaitGroup
	readClosing bool
	readContext context.Context
	cancelReads context.CancelFunc
}

func newV2Runtime(app *App, b daemonstore.Backend) *v2Runtime {
	readContext, cancelReads := context.WithCancel(context.Background())
	v := &v2Runtime{
		Backend: b, app: app, lastStat: time.Now(),
		readContext: readContext, cancelReads: cancelReads,
		stopErrors: make(chan struct{}), errorsDone: make(chan struct{}),
		failures:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "go_carbon_v2_errors_total", Help: "Immutable-parts operation failures."}, []string{"operation"}),
		readLatency: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "go_carbon_v2_read_duration_seconds", Help: "Immutable-parts fetch duration.", Buckets: prometheus.DefBuckets}),
	}
	for _, metric := range []struct{ name, help string }{
		{"ingested_points_total", "Points accepted by the immutable-parts engine."},
		{"parts", "Immutable parts currently retained."},
		{"compaction_debt_bytes", "Bytes of pending compaction work."},
		{"write_stalls_total", "Immutable-parts write stalls."},
		{"snapshot_pins", "Active immutable snapshot pins."},
		{"seal_duration_seconds", "Most recent immutable part seal duration."},
		{"read_p99_seconds", "Backend observed read latency at the 99th percentile."},
	} {
		v.desc = append(v.desc, prometheus.NewDesc("go_carbon_v2_"+metric.name, metric.help, nil, nil))
	}
	go func() {
		defer close(v.errorsDone)
		errors := b.Errors()
		for {
			select {
			case <-v.stopErrors:
				return
			case failure, ok := <-errors:
				if !ok {
					return
				}
				if failure.Err != nil {
					v.failure(failure.Operation, failure.Err, true)
				}
			}
		}
	}()
	return v
}

func (v *v2Runtime) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range v.desc {
		ch <- d
	}
	v.failures.Describe(ch)
	v.readLatency.Describe(ch)
}
func (v *v2Runtime) Collect(ch chan<- prometheus.Metric) {
	s := v.Backend.Stats()
	values := []float64{float64(s.IngestedPoints), float64(s.Parts), float64(s.CompactionDebtBytes), float64(s.WriteStalls), float64(s.SnapshotPins), s.SealLatencySeconds, s.ReadP99Seconds}
	for i, value := range values {
		kind := prometheus.GaugeValue
		if i == 0 || i == 3 {
			kind = prometheus.CounterValue
		}
		ch <- prometheus.MustNewConstMetric(v.desc[i], kind, value)
	}
	v.failures.Collect(ch)
	v.readLatency.Collect(ch)
}
func (v *v2Runtime) Stat(send helper.StatCallback) {
	s := v.Backend.Stats()
	v.statMu.Lock()
	now := time.Now()
	rate := float64(s.IngestedPoints-v.lastPoints) / now.Sub(v.lastStat).Seconds()
	v.lastPoints, v.lastStat = s.IngestedPoints, now
	v.statMu.Unlock()
	send("ingestPointsPerSecond", rate)
	send("parts", float64(s.Parts))
	send("compactionDebtBytes", float64(s.CompactionDebtBytes))
	send("writeStalls", float64(s.WriteStalls))
	send("snapshotPins", float64(s.SnapshotPins))
	send("sealSeconds", s.SealLatencySeconds)
	send("readP99Seconds", s.ReadP99Seconds)
}
