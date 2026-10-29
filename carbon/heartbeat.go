package carbon

import (
	"time"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper/heartbeat"
)

// startHeartbeat starts the optional heartbeat file (common.heartbeat-file).
// It never fails startup: an unusable path is logged and the heartbeat is off.
func (app *App) startHeartbeat(core *cache.Cache) {
	conf := app.Config
	if conf.Common.HeartbeatFile == "" || app.heartbeat != nil {
		return
	}
	logger := zapwriter.Logger("heartbeat")
	app.heartbeatCacheMax.Store(int64(conf.Cache.MaxSize))
	interval := 10 * time.Second
	if conf.Common.HeartbeatInterval != nil && conf.Common.HeartbeatInterval.Value() > 0 {
		interval = conf.Common.HeartbeatInterval.Value()
	}
	h, err := heartbeat.Start(heartbeat.Options{
		Path:       conf.Common.HeartbeatFile,
		Interval:   interval,
		LossWindow: conf.Cache.WriteoutMaxDelay.Value(),
		Cache: func() (int64, int64) {
			return core.Size(), app.heartbeatCacheMax.Load()
		},
	})
	if err != nil {
		logger.Error("heartbeat file problem", zap.String("path", conf.Common.HeartbeatFile), zap.Error(err))
	}
	if h == nil {
		return
	}
	app.heartbeat = h
	app.heartbeatMetrics.Store(h)
	if app.PromRegisterer != nil {
		app.heartbeatCollectorOnce.Do(func() {
			if err := app.PromRegisterer.Register(&heartbeatCollector{app: app}); err != nil {
				logger.Warn("heartbeat metrics not registered", zap.Error(err))
			}
		})
	}
	if w := h.Unclean(); w != nil {
		app.heartbeatUncleanStarts.Add(1)
		logger.Warn("previous run did not stop cleanly; cached points may be missing",
			zap.Time("last_alive", w.LastAlive),
			zap.Int("previous_pid", w.PreviousPID),
			zap.Int64("loss_window_seconds", w.LossWindowSeconds),
			zap.Time("restarted", w.Restarted),
			zap.String("backfill", "replica-backfill --since-unclean-start --heartbeat-file="+conf.Common.HeartbeatFile),
		)
	}
}

func (app *App) heartbeatReady() {
	if err := app.heartbeat.Ready(); err != nil {
		zapwriter.Logger("heartbeat").Warn("heartbeat write failed", zap.Error(err))
	}
}

// markHeartbeatClean records that every cached point is in the dump. It must
// run before a successor may start, so the successor reads a clean stop.
func (app *App) markHeartbeatClean() {
	if app.heartbeat == nil {
		return
	}
	if err := app.heartbeat.MarkClean(); err != nil {
		zapwriter.Logger("heartbeat").Warn("heartbeat clean mark failed", zap.Error(err))
	}
	app.heartbeat = nil
}

// stopHeartbeat leaves the file without the clean mark: a stop that does not
// dump the cache loses its points.
func (app *App) stopHeartbeat() {
	if app.heartbeat == nil {
		return
	}
	_ = app.heartbeat.Stop()
	app.heartbeat = nil
}

// heartbeatCollector exports the heartbeat for the backfill dashboard row,
// through the App registry so the [prometheus.labels] constants apply.
type heartbeatCollector struct{ app *App }

var (
	heartbeatUncleanDesc   = prometheus.NewDesc("heartbeat_unclean_starts_total", "Starts that found the previous run stopped without preserving its cache.", nil, nil)
	heartbeatGapStartDesc  = prometheus.NewDesc("heartbeat_gap_start_timestamp_seconds", "Start of the last recorded window of possibly lost points (last beat minus writeout-max-delay); 0 when none is recorded.", nil, nil)
	heartbeatGapEndDesc    = prometheus.NewDesc("heartbeat_gap_end_timestamp_seconds", "End of the last recorded gap window (input resumed after the restart, or now while starting); 0 when none is recorded.", nil, nil)
	heartbeatLastBeatDesc  = prometheus.NewDesc("heartbeat_last_beat_timestamp_seconds", "Time of the last successful heartbeat write.", nil, nil)
	heartbeatWriteErrsDesc = prometheus.NewDesc("heartbeat_write_errors_total", "Failed heartbeat file writes.", nil, nil)
)

func (c *heartbeatCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{heartbeatUncleanDesc, heartbeatGapStartDesc, heartbeatGapEndDesc, heartbeatLastBeatDesc, heartbeatWriteErrsDesc} {
		ch <- d
	}
}

func (c *heartbeatCollector) Collect(ch chan<- prometheus.Metric) {
	h := c.app.heartbeatMetrics.Load()
	if h == nil {
		return
	}
	var start, end float64
	if w := h.Recorded(); w != nil {
		from, until := w.Range(0, 0, time.Now())
		start, end = float64(from.Unix()), float64(until.Unix())
	}
	ch <- prometheus.MustNewConstMetric(heartbeatUncleanDesc, prometheus.CounterValue, float64(c.app.heartbeatUncleanStarts.Load()))
	ch <- prometheus.MustNewConstMetric(heartbeatGapStartDesc, prometheus.GaugeValue, start)
	ch <- prometheus.MustNewConstMetric(heartbeatGapEndDesc, prometheus.GaugeValue, end)
	ch <- prometheus.MustNewConstMetric(heartbeatLastBeatDesc, prometheus.GaugeValue, float64(h.LastBeat().UnixNano())/1e9)
	ch <- prometheus.MustNewConstMetric(heartbeatWriteErrsDesc, prometheus.CounterValue, float64(h.WriteErrors()))
}
