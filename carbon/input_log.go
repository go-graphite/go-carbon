package carbon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// inputLogConfig configures the continuous input log, which lets an unclean
// exit lose about one sync interval of input instead of the whole cache. Its
// settings take effect on restart only.
type inputLogConfig struct {
	Enabled      bool     `toml:"enabled"`
	Path         string   `toml:"path"`
	SyncInterval Duration `toml:"sync-interval"`
	SegmentSize  int64    `toml:"segment-size"`
	MaxAge       Duration `toml:"max-age"`
	TruncateLag  Duration `toml:"truncate-lag"`
}

func newInputLogConfig() inputLogConfig {
	return inputLogConfig{
		Path:         "/var/lib/graphite/input-log/",
		SyncInterval: Duration{time.Second},
		SegmentSize:  4 << 30,
		TruncateLag:  Duration{time.Minute},
	}
}

// minAutoMaxAge is the shortest max-age chosen by default.
const minAutoMaxAge = 30 * time.Minute

// inputLogMaxAge returns max-age, by default twice cache.writeout-max-delay
// and at least minAutoMaxAge: points of a sparse metric wait up to that delay
// for their writeout, and should not be logged again meanwhile.
func inputLogMaxAge(cfg *Config) time.Duration {
	if age := cfg.InputLog.MaxAge.Value(); age > 0 {
		return age
	}
	return max(2*cfg.Cache.WriteoutMaxDelay.Value(), minAutoMaxAge)
}

// minInputLogSegment leaves room for several of the largest frames.
const minInputLogSegment = 16 << 20

func validateInputLogConfig(cfg *Config) error {
	c := cfg.InputLog
	if !c.Enabled {
		return nil
	}
	switch {
	case c.Path == "":
		return errors.New("input-log.path is required")
	case cfg.Dump.Path != "" && filepath.Clean(c.Path) == filepath.Clean(cfg.Dump.Path):
		return errors.New("input-log.path must differ from dump.path")
	case c.SyncInterval.Value() <= 0:
		return errors.New("input-log.sync-interval must be positive")
	case c.SegmentSize < minInputLogSegment:
		return fmt.Errorf("input-log.segment-size must be at least %d", minInputLogSegment)
	case c.MaxAge.Value() < 0:
		return errors.New("input-log.max-age must not be negative")
	case inputLogMaxAge(cfg) < 2*c.SyncInterval.Value():
		return errors.New("input-log.max-age must be at least two sync intervals")
	case inputLogMaxAge(cfg) <= cfg.Cache.WriteoutMaxDelay.Value():
		return errors.New("input-log.max-age must exceed cache.writeout-max-delay, or every point waiting for its writeout is logged twice")
	case c.TruncateLag.Value() < 0:
		return errors.New("input-log.truncate-lag must not be negative")
	}
	return nil
}

// inputLogReloadError refuses a reload that changes the input log, which
// takes effect on restart only, or that lets points wait for their writeout
// longer than the running log's max-age.
func inputLogReloadError(old, next *Config) error {
	switch {
	case old.InputLog != next.InputLog:
		return errors.New("input-log settings require a restart")
	case old.InputLog.Enabled && next.Cache.WriteoutMaxDelay.Value() >= inputLogMaxAge(old):
		return fmt.Errorf("cache.writeout-max-delay must stay below the running input log's max-age of %v; restart to raise both", inputLogMaxAge(old))
	}
	return nil
}

// openInputLog runs before anything can reach the cache. A log that holds
// points to replay starts only after replayInputLog.
func (app *App) openInputLog(core *cache.Cache) error {
	conf := app.Config.InputLog
	if !conf.Enabled {
		return nil
	}
	copyRecovery := false
	if data, err := os.ReadFile(filepath.Join(conf.Path, inputlog.CopyMarkerName)); err == nil {
		copyRecovery = true
		var marker struct {
			Files []inputlog.CopyFile `json:"files"`
		}
		if err := json.Unmarshal(data, &marker); err != nil {
			return err
		}
		if err := inputlog.ValidateCopyFiles(conf.Path, marker.Files); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var observeCycle func(time.Duration)
	if app.Config.Prometheus.Enabled {
		m := app.inputLogMetrics()
		observeCycle = func(d time.Duration) { m.cycles.Observe(d.Seconds()) }
	}
	l, err := inputlog.Open(conf.Path, inputlog.Options{
		SyncInterval: conf.SyncInterval.Value(),
		SegmentSize:  conf.SegmentSize,
		MaxAge:       inputLogMaxAge(app.Config),
		TruncateLag:  conf.TruncateLag.Value(),
		Logger:       zapwriter.Logger("input-log"),
		ObserveCycle: observeCycle,
	})
	if err != nil {
		return fmt.Errorf("open input log: %w", err)
	}
	zapwriter.Logger("input-log").Info("input log opened", zap.String("path", conf.Path), zap.Int64("segment_size", conf.SegmentSize),
		zap.Duration("max_age", inputLogMaxAge(app.Config)), zap.Bool("needs_replay", l.NeedsReplay()))
	app.inputLog.Store(l)
	core.EnableInputLog()
	if !l.NeedsReplay() {
		l.Start(core, cache.ShardCount)
		if copyRecovery {
			return l.FinishCopyReplay()
		}
	}
	return nil
}

// replayInputLog loads the points a crash left in the log. It runs before
// receivers and legacy dump restore, after a pending checkpoint is attached:
// logged input is newer than any checkpoint, whose history each replayed
// metric claims first, and older than dump files a crash left unrestored.
// Replayed points precede live points in each cache item, so they need not
// drain before receivers open, even for compressed whisper.
func (app *App) replayInputLog(core *cache.Cache) error {
	l := app.inputLog.Load()
	if l == nil || !l.NeedsReplay() {
		return nil
	}
	app.restart.noteReplayed()
	logger := zapwriter.Logger("input-log")
	logger.Info("replaying input log before starting receivers", zap.String("path", app.Config.InputLog.Path))
	draining := app.Persister != nil
	stats, err := l.Replay(func(p *points.Points) {
		// Like tail replay: these points were accepted already, so no live
		// quota applies, but recovery must leave room for persistence.
		for draining && !core.HasRecoveryCapacity(len(p.Data)) {
			time.Sleep(time.Millisecond)
		}
		core.AddRecovered(p)
	})
	logger.Info("input log replayed", zap.Int64("points", stats.Points), zap.Int64("frames", stats.Frames),
		zap.Int64("bytes", stats.Bytes), zap.Int64("corrupt_frames", stats.Corrupt), zap.Duration("runtime", stats.Duration), zap.Error(err))
	if err != nil {
		return fmt.Errorf("replay input log: %w", err)
	}
	strictCopy := false
	if _, err := os.Stat(filepath.Join(app.Config.InputLog.Path, inputlog.CopyMarkerName)); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		if stats.Corrupt != 0 {
			return fmt.Errorf("copied input log contains %d corrupt frames; refusing incomplete replay", stats.Corrupt)
		}
		strictCopy = true
	}
	l.Start(core, cache.ShardCount)
	if strictCopy {
		// The first target cycle records recovery's replay boundary before the
		// original copied segments may be recycled. Subsequent starts use the
		// target's normal log recovery rather than the one-time copy file list.
		return l.FinishCopyReplay()
	}
	return nil
}

// syncInputLog makes restored points durable before their dump file goes.
func (app *App) syncInputLog() error {
	if l := app.inputLog.Load(); l != nil {
		return l.Sync()
	}
	return nil
}

// closeInputLog makes every accepted point durable and keeps the log for the
// next start. Input must be stopped. Assumes we are holding app.Lock().
func (app *App) closeInputLog() {
	l := app.inputLog.Swap(nil)
	if l == nil {
		return
	}
	if err := l.Close(); err != nil {
		zapwriter.Logger("input-log").Error("input log not closed cleanly", zap.Error(err))
	}
	if app.Cache != nil {
		app.Cache.DisableInputLog()
	}
}

// retireInputLog deletes the log once a dump durably holds every point it
// protects, so the next start does not replay them again. Assumes we are
// holding app.Lock().
func (app *App) retireInputLog(logger *zap.Logger) {
	l := app.inputLog.Swap(nil)
	if l == nil {
		return
	}
	if err := l.Retire(); err != nil {
		// Replaying the remaining segments only repeats dumped points.
		logger.Warn("input log not retired after dump", zap.Error(err))
	}
	app.Cache.DisableInputLog()
}

type inputLogStats struct{ log *inputlog.Log }

func (s inputLogStats) Stat(send helper.StatCallback) { s.log.Stat(send) }

// inputLogCollector exports the input log's counters at scrape time. It is
// registered once, with the first log; scrapes read whichever log is open.
type inputLogCollector struct {
	app    *App
	cycles prometheus.Histogram
}

var inputLogDescs = struct {
	bytes, points, syncCount, segments, disk, oldest, relogged, unlogged, errors, replayed, replay, cycleCount, stages *prometheus.Desc
}{
	cycleCount: prometheus.NewDesc("input_log_cycles_total", "Input log group commit cycles, one per sync interval or forced sync.", nil, nil),
	stages: prometheus.NewDesc("input_log_stage_seconds_total", "Seconds input log cycles spent in each stage; collect, encode and compress are summed across encoding workers.",
		[]string{"stage"}, nil),
	bytes:     prometheus.NewDesc("input_log_bytes_written_total", "Bytes the input log wrote, frames and segment headers.", nil, nil),
	points:    prometheus.NewDesc("input_log_points_written_total", "Points in frames the input log wrote.", nil, nil),
	syncCount: prometheus.NewDesc("input_log_syncs_total", "Data syncs of input log segments.", nil, nil),
	segments:  prometheus.NewDesc("input_log_segments", "Input log segments in use.", nil, nil),
	disk:      prometheus.NewDesc("input_log_disk_bytes", "Size of the input log's segment and spare files.", nil, nil),
	oldest:    prometheus.NewDesc("input_log_oldest_unpersisted_age_seconds", "Age of the oldest logged cycle that still holds a point not yet persisted.", nil, nil),
	relogged:  prometheus.NewDesc("input_log_relogged_points_total", "Points logged again because they stayed unpersisted longer than max-age.", nil, nil),
	unlogged:  prometheus.NewDesc("input_log_unlogged_points_total", "Points cached but not logged because a shard's log buffer was full.", nil, nil),
	errors:    prometheus.NewDesc("input_log_write_errors_total", "Input log cycles whose points could not be made durable.", nil, nil),
	replayed:  prometheus.NewDesc("input_log_replayed_points_total", "Points replayed from the input log at start.", nil, nil),
	replay:    prometheus.NewDesc("input_log_replay_seconds", "Duration of the last input log replay.", nil, nil),
}

func (c *inputLogCollector) Describe(ch chan<- *prometheus.Desc) {
	d := inputLogDescs
	for _, desc := range []*prometheus.Desc{d.bytes, d.points, d.syncCount, d.segments, d.disk, d.oldest, d.relogged, d.unlogged, d.errors, d.replayed, d.replay, d.cycleCount, d.stages} {
		ch <- desc
	}
}

func (c *inputLogCollector) Collect(ch chan<- prometheus.Metric) {
	l := c.app.inputLog.Load()
	if l == nil {
		return
	}
	m, d := l.Metrics(), inputLogDescs
	counter := func(desc *prometheus.Desc, v int64) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(v))
	}
	gauge := func(desc *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v)
	}
	counter(d.bytes, m.BytesWritten)
	counter(d.points, m.PointsWritten)
	counter(d.syncCount, m.Syncs)
	counter(d.relogged, m.Relogged)
	counter(d.unlogged, m.Unlogged)
	counter(d.errors, m.WriteErrors)
	counter(d.replayed, m.ReplayedPoints)
	gauge(d.segments, float64(m.Segments))
	gauge(d.disk, float64(m.DiskBytes))
	gauge(d.oldest, m.OldestUnpersisted.Seconds())
	gauge(d.replay, m.ReplayDuration.Seconds())
	counter(d.cycleCount, m.Cycles)
	for i, name := range inputlog.StageNames {
		ch <- prometheus.MustNewConstMetric(d.stages, prometheus.CounterValue, m.Stages[i].Seconds(), name)
	}
}

// inputLogMetrics registers the input log's metrics on first use.
func (app *App) inputLogMetrics() *inputLogCollector {
	app.inputLogMetricsOnce.Do(func() {
		app.inputLogCollector = &inputLogCollector{app: app, cycles: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "input_log_sync_seconds_exp",
			Help:    "Duration of input log cycles that committed points, from collecting them to their sync (exponential buckets)",
			Buckets: prometheus.ExponentialBuckets(time.Millisecond.Seconds(), 2.0, 20),
		})}
		app.PromRegisterer.MustRegister(app.inputLogCollector, app.inputLogCollector.cycles)
	})
	return app.inputLogCollector
}
