package carbon

import (
	"math"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/cache"
)

// defaultCacheMemoryShare reserves 15% of total memory for estimated cache bytes.
const defaultCacheMemoryShare = 0.15

// autoCacheBytesPerPoint estimates point cost until the cache can be measured.
const autoCacheBytesPerPoint = 3.0

// autoCacheMinMeasuredPoints is the smallest cache whose estimated bytes
// per point is trusted; small caches are dominated by names and headers.
const autoCacheMinMeasuredPoints = 1_000_000

// cacheLimit is the resolved automatic write-cache limit, for metrics.
type cacheLimit struct {
	auto          bool
	share         float64
	totalBytes    uint64
	limitBytes    uint64
	bytesPerPoint float64
	points        uint64
}

// autoCacheLimit derives a point limit from total memory: share of total,
// divided by bytes per point (fixed, else measured, else the fallback).
// MemAvailable is deliberately not used: it moves with go-carbon's own
// usage, so a limit based on it chases itself.
func autoCacheLimit(total uint64, share, fixedBytesPerPoint float64, cacheBytes, cachePoints int64) cacheLimit {
	bpp := fixedBytesPerPoint
	if bpp <= 0 {
		bpp = autoCacheBytesPerPoint
		if cachePoints >= autoCacheMinMeasuredPoints && cacheBytes > 0 {
			bpp = float64(cacheBytes) / float64(cachePoints)
		}
	}
	limitBytes := uint64(float64(total) * share)
	points := uint64(math.Min(float64(limitBytes)/bpp, float64(math.MaxInt64)))
	return cacheLimit{auto: true, share: share, totalBytes: total, limitBytes: limitBytes, bytesPerPoint: bpp, points: max(points, 1)}
}

// resolveCacheLimit sets cfg.Cache.MaxSize from total memory when the config
// file leaves max-size unset. It runs at startup and on reload only, so every
// reader of the configured max-size sees the derived value. core may be nil.
func (app *App) resolveCacheLimit(cfg *Config, core *cache.Cache) {
	if !cfg.Cache.autoMaxSize {
		app.cacheLimit.Store(&cacheLimit{points: cfg.Cache.MaxSize, share: cfg.Cache.MemoryShare})
		return
	}
	logger := zapwriter.Logger("cache")
	total, err := availableMemory()
	if err != nil || total == 0 {
		logger.Error("automatic cache max-size unavailable; keeping the default", zap.Uint64("max_size", cfg.Cache.MaxSize), zap.Error(err))
		app.cacheLimit.Store(&cacheLimit{points: cfg.Cache.MaxSize, share: cfg.Cache.MemoryShare})
		return
	}
	var bytes, points int64
	if core != nil {
		bytes, points = core.EstimatedBytes(), core.Size()
	}
	limit := autoCacheLimit(total, cfg.Cache.MemoryShare, cfg.Cache.BytesPerPoint, bytes, points)
	cfg.Cache.MaxSize = limit.points
	app.cacheLimit.Store(&limit)
	logger.Info("automatic cache max-size resolved",
		zap.Float64("memory_share", limit.share), zap.Uint64("total_memory_bytes", limit.totalBytes),
		zap.Uint64("limit_bytes", limit.limitBytes), zap.Float64("bytes_per_point", limit.bytesPerPoint),
		zap.Bool("bytes_per_point_measured", cfg.Cache.BytesPerPoint <= 0 && points >= autoCacheMinMeasuredPoints),
		zap.Uint64("max_size", limit.points))
}

var (
	cacheLimitAutoDesc   = prometheus.NewDesc("cache_limit_auto", "One when max-size is derived from total memory; zero when the config sets it.", nil, nil)
	cacheLimitShareDesc  = prometheus.NewDesc("cache_limit_memory_share", "Fraction of total memory given to the write cache by the automatic max-size.", nil, nil)
	cacheLimitBytesDesc  = prometheus.NewDesc("cache_limit_bytes", "Write-cache byte budget behind the automatic max-size: total memory times memory-share.", nil, nil)
	cacheLimitPointsDesc = prometheus.NewDesc("cache_limit_points", "Effective write-cache max-size in points, derived or configured.", nil, nil)
	cacheLimitBPPDesc    = prometheus.NewDesc("cache_limit_bytes_per_point", "Bytes per point the automatic max-size divided by.", nil, nil)
	checkpointSaveFailed = prometheus.NewDesc("store_metadata_checkpoint_save_failures_total", "Stop-time metadata cache checkpoint saves that failed or ran out of time; the successor then starts with a cold metadata cache.", nil, nil)
)

// cacheLimitCollector exports the resolved cache limit and checkpoint save
// failures.
type cacheLimitCollector struct{ app *App }

func (c *cacheLimitCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{cacheLimitAutoDesc, cacheLimitShareDesc, cacheLimitBytesDesc, cacheLimitPointsDesc, cacheLimitBPPDesc, checkpointSaveFailed} {
		ch <- d
	}
}

func (c *cacheLimitCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(checkpointSaveFailed, prometheus.CounterValue, float64(c.app.metadataCheckpointSaveFailures.Load()))
	limit := c.app.cacheLimit.Load()
	if limit == nil {
		return
	}
	auto := 0.0
	if limit.auto {
		auto = 1
	}
	ch <- prometheus.MustNewConstMetric(cacheLimitAutoDesc, prometheus.GaugeValue, auto)
	ch <- prometheus.MustNewConstMetric(cacheLimitShareDesc, prometheus.GaugeValue, limit.share)
	ch <- prometheus.MustNewConstMetric(cacheLimitBytesDesc, prometheus.GaugeValue, float64(limit.limitBytes))
	ch <- prometheus.MustNewConstMetric(cacheLimitPointsDesc, prometheus.GaugeValue, float64(limit.points))
	ch <- prometheus.MustNewConstMetric(cacheLimitBPPDesc, prometheus.GaugeValue, limit.bytesPerPoint)
}

// registerCacheLimitMetrics registers the collector once per process.
func (app *App) registerCacheLimitMetrics() {
	if app.PromRegisterer == nil {
		return
	}
	app.cacheLimitCollectorOnce.Do(func() {
		if err := app.PromRegisterer.Register(&cacheLimitCollector{app: app}); err != nil {
			zapwriter.Logger("cache").Warn("cache limit metrics not registered", zap.Error(err))
		}
	})
}
