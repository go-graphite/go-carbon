package carbonserver

import (
	"context"
	"errors"
	"fmt"
	_ "net/http/pprof" // skipcq: GO-S2108
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-whisper"

	"go.uber.org/zap"
)

type Metadata struct {
	ConsolidationFunc string
	XFilesFactor      float32
}

type metricFromDisk struct {
	DiskStartTime time.Time
	CacheData     []points.Point
	Timeseries    timeseries
	Metadata      Metadata
}

// timeseries is the narrow response shape shared by classic Whisper and the
// embedded shared store. It prevents the storage engine leaking into render.
type timeseries interface {
	Values() []float64
	FromTime() int
	UntilTime() int
	Step() int
}

type metricStoreSeries struct{ *store.Series }

func (s metricStoreSeries) FromTime() int  { return s.Series.FromTime }
func (s metricStoreSeries) UntilTime() int { return s.Series.UntilTime }
func (s metricStoreSeries) Step() int      { return s.Series.Step }
func (s metricStoreSeries) Values() []float64 {
	return s.Series.Values
}

func (listener *CarbonserverListener) fetchFromDisk(metric string, fromTime, untilTime int32) (*metricFromDisk, error) {
	if metricStore := listener.getMetricStore(); metricStore != nil {
		return listener.fetchFromMetricStore(metricStore, metric, fromTime, untilTime)
	}
	var step int32

	// We need to obtain the metadata from whisper file anyway.
	path := listener.whisperData + "/" + strings.ReplaceAll(metric, ".", "/") + ".wsp"
	w, err := whisper.OpenWithOptions(path, &whisper.Options{
		FLock: listener.flock, FlockType: syscall.LOCK_SH,
	})
	if err != nil {
		// the FE/carbonzipper often requests metrics we don't have
		// We shouldn't really see this anymore -- expandGlobs() should filter them out
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		listener.logger.Error("open error", zap.String("path", path), zap.Error(err))
		return nil, err
	}

	logger := listener.logger.With(
		zap.String("path", path),
		zap.Int("fromTime", int(fromTime)),
		zap.Int("untilTime", int(untilTime)),
	)

	retentions := w.Retentions()
	now := int32(time.Now().Unix())
	diff := now - fromTime
	var bestStep int32 = -1
	for _, retention := range retentions {
		if bestStep == -1 {
			bestStep = int32(retention.SecondsPerPoint())
		}
		if int32(retention.MaxRetention()) >= diff {
			step = int32(retention.SecondsPerPoint())
			break
		}
	}
	if bestStep == -1 {
		logger.Error("metric file %s doesn't have archives info", zap.String("metric", metric))
		return nil, errors.New("corrupt metric file")
	}

	if step == 0 {
		maxRetention := int32(retentions[len(retentions)-1].MaxRetention())
		if now-maxRetention > untilTime {
			logger.Warn("can't find proper archive for the request")
			return nil, errors.New("Can't find proper archive")
		}
		logger.Debug("can't find archive that contains full set of data, using the least precise one")
		step = maxRetention
	}

	res := &metricFromDisk{
		Metadata: Metadata{
			ConsolidationFunc: titleizeAggrMethod(w.AggregationMethod().String()),
			XFilesFactor:      w.XFilesFactor(),
		},
	}
	if step != bestStep {
		logger.Debug("cache is not supported for this query (required step != best step)",
			zap.Int("step", int(step)),
			zap.Int("bestStep", int(bestStep)),
		)
	} else {
		// query cache
		cacheStartTime := time.Now()
		res.CacheData = listener.cacheGet(metric)
		waitTime := time.Since(cacheStartTime)
		atomic.AddUint64(&listener.metrics.CacheWaitTimeFetchNS, uint64(waitTime.Nanoseconds()))
		listener.prometheus.cacheDuration("wait", waitTime)
	}

	logger.Debug("fetching disk metric")
	atomic.AddUint64(&listener.metrics.DiskRequests, 1)
	listener.prometheus.diskRequest()

	res.DiskStartTime = time.Now()
	points, err := w.Fetch(int(fromTime), int(untilTime))
	w.Close()
	if err != nil {
		logger.Warn("failed to fetch points", zap.Error(err))
		return nil, errors.New("failed to fetch points")
	}

	// Should never happen, because we have a check for proper archive now
	if points == nil {
		logger.Warn("metric time range not found")
		return nil, errors.New("time range not found")
	}

	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	listener.prometheus.returnedMetric()

	waitTime := time.Since(res.DiskStartTime)
	atomic.AddUint64(&listener.metrics.DiskWaitTimeNS, uint64(waitTime.Nanoseconds()))
	listener.prometheus.diskWaitDuration(waitTime)

	values := points.Values()
	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(values)))
	listener.prometheus.returnedPoint(len(values))

	res.Timeseries = points

	return res, nil
}

func (listener *CarbonserverListener) fetchFromMetricStore(metricStore *store.Store, metric string, fromTime, untilTime int32) (*metricFromDisk, error) {
	started := time.Now()
	series, err := metricStore.Fetch(context.Background(), metric, int(fromTime), int(untilTime))
	if errors.Is(err, store.ErrNotFound) {
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		return nil, fmt.Errorf("open shared metric %q: %w", metric, os.ErrNotExist)
	}
	if err != nil {
		return nil, err
	}
	if series == nil {
		return nil, errors.New("time range not found")
	}
	metadata := series.Metadata

	res := &metricFromDisk{
		DiskStartTime: started,
		Timeseries:    metricStoreSeries{series},
		Metadata: Metadata{
			ConsolidationFunc: titleizeAggrMethod(metadata.AggregationMethod.String()),
			XFilesFactor:      metadata.XFilesFactor,
		},
	}
	if len(metadata.Retentions) > 0 && series.Step == metadata.Retentions[0].SecondsPerPoint() {
		cacheStarted := time.Now()
		res.CacheData = listener.cacheGet(metric)
		wait := time.Since(cacheStarted)
		atomic.AddUint64(&listener.metrics.CacheWaitTimeFetchNS, uint64(wait.Nanoseconds()))
		listener.prometheus.cacheDuration("wait", wait)
	}
	atomic.AddUint64(&listener.metrics.DiskRequests, 1)
	listener.prometheus.diskRequest()
	wait := time.Since(started)
	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	listener.prometheus.returnedMetric()
	atomic.AddUint64(&listener.metrics.DiskWaitTimeNS, uint64(wait.Nanoseconds()))
	listener.prometheus.diskWaitDuration(wait)
	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(series.Values)))
	listener.prometheus.returnedPoint(len(series.Values))
	return res, nil
}
