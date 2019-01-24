package carbonserver

import (
	"errors"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-whisper"
	"github.com/lomik/go-carbon/points"
)

type Metadata struct {
	ConsolidationFunc string
	XFilesFactor      float32
}

type metricFromDisk struct {
	DiskStartTime time.Time
	CacheData     []points.Point
	Timeseries    *whisper.TimeSeries
	Metadata      Metadata
}

func (listener *CarbonserverListener) fetchFromDisk(metric string, fromTime, untilTime int32) (*metricFromDisk, error) {
	var step int32

	// We need to obtain the metadata from whisper file anyway.
	path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.OpenWithOptions(path, &whisper.Options{
		FLock: listener.flock,
	})
	if err != nil {
		// the FE/carbonzipper often requests metrics we don't have
		// We shouldn't really see this any more -- expandGlobs() should filter them out
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
	bestStep := int32(retentions[0].SecondsPerPoint())
	for _, retention := range retentions {
		if int32(retention.MaxRetention()) >= diff {
			step = int32(retention.SecondsPerPoint())
			break
		}
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
			ConsolidationFunc: w.AggregationMethod(),
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
