package carbonserver

import (
	"errors"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func (listener *CarbonserverListener) fetchSingleMetricV3(metric string, pathExpression string, fromTime, untilTime int32) (*protov3.FetchResponse, error) {
	logger := listener.logger.With(
		zap.String("metric", metric),
		zap.Int("fromTime", int(fromTime)),
		zap.Int("untilTime", int(untilTime)),
	)
	m, err := listener.fetchFromDisk(metric, fromTime, untilTime)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		logger.Warn("failed to fetch points", zap.Error(err))
		return nil, err
	}

	// Should never happen, because we have a check for proper archive now
	if m.Timeseries == nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		logger.Warn("metric time range not found")
		return nil, errors.New("time range not found")
	}
	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	values := m.Timeseries.Values()

	from := int64(m.Timeseries.FromTime())
	until := int64(m.Timeseries.UntilTime())
	step := int64(m.Timeseries.Step())

	waitTime := uint64(time.Since(m.DiskStartTime).Nanoseconds())
	atomic.AddUint64(&listener.metrics.DiskWaitTimeNS, waitTime)
	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(values)))

	response := protov3.FetchResponse{
		Name:              metric,
		StartTime:         from,
		StopTime:          until,
		StepTime:          step,
		Values:            values,
		PathExpression:    pathExpression,
		ConsolidationFunc: m.Metadata.ConsolidationFunc,
		XFilesFactor:      m.Metadata.XFilesFactor,
	}

	if m.CacheData != nil {
		atomic.AddUint64(&listener.metrics.CacheRequestsTotal, 1)
		cacheStartTime := time.Now()
		pointsFetchedFromCache := 0
		for _, item := range m.CacheData {
			ts := int64(item.Timestamp) - int64(item.Timestamp)%step
			if ts < from || ts >= until {
				continue
			}
			pointsFetchedFromCache++
			index := (ts - from) / step
			response.Values[index] = item.Value
		}
		waitTime := uint64(time.Since(cacheStartTime).Nanoseconds())
		atomic.AddUint64(&listener.metrics.CacheWorkTimeNS, waitTime)
		if pointsFetchedFromCache > 0 {
			atomic.AddUint64(&listener.metrics.CacheHit, 1)
		} else {
			atomic.AddUint64(&listener.metrics.CacheMiss, 1)
		}
	}

	logger.Debug("fetched",
		zap.Any("response", response),
	)
	return &response, nil
}
