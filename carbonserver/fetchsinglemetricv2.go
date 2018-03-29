package carbonserver

import (
	"errors"
	"math"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

func (listener *CarbonserverListener) fetchSingleMetricV2(metric string, fromTime, untilTime int32) (*protov2.FetchResponse, error) {
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

	fromTime = int32(m.Timeseries.FromTime())
	untilTime = int32(m.Timeseries.UntilTime())
	step := int32(m.Timeseries.Step())

	waitTime := uint64(time.Since(m.DiskStartTime).Nanoseconds())
	atomic.AddUint64(&listener.metrics.DiskWaitTimeNS, waitTime)
	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(values)))

	response := protov2.FetchResponse{
		Name:      metric,
		StartTime: fromTime,
		StopTime:  untilTime,
		StepTime:  step,
		Values:    make([]float64, len(values)),
		IsAbsent:  make([]bool, len(values)),
	}

	for i, p := range values {
		if math.IsNaN(p) {
			response.Values[i] = 0
			response.IsAbsent[i] = true
		} else {
			response.Values[i] = p
			response.IsAbsent[i] = false
		}
	}

	if m.CacheData != nil {
		atomic.AddUint64(&listener.metrics.CacheRequestsTotal, 1)
		cacheStartTime := time.Now()
		pointsFetchedFromCache := 0
		for _, item := range m.CacheData {
			ts := int32(item.Timestamp) - int32(item.Timestamp)%step
			if ts < fromTime || ts >= untilTime {
				continue
			}
			pointsFetchedFromCache++
			index := (ts - fromTime) / step
			response.Values[index] = item.Value
			response.IsAbsent[index] = false
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
