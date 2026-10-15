package carbonserver

import (
	"errors"
	"math"
	"os"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-graphite/go-carbon/points"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

type response struct {
	Name              string
	StartTime         int64
	StopTime          int64
	StepTime          int64
	Values            []float64
	PathExpression    string
	ConsolidationFunc string
	XFilesFactor      float32
	RequestStartTime  int64
	RequestStopTime   int64
}

func (r response) enrichFromCache(listener *CarbonserverListener, cacheData []points.Point) {
	if cacheData == nil {
		return
	}

	atomic.AddUint64(&listener.metrics.CacheRequestsTotal, 1)
	cacheStartTime := time.Now()
	pointsFetchedFromCache := 0
	// extend values array if needed
	max_index := (r.StopTime - r.StartTime) / r.StepTime
	start_index := int64(len(r.Values))
	if start_index < max_index {
		for i := start_index; i < max_index; i++ {
			// math.NaN is not working here
			// values will be replaced when merging with cache
			r.Values = append(r.Values, 0)
		}
	}
	for _, item := range cacheData {
		ts := int64(item.Timestamp) - int64(item.Timestamp)%r.StepTime
		if ts < r.StartTime || ts >= r.StopTime {
			continue
		}
		pointsFetchedFromCache++
		index := (ts - r.StartTime) / r.StepTime
		if index >= 0 && index < int64(len(r.Values)) {
			r.Values[index] = item.Value
		}
	}
	waitTime := time.Since(cacheStartTime)
	atomic.AddUint64(&listener.metrics.CacheWorkTimeNS, uint64(waitTime.Nanoseconds()))
	listener.prometheus.cacheDuration("work", waitTime)

	listener.prometheus.cacheRequest("metric", pointsFetchedFromCache > 0)
	if pointsFetchedFromCache > 0 {
		atomic.AddUint64(&listener.metrics.CacheHit, 1)
	} else {
		atomic.AddUint64(&listener.metrics.CacheMiss, 1)
	}
}

func (r response) proto2() *protov2.FetchResponse {
	resp := &protov2.FetchResponse{
		Name:      r.Name,
		StartTime: int32(r.StartTime),
		StopTime:  int32(r.StopTime),
		StepTime:  int32(r.StepTime),
		Values:    make([]float64, len(r.Values)),
		IsAbsent:  make([]bool, len(r.Values)),
	}

	for i, p := range r.Values {
		if math.IsNaN(p) {
			resp.Values[i] = 0
			resp.IsAbsent[i] = true
		} else {
			resp.Values[i] = p
		}
	}

	return resp
}

func (r response) proto3() *protov3.FetchResponse {
	return &protov3.FetchResponse{
		Name:              r.Name,
		StartTime:         r.StartTime,
		StopTime:          r.StopTime,
		StepTime:          r.StepTime,
		Values:            r.Values,
		PathExpression:    r.PathExpression,
		ConsolidationFunc: r.ConsolidationFunc,
		XFilesFactor:      r.XFilesFactor,
		RequestStartTime:  r.RequestStartTime,
		RequestStopTime:   r.RequestStopTime,
	}
}

func (listener *CarbonserverListener) fetchSingleMetric(metric string, pathExpression string, fromTime, untilTime int32) (response, error) {
	// Encode the request fields only for messages that are logged: a With
	// logger per fetched series is a measurable share of render allocations.
	log := func(level zapcore.Level, msg string, fields ...zap.Field) {
		if ce := listener.logger.Check(level, msg); ce != nil {
			ce.Write(append([]zap.Field{
				zap.String("metric", metric),
				zap.Int("fromTime", int(fromTime)),
				zap.Int("untilTime", int(untilTime)),
			}, fields...)...)
		}
	}
	m, err := listener.fetchFromDisk(metric, fromTime, untilTime)
	switch {
	case err == nil:
		// Should never happen, because we have a check for proper archive now
		if m.Timeseries == nil {
			log(zap.WarnLevel, "metric time range not found")
			return response{}, errors.New("time range not found")
		}

		values := m.Timeseries.Values()
		from := int64(m.Timeseries.FromTime())
		until := int64(m.Timeseries.UntilTime())
		step := int64(m.Timeseries.Step())

		resp := response{
			Name:              metric,
			StartTime:         from,
			StopTime:          until,
			StepTime:          step,
			Values:            values,
			PathExpression:    pathExpression,
			ConsolidationFunc: m.Metadata.ConsolidationFunc,
			XFilesFactor:      m.Metadata.XFilesFactor,
			RequestStartTime:  int64(fromTime),
			RequestStopTime:   int64(untilTime),
		}

		resp.enrichFromCache(listener, m.CacheData)

		if listener.logger.Core().Enabled(zap.DebugLevel) {
			log(zap.DebugLevel, "fetched", zap.Any("response", resp))
		}

		return resp, nil
	case os.IsNotExist(err) && (listener.cacheGetRecentMetrics != nil || listener.realtimeIndex > 0):
		// Failed to fetch from disk, try to fetch from cache
		resp := response{
			Name:           metric,
			PathExpression: pathExpression,
		}
		cacheData, cerr := listener.fetchFromCache(metric, fromTime, untilTime, &resp)
		if cerr != nil {
			log(zap.WarnLevel, "metric not found even in Cache", zap.Error(err))
			// Metric has no Whisper file and/or has no datapoints in Cache
			return response{}, cerr
		}
		resp.enrichFromCache(listener, cacheData)
		if listener.logger.Core().Enabled(zap.DebugLevel) {
			log(zap.DebugLevel, "fetched cache-only", zap.Any("response", resp))
		}

		return resp, nil
	default:
		log(zap.WarnLevel, "failed to fetch points", zap.Error(err))
		return response{}, err
	}
}
