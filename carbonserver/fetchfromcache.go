package carbonserver

import (
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/go-carbon/points"
)

// https://github.com/golang/go/issues/448
func mod(a, b int32) int32 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// from go-graphite/go-whisper
func interval(time, secondsPerPoint int32) int32 {
	return time - mod(time, secondsPerPoint) + secondsPerPoint
}

func (listener *CarbonserverListener) fetchFromCache(metric string, fromTime, untilTime int32, resp *response) ([]points.Point, error) {
	var step, i int32
	path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"

	logger := listener.logger.With(
		zap.String("path", path),
		zap.Int("fromTime", int(fromTime)),
		zap.Int("untilTime", int(untilTime)),
	)

	// query cache
	cacheStartTime := time.Now()
	cacheData := listener.cacheGet(metric)
	waitTime := time.Since(cacheStartTime)
	atomic.AddUint64(&listener.metrics.CacheWaitTimeFetchNS, uint64(waitTime.Nanoseconds()))
	listener.prometheus.cacheDuration("wait", waitTime)

	if cacheData == nil {
		// we really can't find this metric
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		listener.logger.Error("No data (wsp file nor cache) exists for the metric", zap.String("path", path))
		return nil, errors.New("No data (wsp file nor cache) exists for the metric")
	}

	logger.Debug("fetching cache only metric")

	// retentions, aggMethod, xFilesFactor from matched schema/agg
	retentionStep, ok := listener.whisperGetConfig.MetricRetentionPeriod(metric)
	if !ok {
		return nil, errors.New("no retention schemas defined")
	}
	aggrName, aggrXFilesFact, ok := listener.whisperGetConfig.MetricAggrConf(metric)
	if !ok {
		return nil, errors.New("no storage aggregation defined")
	}

	step = int32(retentionStep)

	// create dummy values slice which will be
	// popolated with real values from cache in
	// enrichFromCache in fetchsinglemetric
	var values []float64
	for i = 0; i < (untilTime-fromTime)/step; i++ {
		values = append(values, math.NaN())
	}

	resp.StartTime = int64(interval(fromTime, step))
	resp.StopTime = int64(interval(untilTime, step))
	resp.StepTime = int64(retentionStep)
	resp.Values = values
	resp.ConsolidationFunc = aggrName
	resp.XFilesFactor = float32(aggrXFilesFact)

	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	listener.prometheus.returnedMetric()

	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(values)))
	listener.prometheus.returnedPoint(len(values))

	return cacheData, nil
}
