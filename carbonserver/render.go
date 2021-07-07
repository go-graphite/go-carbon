package carbonserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	pickle "github.com/lomik/og-rek"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
)

type fetchResponse struct {
	data           []byte
	contentType    string
	metricsFetched int
	valuesFetched  int
	memoryUsed     int
	metrics        []string
}

type target struct {
	Name           string
	PathExpression string
}

type timeRange struct {
	from  int32
	until int32
}

func getTargetNames(targets map[timeRange][]target) []string {
	c := 0
	for _, v := range targets {
		c += len(v)
	}

	names := make([]string, 0, c)
	for _, v := range targets {
		for _, t := range v {
			names = append(names, t.Name)
		}
	}

	return names
}

func stringToInt32(t string) (int32, error) {
	i, err := strconv.Atoi(t)

	if err != nil {
		return int32(i), err
	}

	return int32(i), nil
}

func getFormat(req *http.Request) (responseFormat, error) {
	format := req.FormValue("format")
	if format == "" {
		format = "json"
	}

	for _, accept := range req.Header["Accept"] {
		if accept == httpHeaders.ContentTypeCarbonAPIv3PB {
			format = "carbonapi_v3_pb"
			break
		}
	}

	formatCode, ok := knownFormats[format]
	if !ok {
		return formatCode, fmt.Errorf("Unknown format")
	}

	return formatCode, nil
}

func getTargets(req *http.Request, format responseFormat) (map[timeRange][]target, error) {
	targets := make(map[timeRange][]target)

	switch format {
	case protoV3Format:
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return targets, fmt.Errorf("error reading body: %s", err.Error())
		}

		var pv3Request protov3.MultiFetchRequest
		if err := pv3Request.Unmarshal(body); err != nil {
			return targets, fmt.Errorf("invalid payload: %s", err.Error())
		}

		for _, t := range pv3Request.Metrics {
			tr := timeRange{
				from:  int32(t.StartTime),
				until: int32(t.StopTime),
			}
			targets[tr] = append(targets[tr], target{Name: t.Name, PathExpression: t.PathExpression})
		}

	default:
		from, err := stringToInt32(req.FormValue("from"))
		if err != nil {
			return targets, fmt.Errorf("invalid 'from' time")
		}

		until, err := stringToInt32(req.FormValue("until"))
		if err != nil {
			return targets, fmt.Errorf("invalid 'until' time")
		}

		tr := timeRange{from: from, until: until}

		for _, t := range req.Form["target"] {
			targets[tr] = append(targets[tr], target{Name: t, PathExpression: t})
		}
	}

	return targets, nil
}

func (listener *CarbonserverListener) renderHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /render/?target=the.metric.Name&format=pickle&from=1396008021&until=1396022421
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.RenderRequests, 1)

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "render"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
	))

	format, err := getFormat(req)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, fmt.Sprintf("Bad request: %s", err), http.StatusBadRequest)
		return
	}

	accessLogger = accessLogger.With(
		zap.String("format", format.String()),
	)

	targets, err := getTargets(req, format)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, fmt.Sprintf("Bad request: %s", err), http.StatusBadRequest)
		return
	}

	tgs := getTargetNames(targets)
	accessLogger = accessLogger.With(
		zap.Strings("targets", tgs),
	)

	logger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "render"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.Strings("targets", tgs),
		zap.String("format", format.String()),
	))

	// Make sure we log which metric caused a panic()
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic recovered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
			accessLogger.Error("fetch failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "panic during serving the request"),
				zap.Stack("stack"),
				zap.Any("error", r),
				zap.Int("http_code", http.StatusInternalServerError),
			)
			http.Error(wr, "Panic occured, see logs for more information", http.StatusInternalServerError)
		}
	}()

	response, fromCache, err := listener.fetchWithCache(ctx, logger, format, targets)

	wr.Header().Set("Content-Type", response.contentType)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "failed to read data"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Bad request (%s)", err), http.StatusBadRequest)
		return
	}

	if response.metricsFetched == 0 {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "no metrics found"),
			zap.Int("http_code", http.StatusNotFound),
			zap.Error(err),
		)
		http.Error(wr, "Bad request (Not Found)", http.StatusNotFound)
		return
	}

	if listener.internalStatsDir != "" {
		listener.UpdateMetricsAccessTimesByRequest(response.metrics)
	}

	wr.Write(response.data)

	atomic.AddUint64(&listener.metrics.FetchSize, uint64(response.memoryUsed))
	logger.Info("fetch served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Bool("query_cache_enabled", listener.queryCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("metrics_fetched", response.metricsFetched),
		zap.Int("values_fetched", response.valuesFetched),
		zap.Int("memory_used_bytes", response.memoryUsed),
		zap.Int("http_code", http.StatusOK),
	)

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		kv.Bool("graphite.from_cache", fromCache),
		kv.Int("graphite.metrics", response.metricsFetched),
		kv.Int("graphite.values", response.valuesFetched),
		kv.Int("graphite.memory_used", response.memoryUsed),
	)

}

func (listener *CarbonserverListener) fetchWithCache(ctx context.Context, logger *zap.Logger, format responseFormat, targets map[timeRange][]target) (fetchResponse, bool, error) {
	logger = logger.With(
		zap.String("function", "fetchWithCache"),
	)
	fromCache := false
	var err error

	var response fetchResponse
	if listener.queryCacheEnabled {
		targetKeys := make([]string, 0, len(targets))
		for tr, ts := range targets {
			names := make([]string, 0, len(ts))
			for _, t := range ts {
				names = append(names, t.Name)
			}
			targetKeys = append(targetKeys, fmt.Sprintf("%s&%d&%d", strings.Join(names, "&"), tr.from, tr.until))
		}
		key := fmt.Sprintf("%s&%s", strings.Join(targetKeys, "&"), format)

		size := uint64(100 * 1024 * 1024)
		renderRequests := atomic.LoadUint64(&listener.metrics.RenderRequests)
		fetchSize := atomic.LoadUint64(&listener.metrics.FetchSize)
		if renderRequests > 0 {
			size = fetchSize / renderRequests
		}

		// TODO(gmagnusson): Our cache key changes if we permute any of the
		// metric names while keeping the from/until pairs fixed, or permute
		// any combination of metric names and from/until pairs as a unit.
		// These permutations to not change the response, so we should be more
		// clever about how we construct the cache key.
		//
		// An option is to sort the metric names within each from/until pair,
		// and to order (names,from,until) triples by the from timestamp.
		//
		// This is probably worth doing, as the only source of differences in
		// from/until pairs I know of comes from using timeShift or timeStack,
		// which again probably comes from trying to compare trends from last
		// week to what's going on now, which is something people are generally
		// interested in. It wouldn't be good to start adding false negative
		// cache misses to those kinds of queries.
		item := listener.queryCache.getQueryItem(key, size, 60)
		res, ok := item.FetchOrLock()
		listener.prometheus.cacheRequest("query", ok)
		if !ok {
			logger.Debug("query cache miss")
			atomic.AddUint64(&listener.metrics.QueryCacheMiss, 1)

			response, err = listener.prepareDataProto(ctx, logger, format, targets)
			if err != nil {
				item.StoreAbort()
			} else {
				item.StoreAndUnlock(response)
			}
		} else {
			logger.Debug("query cache hit")
			atomic.AddUint64(&listener.metrics.QueryCacheHit, 1)

			response = res.(fetchResponse)
			fromCache = true
		}
	} else {
		response, err = listener.prepareDataProto(ctx, logger, format, targets)
	}
	return response, fromCache, err
}

func (listener *CarbonserverListener) prepareDataProto(ctx context.Context, logger *zap.Logger, format responseFormat, targets map[timeRange][]target) (fetchResponse, error) {
	contentType := "application/text"
	var b []byte
	var metricsFetched int
	var memoryUsed int
	var valuesFetched int

	var multiv3 protov3.MultiFetchResponse
	var multiv2 protov2.MultiFetchResponse

	metricMap := make(map[string]bool)

	for _, ts := range targets {
		for _, metric := range ts {
			metricMap[metric.Name] = true
		}
	}
	metricNames := make([]string, len(metricMap))
	i := 0
	for k := range metricMap {
		metricNames[i] = k
		i++
	}
	expandedGlobs, err := listener.getExpandedGlobs(ctx, logger, time.Now(), metricNames)

	if expandedGlobs == nil {
		return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
	}

	metricGlobMap := make(map[string]globs)
	for _, expandedGlob := range expandedGlobs {
		metricGlobMap[strings.ReplaceAll(expandedGlob.Name, "/", ".")] = expandedGlob
	}

	var metrics []string
	for tr, ts := range targets {
		for _, metric := range ts {
			fromTime := tr.from
			untilTime := tr.until

			listener.logger.Debug("fetching data...")
			if expandedResult, ok := metricGlobMap[metric.Name]; ok {
				files, leafs := expandedResult.Files, expandedResult.Leafs
				if len(files) > listener.maxMetricsRendered {
					listener.accessLogger.Error(
						"rendering too many metrics",
						zap.Int("limit", listener.maxMetricsRendered),
						zap.Int("target", len(files)),
					)

					files = files[:listener.maxMetricsRendered]
					leafs = leafs[:listener.maxMetricsRendered]
				}

				metricsCount := 0
				for i := range files {
					if leafs[i] {
						metricsCount++
					}
				}
				listener.logger.Debug("expandGlobs result",
					zap.String("handler", "render"),
					zap.String("action", "expandGlobs"),
					zap.String("metric", metric.Name),
					zap.Int("metrics_count", metricsCount),
					zap.Int32("from", fromTime),
					zap.Int32("until", untilTime),
				)

				if format == protoV2Format || format == jsonFormat {
					res, err := listener.fetchDataPB(metric.Name, files, leafs, fromTime, untilTime)
					if err != nil {
						atomic.AddUint64(&listener.metrics.RenderErrors, 1)
						listener.logger.Error("error while fetching the data",
							zap.Error(err),
						)
						continue
					}
					multiv2.Metrics = append(multiv2.Metrics, res.Metrics...)
				} else {
					res, err := listener.fetchDataPB3(metric.Name, files, leafs, fromTime, untilTime)
					if err != nil {
						atomic.AddUint64(&listener.metrics.RenderErrors, 1)
						listener.logger.Error("error while fetching the data",
							zap.Error(err),
						)
						continue
					}
					for i := range res.Metrics {
						res.Metrics[i].PathExpression = metric.PathExpression
					}
					multiv3.Metrics = append(multiv3.Metrics, res.Metrics...)
				}
			} else {
				listener.logger.Debug("expand globs returned an error",
					zap.Error(err),
				)
				continue
			}

		}
	}

	if format == protoV2Format || format == jsonFormat {
		if len(multiv2.Metrics) == 0 && format == protoV2Format {
			return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
		}

		metricsFetched = len(multiv2.Metrics)
		for i := range multiv2.Metrics {
			metrics = append(metrics, multiv2.Metrics[i].Name)
			memoryUsed += multiv2.Metrics[i].Size()
			valuesFetched += len(multiv2.Metrics[i].Values)
		}
	} else {
		if len(multiv3.Metrics) == 0 && format == protoV3Format {
			return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
		}

		metricsFetched = len(multiv3.Metrics)
		for i := range multiv3.Metrics {
			metrics = append(metrics, multiv3.Metrics[i].Name)
			memoryUsed += multiv3.Metrics[i].Size()
			valuesFetched += len(multiv3.Metrics[i].Values)
		}
	}

	switch format {
	// We still keep old json format, because it's painful to deal with math.NaN that can occur in new format.
	case jsonFormat:
		contentType = "application/json"
		b, err = json.Marshal(multiv2)
	case protoV2Format:
		contentType = httpHeaders.ContentTypeCarbonAPIv2PB
		b, err = multiv2.Marshal()
	case protoV3Format:
		contentType = httpHeaders.ContentTypeCarbonAPIv3PB
		b, err = multiv3.Marshal()
	case pickleFormat:
		// transform protobuf data into what pickle expects
		// [{'start': 1396271100, 'step': 60, 'Name': 'metric',
		// 'values': [9.0, 19.0, None], 'end': 1396273140}

		var response []map[string]interface{}

		for _, metric := range multiv3.GetMetrics() {

			var m map[string]interface{} //nolint:gosimple

			m = make(map[string]interface{})
			m["start"] = metric.StartTime
			m["step"] = metric.StepTime
			m["end"] = metric.StopTime
			m["name"] = metric.Name
			m["pathExpression"] = metric.PathExpression
			m["xFilesFactor"] = metric.XFilesFactor
			m["consolidationFunc"] = metric.ConsolidationFunc

			mv := make([]interface{}, len(metric.Values))
			for i, p := range metric.Values {
				if math.IsNaN(metric.Values[i]) {
					mv[i] = nil
				} else {
					mv[i] = p
				}
			}

			m["values"] = mv
			response = append(response, m)
		}

		contentType = httpHeaders.ContentTypePickle
		var buf bytes.Buffer
		pEnc := pickle.NewEncoder(&buf)
		err = pEnc.Encode(response)
		b = buf.Bytes()
	default:
		err = fmt.Errorf("unknown format: %v", format)
	}

	if err != nil {
		return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
	}
	return fetchResponse{b, contentType, metricsFetched, valuesFetched, memoryUsed, metrics}, nil
}

func (listener *CarbonserverListener) fetchDataPB3(pathExpression string, files []string, leafs []bool, fromTime, untilTime int32) (*protov3.MultiFetchResponse, error) {
	var multi protov3.MultiFetchResponse
	var errs []error
	for i, fileName := range files {
		if !leafs[i] {
			listener.logger.Debug("skipping directory", zap.String("PathExpression", pathExpression))
			// can't fetch a directory
			continue
		}
		response, err := listener.fetchSingleMetricV3(fileName, pathExpression, fromTime, untilTime)
		if err == nil {
			multi.Metrics = append(multi.Metrics, *response)
		} else {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		listener.logger.Warn("errors occur while fetching data",
			zap.Any("errors", errs),
		)
	}
	return &multi, nil
}

func (listener *CarbonserverListener) fetchDataPB(metric string, files []string, leafs []bool, fromTime, untilTime int32) (*protov2.MultiFetchResponse, error) {
	var multi protov2.MultiFetchResponse
	var errs []error
	for i, metric := range files {
		if !leafs[i] {
			listener.logger.Debug("skipping directory", zap.String("metric", metric))
			// can't fetch a directory
			continue
		}
		response, err := listener.fetchSingleMetricV2(metric, fromTime, untilTime)
		if err == nil {
			multi.Metrics = append(multi.Metrics, *response)
		} else {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		listener.logger.Warn("errors occur while fetching data",
			zap.Any("errors", errs),
		)
	}
	return &multi, nil
}
