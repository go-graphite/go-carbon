package carbonserver

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	_ "net/http/pprof" // skipcq: GO-S2108
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	"github.com/go-graphite/go-whisper"
	grpcv2 "github.com/go-graphite/protocol/carbonapi_v2_grpc"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	pickle "github.com/lomik/og-rek"
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

// TODO: year-2038 problem. need to move over to int64?
type timeRange struct {
	from  int32
	until int32
}

func getMD5HashString(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
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
		return int32(i), err // skipcq: GO-E1006
	}

	return int32(i), nil // skipcq: GO-E1006
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

func getProtoV3Targets(request *protov3.MultiFetchRequest) map[timeRange][]target {
	targets := make(map[timeRange][]target)
	for _, t := range request.Metrics {
		tr := timeRange{
			from:  int32(t.StartTime),
			until: int32(t.StopTime),
		}
		targets[tr] = append(targets[tr], target{Name: t.Name, PathExpression: t.PathExpression})
	}
	return targets
}

func getProtoV2Targets(request *protov2.MultiFetchRequest) map[timeRange][]target {
	targets := make(map[timeRange][]target)
	for _, t := range request.Metrics {
		tr := timeRange{
			from:  t.StartTime,
			until: t.StopTime,
		}
		targets[tr] = append(targets[tr], target{Name: t.Name, PathExpression: t.Name})
	}
	return targets
}

func getTargets(req *http.Request, format responseFormat) (map[timeRange][]target, error) {
	targets := make(map[timeRange][]target)

	switch format {
	case protoV3Format:
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return targets, fmt.Errorf("error reading body: %s", err.Error())
		}

		var pv3Request protov3.MultiFetchRequest
		if err := pv3Request.UnmarshalVT(body); err != nil {
			return targets, fmt.Errorf("invalid payload: %s", err.Error())
		}

		targets = getProtoV3Targets(&pv3Request)
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

	logger := TraceContextToZap(ctx, listener.logger.With(
		zap.String("handler", "render"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.Strings("targets", tgs),
		zap.String("format", format.String()),
	))

	tle := &traceLogEntries{}
	if listener.renderTraceLoggingEnabled {
		defer func() {
			tle.TotalDuration = float64(time.Since(t0)) / float64(time.Second)
			logger.Info("render trace", zap.Any("trace_log_entries", *tle))
		}()
	}
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

	response, fromCache, err := listener.fetchWithCache(ctx, logger, format, targets, tle)
	tle.FetchSize = response.memoryUsed
	tle.MetricsFetched = response.metricsFetched
	tle.ValuesFetched = response.valuesFetched

	wr.Header().Set("Content-Type", response.contentType)
	if err != nil {
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "failed to read data"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Bad request (%s)", err), http.StatusBadRequest)
		return
	}

	if response.metricsFetched == 0 && !listener.emptyResultOk {
		if !listener.doNotLog404s {
			accessLogger.Error("fetch failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "no metrics found"),
				zap.Int("http_code", http.StatusNotFound),
				zap.Error(err),
			)
		}
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
}

func (listener *CarbonserverListener) getRenderCacheKeyAndSize(targets map[timeRange][]target, format string) (string, uint64) {
	targetKeys := make([]string, 0, len(targets))
	for tr, ts := range targets {
		names := make([]string, 0, len(ts))
		for _, t := range ts {
			pathExpressionMD5 := getMD5HashString(t.PathExpression)
			names = append(names, t.Name+"&"+pathExpressionMD5)
		}
		targetKeys = append(targetKeys, fmt.Sprintf("%s&%d&%d", strings.Join(names, "&"), tr.from, tr.until))
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
	key := fmt.Sprintf("%s&%s", strings.Join(targetKeys, "&"), format)
	size := uint64(100 * 1024 * 1024)
	renderRequests := atomic.LoadUint64(&listener.metrics.RenderRequests)
	fetchSize := atomic.LoadUint64(&listener.metrics.FetchSize)
	if renderRequests > 0 {
		size = fetchSize / renderRequests
	}
	return key, size
}

func (listener *CarbonserverListener) fetchWithCache(ctx context.Context, logger *zap.Logger, format responseFormat, targets map[timeRange][]target, tle *traceLogEntries) (fetchResponse, bool, error) {
	logger = logger.With(
		zap.String("function", "fetchWithCache"),
	)
	fromCache := false
	var err error

	var response fetchResponse
	if listener.queryCacheEnabled {
		key, size := listener.getRenderCacheKeyAndSize(targets, format.String())
		var res interface{}
		cacheT0 := time.Now()
		res, fromCache, err = getWithCache(logger, listener.queryCache, key, size, 60,
			func() (interface{}, error) {
				return listener.prepareDataProto(ctx, logger, format, targets, tle)
			})
		tle.CacheDuration = float64(time.Since(cacheT0)) / float64(time.Second)
		if err == nil {
			response = res.(fetchResponse)
			listener.prometheus.cacheRequest("query", fromCache)
			if fromCache {
				atomic.AddUint64(&listener.metrics.QueryCacheHit, 1)
			} else {
				atomic.AddUint64(&listener.metrics.QueryCacheMiss, 1)
			}
			tle.FromCache = fromCache
		}
	} else {
		response, err = listener.prepareDataProto(ctx, logger, format, targets, tle)
	}
	return response, fromCache, err
}

func getUniqueMetricNames(targets map[timeRange][]target) []string {
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
	return metricNames
}

func getMetricGlobMapFromExpandedGlobs(expandedGlobs []globs) map[string]globs {
	metricGlobMap := make(map[string]globs)
	for _, expandedGlob := range expandedGlobs {
		metricGlobMap[strings.ReplaceAll(expandedGlob.Name, "/", ".")] = expandedGlob
	}
	return metricGlobMap
}

func getStreamingChannelSize(filesCount int) int {
	channelSize := 100
	if filesCount < channelSize {
		channelSize = filesCount
	}
	return channelSize
}

func (listener *CarbonserverListener) prepareDataStream(ctx context.Context, format responseFormat, targets map[timeRange][]target, metricGlobMap map[string]globs, responseChan chan<- response) {
	defer close(responseChan)
	for tr, ts := range targets {
		for _, metric := range ts {
			select {
			case <-ctx.Done():
				return
			default:
			}
			fromTime := tr.from
			untilTime := tr.until

			listener.logger.Debug("fetching data...")
			if expandedResult, ok := metricGlobMap[metric.Name]; ok {
				files, leafs, trieNodes := expandedResult.Files, expandedResult.Leafs, expandedResult.TrieNodes
				if len(files) > listener.maxMetricsRendered {
					listener.accessLogger.Error(
						"rendering too many metrics",
						zap.Int("limit", listener.maxMetricsRendered),
						zap.Int("files", len(files)),
						zap.Int("leafs", len(leafs)),
						zap.Int("trieNodes", len(trieNodes)),
					)
					if len(files) > listener.maxMetricsRendered {
						files = files[:listener.maxMetricsRendered]
					}
					if len(leafs) > listener.maxMetricsRendered {
						leafs = leafs[:listener.maxMetricsRendered]
					}
					if len(trieNodes) > listener.maxMetricsRendered {
						trieNodes = trieNodes[:listener.maxMetricsRendered]
					}
				}

				metricsCount := 0
				for i := range files {
					if leafs[i] {
						metricsCount++
					}
				}
				listener.accessLogger.Debug("expandGlobs result",
					zap.String("handler", "render"),
					zap.String("action", "expandGlobs"),
					zap.String("metric", metric.Name),
					zap.Int("metrics_count", metricsCount),
					zap.Int32("from", fromTime),
					zap.Int32("until", untilTime),
				)
				var res []response
				var err error
				if format == protoV2Format || format == jsonFormat {
					res, err = listener.fetchData(metric.Name, "", files, leafs, trieNodes, fromTime, untilTime)
				} else {
					// FIXME: why should we pass metric name instead of path Expression and fill it in afterwards?
					res, err = listener.fetchData(metric.Name, metric.Name, files, leafs, trieNodes, fromTime, untilTime)
					for i := range res {
						res[i].PathExpression = metric.PathExpression
					}
				}
				if err != nil {
					listener.accessLogger.Error("error while fetching the data",
						zap.Error(err),
					)
					continue
				}
				for i := range res {
					responseChan <- res[i]
				}
			} else {
				listener.accessLogger.Debug("expand globs returned an error",
					zap.String("metricName", metric.Name))
				continue
			}
		}
	}
}

func (listener *CarbonserverListener) prepareDataProto(ctx context.Context, logger *zap.Logger, format responseFormat, targets map[timeRange][]target, tle *traceLogEntries) (fetchResponse, error) {
	contentType := "application/text"
	var b []byte
	var metricsFetched int
	var memoryUsed int
	var valuesFetched int

	var multiv3 protov3.MultiFetchResponse
	var multiv2 protov2.MultiFetchResponse

	// TODO: should chan buffer size be configurable?
	responseChan := make(chan response, 1000)
	metricNames := getUniqueMetricNames(targets)
	// TODO: pipeline?
	expansionT0 := time.Now()
	expandedGlobs, isExpandCacheHit, err := listener.getExpandedGlobsWithCache(ctx, logger, "render", metricNames)
	tle.GlobExpansionDuration = float64(time.Since(expansionT0)) / float64(time.Second)
	tle.FindFromCache = isExpandCacheHit
	if expandedGlobs == nil {
		return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
	}
	metricGlobMap := getMetricGlobMapFromExpandedGlobs(expandedGlobs)
	go func() {
		prepareT0 := time.Now()
		listener.prepareDataStream(ctx, format, targets, metricGlobMap, responseChan)
		tle.PrepareDuration = float64(time.Since(prepareT0)) / float64(time.Second)
	}()

	var metrics []string
	for renderResponse := range responseChan {
		if format == protoV2Format || format == jsonFormat {
			res := renderResponse.proto2()
			multiv2.Metrics = append(multiv2.Metrics, res)
		} else {
			res := renderResponse.proto3()
			multiv3.Metrics = append(multiv3.Metrics, res)
		}
	}

	if listener.checkRequestCtx(ctx) != nil {
		err := fmt.Errorf("time out while preparing data proto")
		return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
	}

	if format == protoV2Format || format == jsonFormat {
		if len(multiv2.Metrics) == 0 && format == protoV2Format {
			return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
		}

		metricsFetched = len(multiv2.Metrics)
		for i := range multiv2.Metrics {
			metrics = append(metrics, multiv2.Metrics[i].Name)
			memoryUsed += multiv2.Metrics[i].SizeVT()
			valuesFetched += len(multiv2.Metrics[i].Values)
		}
	} else {
		if len(multiv3.Metrics) == 0 && format == protoV3Format {
			return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
		}

		metricsFetched = len(multiv3.Metrics)
		for i := range multiv3.Metrics {
			metrics = append(metrics, multiv3.Metrics[i].Name)
			memoryUsed += multiv3.Metrics[i].SizeVT()
			valuesFetched += len(multiv3.Metrics[i].Values)
		}
	}

	marshalT0 := time.Now()
	switch format {
	// We still keep old json format, because it's painful to deal with math.NaN that can occur in new format.
	case jsonFormat:
		contentType = "application/json"
		b, err = protojson.Marshal(multiv2.ProtoReflect().Interface())
	case protoV2Format:
		contentType = httpHeaders.ContentTypeCarbonAPIv2PB
		b, err = multiv2.MarshalVT()
	case protoV3Format:
		contentType = httpHeaders.ContentTypeCarbonAPIv3PB
		b, err = multiv3.MarshalVT()
	case pickleFormat:
		// transform protobuf data into what pickle expects
		// [{'start': 1396271100, 'step': 60, 'Name': 'metric',
		// 'values': [9.0, 19.0, None], 'end': 1396273140}

		var response []map[string]interface{}

		for _, metric := range multiv3.GetMetrics() {
			m := make(map[string]interface{})
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
	tle.MarshalDuration = float64(time.Since(marshalT0)) / float64(time.Second)

	if err != nil {
		return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
	}
	return fetchResponse{b, contentType, metricsFetched, valuesFetched, memoryUsed, metrics}, nil
}

func (listener *CarbonserverListener) fetchData(metric, pathExpression string, files []string, leafs []bool, trieNodes []*trieNode, fromTime, untilTime int32) ([]response, error) {
	var multi []response
	var errs []error
	var wg sync.WaitGroup
	var mu sync.Mutex
	var maxFetchDataGoroutines int
	// listener.maxFetchDataGoroutines is the maximum number of goroutines you want to allow
	// but < 0 is always wrong
	if listener.maxFetchDataGoroutines > 0 {
		maxFetchDataGoroutines = listener.maxFetchDataGoroutines
	} else {
		maxFetchDataGoroutines = 2 * runtime.GOMAXPROCS(0)
	}
	sem := make(chan struct{}, maxFetchDataGoroutines)
	for i, fileName := range files {
		wg.Add(1)
		sem <- struct{}{} // acquire a slot
		go func(i int, fileName string) {
			defer wg.Done()
			defer func() { <-sem }() // release the slot
			if !leafs[i] {
				listener.logger.Debug("skipping directory", zap.String("PathExpression", pathExpression),
					zap.String("metricName", metric), zap.String("fileName", fileName))
				// can't fetch a directory
				return
			}
			response, err := listener.fetchSingleMetric(fileName, pathExpression, fromTime, untilTime)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				multi = append(multi, response)
				if listener.trieIndex {
					readBytesNumber := int64(len(response.Values) * whisper.PointSize) // bytes read from the disc, 12 bytes for each point
					if len(trieNodes) > i {
						trieNodes[i].incrementReadBytesMetric(readBytesNumber)
					}
				}
			} else {
				errs = append(errs, err)
			}
		}(i, fileName)
	}
	wg.Wait()
	if len(errs) != 0 {
		listener.logger.Warn("errors occur while fetching data",
			zap.Any("errors", errs),
		)
	}
	return multi, nil
}

func (listener *CarbonserverListener) streamMetrics(stream grpcv2.CarbonV2_RenderServer, responseChan <-chan response) (metricsFetched, valuesFetched, fetchSize int, err error) {
	var metricAccessBatch []string
	for renderResponse := range responseChan {
		protoRes := renderResponse.proto2()
		fetchSize += protoRes.SizeVT()
		err = stream.Send(protoRes)
		if err != nil {
			return
		}
		metricsFetched++
		valuesFetched += len(renderResponse.Values)
		if listener.internalStatsDir != "" {
			metricAccessBatch = append(metricAccessBatch, renderResponse.Name)
		}
	}
	if listener.internalStatsDir != "" {
		listener.UpdateMetricsAccessTimesByRequest(metricAccessBatch)
	}
	return
}

const gRPCRenderMetricsCountHeaderKey = "metrics-count"

func sendRenderMetadataHeader(stream grpcv2.CarbonV2_RenderServer, filesCount int) error {
	header := metadata.Pairs(gRPCRenderMetricsCountHeaderKey, strconv.Itoa(filesCount))
	return stream.SendHeader(header)
}

// Render implements Render rpc of CarbonV2 gRPC service
func (listener *CarbonserverListener) Render(req *protov2.MultiFetchRequest, stream grpcv2.CarbonV2_RenderServer) (rpcErr error) {
	t0 := time.Now()
	ctx := stream.Context()

	var reqPeer string
	p, ok := peer.FromContext(stream.Context())
	if ok {
		reqPeer = p.Addr.String()
	}

	atomic.AddUint64(&listener.metrics.RenderRequests, 1)

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "grpc-render"),
		zap.String("request_payload", req.String()),
		zap.String("peer", reqPeer),
	))

	format := protoV2Format

	targets := getProtoV2Targets(req)
	tgs := getTargetNames(targets)
	accessLogger = accessLogger.With(
		zap.String("format", format.String()),
		zap.Strings("targets", tgs),
	)

	logger := TraceContextToZap(ctx, listener.logger.With(
		zap.String("handler", "grpc-render"),
		zap.String("request_payload", req.String()),
		zap.Strings("targets", tgs),
		zap.String("format", format.String()),
		zap.String("peer", reqPeer),
	))

	tle := &traceLogEntries{}
	if listener.renderTraceLoggingEnabled {
		defer func() {
			tle.TotalDuration = float64(time.Since(t0)) / float64(time.Second)
			logger.Info("render trace", zap.Any("trace_log_entries", *tle))
		}()
	}
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
			rpcErr = status.New(codes.Internal, "Panic occured, see logs for more information").Err()
		}
	}()

	metricsFetched := 0
	valuesFetched := 0
	fetchSize := 0

	fetchMetricsFunc := func() (chan response, error) {
		metricNames := getUniqueMetricNames(targets)
		// TODO: pipeline?
		expansionT0 := time.Now()
		expandedGlobs, isExpandCacheHit, err := listener.getExpandedGlobsWithCache(ctx, logger, "render", metricNames)
		tle.GlobExpansionDuration = float64(time.Since(expansionT0)) / float64(time.Second)
		tle.FindFromCache = isExpandCacheHit
		if expandedGlobs == nil {
			if err != nil {
				return nil, status.New(codes.InvalidArgument, err.Error()).Err()
			}
		}
		metricGlobMap := getMetricGlobMapFromExpandedGlobs(expandedGlobs)
		tle.MetricGlobMapLength = len(metricGlobMap)
		filesCount := countFilesInExpandedGlobs(expandedGlobs)
		err = sendRenderMetadataHeader(stream, filesCount)
		if err != nil {
			return nil, err
		}
		prepareChan := make(chan response, getStreamingChannelSize(filesCount))
		go func() {
			prepareT0 := time.Now()
			listener.prepareDataStream(ctx, format, targets, metricGlobMap, prepareChan)
			tle.PrepareDuration = float64(time.Since(prepareT0)) / float64(time.Second)
		}()
		return prepareChan, nil
	}

	var responseChanToStream chan response
	var fromCache bool
	var err error
	if listener.streamingQueryCacheEnabled {
		key, size := listener.getRenderCacheKeyAndSize(targets, format.String()+"grpc")
		var res interface{}
		cacheT0 := time.Now()

		item := listener.queryCache.getQueryItem(key, size, 60)
		res, found := item.FetchOrLock()
		switch {
		case !found:
			atomic.AddUint64(&listener.metrics.QueryCacheMiss, 1)
			responseChan, err := fetchMetricsFunc()
			if err != nil {
				item.StoreAbort()
				tle.CacheDuration = float64(time.Since(cacheT0)) / float64(time.Second)
			} else {
				responseChanToStream = make(chan response, cap(responseChan))
				go func() {
					var responses []response
					for r := range responseChan {
						responses = append(responses, r)
						responseChanToStream <- r
					}
					close(responseChanToStream)
					item.StoreAndUnlock(responses)
					tle.CacheDuration = float64(time.Since(cacheT0)) / float64(time.Second)
				}()
			}
		case res != nil:
			atomic.AddUint64(&listener.metrics.QueryCacheHit, 1)
			cachedResponses := res.([]response)
			err = sendRenderMetadataHeader(stream, len(cachedResponses))
			responseChanToStream = make(chan response, getStreamingChannelSize(len(cachedResponses)))
			go func() {
				for _, r := range cachedResponses {
					responseChanToStream <- r
				}
				close(responseChanToStream)
				tle.CacheDuration = float64(time.Since(cacheT0)) / float64(time.Second)
			}()
			fromCache = true
		default:
			err = fmt.Errorf("invalid cache record for the request")
			tle.CacheDuration = float64(time.Since(cacheT0)) / float64(time.Second)
		}
		listener.prometheus.cacheRequest("query", fromCache)
		tle.FromCache = fromCache
	} else {
		responseChanToStream, err = fetchMetricsFunc()
	}

	if err == nil {
		streamT0 := time.Now()
		metricsFetched, valuesFetched, fetchSize, err = listener.streamMetrics(stream, responseChanToStream)
		tle.StreamDuration = float64(time.Since(streamT0)) / float64(time.Second)
	}

	tle.FetchSize = fetchSize
	tle.MetricsFetched = metricsFetched
	tle.ValuesFetched = valuesFetched
	if err != nil {
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "stream send failed"),
			zap.Error(err),
		)
		return err
	}

	if listener.checkRequestCtx(ctx) != nil {
		return status.New(codes.DeadlineExceeded, "time out while preparing data proto").Err()
	}

	if metricsFetched == 0 && !listener.emptyResultOk {
		if !listener.doNotLog404s {
			accessLogger.Error("fetch failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "no metrics found"),
				zap.Error(err),
			)
		}
		return status.New(codes.NotFound, "no metrics found").Err()
	}

	atomic.AddUint64(&listener.metrics.FetchSize, uint64(fetchSize))
	logger.Info("fetch served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Bool("query_cache_enabled", listener.streamingQueryCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("metrics_fetched", metricsFetched),
		zap.Int("values_fetched", valuesFetched),
		zap.Int("http_code", http.StatusOK),
	)

	return nil
}

type traceLogEntries struct {
	FromCache             bool    `json:"from_cache"`
	FindFromCache         bool    `json:"find_from_cache"`
	FetchSize             int     `json:"fetch_size"`
	MetricsFetched        int     `json:"metrics_fetched"`
	ValuesFetched         int     `json:"values_fetched"`
	GlobExpansionDuration float64 `json:"glob_expansion_duration"`
	CacheDuration         float64 `json:"cache_duration"`
	PrepareDuration       float64 `json:"prepare_duration"`
	StreamDuration        float64 `json:"stream_duration"`
	MarshalDuration       float64 `json:"marshal_duration"`
	TotalDuration         float64 `json:"total_duration"`
	MetricGlobMapLength   int     `json:"metric_glob_map_length"`
}
