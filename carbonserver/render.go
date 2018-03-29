package carbonserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
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

func (listener *CarbonserverListener) renderHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /render/?target=the.metric.Name&format=pickle&from=1396008021&until=1396022421
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.RenderRequests, 1)

	req.ParseForm()
	targetsStr := req.Form["target"]
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	var targets []target
	for _, t := range targetsStr {
		targets = append(targets, target{Name: t, PathExpression: t})
	}

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "render"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
	))

	accepts := req.Header["Accept"]
	for _, accept := range accepts {
		if accept == httpHeaders.ContentTypeCarbonAPIv3PB {
			format = "carbonapi_v3_pb"
			break
		}
	}

	if format == "" {
		format = "json"
	}

	accessLogger = accessLogger.With(
		zap.String("format", format),
	)

	formatCode, ok := knownFormats[format]
	if !ok {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	var fromTime int32
	var untilTime int32
	if formatCode != protoV3Format {
		i, err := strconv.Atoi(from)
		if err != nil {
			atomic.AddUint64(&listener.metrics.RenderErrors, 1)
			accessLogger.Error("fetch failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "invalid 'from' time"),
				zap.Int("http_code", http.StatusBadRequest),
			)
			http.Error(wr, "invalid 'from' time",
				http.StatusBadRequest)
			return
		}
		fromTime = int32(i)
		i, err = strconv.Atoi(until)
		if err != nil {
			atomic.AddUint64(&listener.metrics.RenderErrors, 1)
			accessLogger.Error("fetch failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "invalid 'until' time"),
				zap.Int("http_code", http.StatusBadRequest),
			)
			http.Error(wr, "invalid 'until' time",
				http.StatusBadRequest)
			return
		}
		untilTime = int32(i)
	} else {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			accessLogger.Error("info failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", err.Error()),
				zap.Int("http_code", http.StatusBadRequest),
			)
			http.Error(wr, "Bad request (unsupported format)",
				http.StatusBadRequest)
		}

		var pv3Request protov3.MultiFetchRequest
		pv3Request.Unmarshal(body)

		for i, t := range pv3Request.Metrics {
			if i == 0 {
				fromTime = int32(t.StartTime)
				untilTime = int32(t.StopTime)
				from = strconv.FormatInt(t.StartTime, 10)
				until = strconv.FormatInt(t.StopTime, 10)
			}
			targets = append(targets, target{Name: t.Name, PathExpression: t.PathExpression})
		}
	}

	accessLogger = accessLogger.With(
		zap.Any("targets", targets),
		zap.String("from", from),
		zap.String("until", until),
	)

	logger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "render"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.Any("targets", targets),
		zap.String("from", from),
		zap.String("until", until),
		zap.String("format", format),
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
			http.Error(wr, fmt.Sprintf("Panic occured, see logs for more information"),
				http.StatusInternalServerError)
		}
	}()

	response, fromCache, err := listener.fetchWithCache(logger, formatCode, targets, from, until, fromTime, untilTime)

	wr.Header().Set("Content-Type", response.contentType)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "failed to read data"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Bad request (%s)", err),
			http.StatusBadRequest)
		return
	}

	listener.UpdateMetricsAccessTimesByRequest(response.metrics)

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

func (listener *CarbonserverListener) fetchWithCache(logger *zap.Logger, format responseFormat, targets []target, from, until string, fromTime, untilTime int32) (fetchResponse, bool, error) {
	logger = logger.With(
		zap.String("function", "fetchWithCache"),
	)
	fromCache := false
	var err error

	var response fetchResponse
	if listener.queryCacheEnabled {
		targetsKey := make([]byte, len(targets) * 20)
		for i := range targets {
			if i != 0 {
				targetsKey = append(targetsKey, byte('&'))
			}
			targetsKey = append(targetsKey, []byte(targets[i].Name)...)
		}
		key := string(targetsKey) + "&" + format.String() + "&" + from + "&" + until
		size := uint64(100 * 1024 * 1024)
		renderRequests := atomic.LoadUint64(&listener.metrics.RenderRequests)
		fetchSize := atomic.LoadUint64(&listener.metrics.FetchSize)
		if renderRequests > 0 {
			size = fetchSize / renderRequests
		}
		item := listener.queryCache.getQueryItem(key, size, 60)
		res, ok := item.FetchOrLock()
		if !ok {
			logger.Debug("query cache miss")
			atomic.AddUint64(&listener.metrics.QueryCacheMiss, 1)
			response, err = listener.prepareDataProto(format, targets, fromTime, untilTime)
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
		response, err = listener.prepareDataProto(format, targets, fromTime, untilTime)
	}
	return response, fromCache, err
}

func (listener *CarbonserverListener) prepareDataProto(format responseFormat, targets []target, fromTime, untilTime int32) (fetchResponse, error) {
	contentType := "application/text"
	var b []byte
	var err error
	var metricsFetched int
	var memoryUsed int
	var valuesFetched int

	var multiv3 protov3.MultiFetchResponse
	var multiv2 protov2.MultiFetchResponse

	var metrics []string
	for _, metric := range targets {
		listener.logger.Debug("fetching data...")
		files, leafs, err := listener.expandGlobs(metric.Name)
		if err != nil {
			listener.logger.Debug("expand globs returned an error",
				zap.Error(err),
			)
			continue
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

			if len(multiv2.Metrics) == 0 {
				return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
			}

			metricsFetched = len(multiv2.Metrics)
			for i := range multiv2.Metrics {
				metrics = append(metrics, multiv2.Metrics[i].Name)
				memoryUsed += multiv2.Metrics[i].Size()
				valuesFetched += len(multiv2.Metrics[i].Values)
			}
			continue
		}

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

		if len(multiv3.Metrics) == 0 {
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
		//[{'start': 1396271100, 'step': 60, 'Name': 'metric',
		//'values': [9.0, 19.0, None], 'end': 1396273140}

		var response []map[string]interface{}

		for _, metric := range multiv3.GetMetrics() {

			var m map[string]interface{}

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
