package carbonserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	pickle "github.com/lomik/og-rek"
)

type findResponse struct {
	data        []byte
	contentType string
	files       int
}

func (listener *CarbonserverListener) findHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/find/?local=1&format=pickle&query=the.metric.path.with.glob

	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.FindRequests, 1)

	format := req.FormValue("format")
	query := req.Form["query"]

	var response *findResponse

	logger := TraceContextToZap(ctx, listener.logger.With(
		zap.String("handler", "find"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
	))

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "find"),
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

	formatCode, ok := knownFormats[format]
	if !ok {
		atomic.AddUint64(&listener.metrics.FindErrors, 1)
		accessLogger.Error("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	if formatCode == protoV3Format {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			accessLogger.Error("find failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", err.Error()),
				zap.Int("http_code", http.StatusBadRequest),
			)
			http.Error(wr, "Bad request (unsupported format)",
				http.StatusBadRequest)
		}

		var pv3Request protov3.MultiGlobRequest
		pv3Request.Unmarshal(body)

		fmt.Printf("\n\n%+v\n\n", pv3Request)

		query = pv3Request.Metrics
	}

	logger = logger.With(
		zap.Strings("query", query),
		zap.String("format", format),
	)

	accessLogger = accessLogger.With(
		zap.Strings("query", query),
		zap.String("format", format),
	)

	if len(query) == 0 {
		atomic.AddUint64(&listener.metrics.FindErrors, 1)
		accessLogger.Error("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "empty query"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (no query)", http.StatusBadRequest)
		return
	}

	var err error
	fromCache := false
	if listener.findCacheEnabled {
		key := strings.Join(query, ",") + "&" + format
		size := uint64(100 * 1024 * 1024)
		item := listener.findCache.getQueryItem(key, size, 300)
		res, ok := item.FetchOrLock()
		listener.prometheus.cacheRequest("find", ok)
		if !ok {
			logger.Debug("find cache miss")
			atomic.AddUint64(&listener.metrics.FindCacheMiss, 1)
			response, err = listener.findMetrics(logger, t0, formatCode, query)
			if err != nil {
				item.StoreAbort()
			} else {
				item.StoreAndUnlock(response)
			}
		} else if res != nil {
			logger.Debug("query cache hit")
			atomic.AddUint64(&listener.metrics.FindCacheHit, 1)
			response = res.(*findResponse)
			fromCache = true
		}
	} else {
		response, err = listener.findMetrics(logger, t0, formatCode, query)
	}

	if err != nil || response == nil {
		var code int
		var reason string
		if _, ok := err.(errorNotFound); ok {
			reason = "Not Found"
			code = http.StatusNotFound
		} else {
			reason = "Internal error while processing request"
			code = http.StatusInternalServerError
		}

		accessLogger.Error("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", reason),
			zap.Error(err),
			zap.Int("http_code", code),
		)
		http.Error(wr, fmt.Sprintf("%s (%v)", reason, err), code)

		return
	}

	wr.Header().Set("Content-Type", response.contentType)
	wr.Write(response.data)

	if response.files == 0 {
		// to get an idea how often we search for nothing
		atomic.AddUint64(&listener.metrics.FindZero, 1)
	}

	accessLogger.Info("find success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("Files", response.files),
		zap.Bool("find_cache_enabled", listener.findCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("http_code", http.StatusOK),
	)
	return
}

type errorNotFound struct{}

func (err errorNotFound) Error() string {
	return "Not Found"
}

type findError struct {
	name string
	err  error
}

type globs struct {
	Name  string
	Files []string
	Leafs []bool
}

func (listener *CarbonserverListener) findMetrics(logger *zap.Logger, t0 time.Time, format responseFormat, names []string) (*findResponse, error) {
	var result findResponse
	var expandedGlobs []globs
	var errors []findError
	var err error
	metricsCount := uint64(0)
	for _, name := range names {

		glob := globs{
			Name: name,
		}
		glob.Files, glob.Leafs, err = listener.expandGlobs(name)
		if err != nil {
			errors = append(errors, findError{name: name, err: err})
			continue
		}

		expandedGlobs = append(expandedGlobs, glob)
	}

	if len(errors) > 0 {
		atomic.AddUint64(&listener.metrics.FindErrors, uint64(len(errors)))

		if len(errors) == len(names) {
			logger.Error("find failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "can't expand globs"),
				zap.Any("errors", errors),
			)
			return nil, fmt.Errorf("find failed, can't expand globs")
		} else {
			logger.Warn("find partly failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "can't expand globs for some metrics"),
				zap.Any("errors", errors),
			)
		}
	}

	atomic.AddUint64(&listener.metrics.MetricsFound, metricsCount)
	logger.Debug("expandGlobs result",
		zap.String("action", "expandGlobs"),
		zap.Strings("metrics", names),
		zap.String("format", format.String()),
		zap.Any("result", expandedGlobs),
	)

	switch format {
	case protoV3Format, jsonFormat:
		var err error
		multiResponse := protov3.MultiGlobResponse{}
		for _, glob := range expandedGlobs {
			result.files += len(glob.Files)
			response := protov3.GlobResponse{
				Name:    glob.Name,
				Matches: make([]protov3.GlobMatch, 0),
			}

			for i, p := range glob.Files {
				if glob.Leafs[i] {
					metricsCount++
				}
				response.Matches = append(response.Matches, protov3.GlobMatch{Path: p, IsLeaf: glob.Leafs[i]})
			}
			multiResponse.Metrics = append(multiResponse.Metrics, response)
		}

		logger.Debug("will send out response",
			zap.Any("response", multiResponse),
		)

		switch format {
		case jsonFormat:
			result.contentType = httpHeaders.ContentTypeJSON
			result.data, err = json.Marshal(multiResponse)
		case protoV3Format:
			result.contentType = httpHeaders.ContentTypeCarbonAPIv3PB
			result.data, err = multiResponse.Marshal()
		}

		if err != nil {
			atomic.AddUint64(&listener.metrics.FindErrors, 1)

			logger.Error("find failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "response encode failed"),
				zap.Error(err),
			)
			return nil, err
		}

		if len(multiResponse.Metrics) == 0 {
			return nil, errorNotFound{}
		}

		return &result, err

	case protoV2Format:
		result.contentType = httpHeaders.ContentTypeProtobuf
		var err error
		response := protov2.GlobResponse{
			Name:    names[0],
			Matches: make([]protov2.GlobMatch, 0),
		}

		result.files += len(expandedGlobs[0].Files)
		for i, p := range expandedGlobs[0].Files {
			if expandedGlobs[0].Leafs[i] {
				metricsCount++
			}
			response.Matches = append(response.Matches, protov2.GlobMatch{Path: p, IsLeaf: expandedGlobs[0].Leafs[i]})
		}
		result.data, err = response.Marshal()
		result.contentType = httpHeaders.ContentTypeProtobuf

		if err != nil {
			atomic.AddUint64(&listener.metrics.FindErrors, 1)

			logger.Error("find failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "response encode failed"),
				zap.Error(err),
			)
			return nil, err
		}

		if len(response.Matches) == 0 {
			return nil, errorNotFound{}
		}

		return &result, err

	case pickleFormat:
		// [{'metric_path': 'metric', 'intervals': [(x,y)], 'isLeaf': True},]
		var metrics []map[string]interface{}
		var m map[string]interface{}
		files := 0

		glob := expandedGlobs[0]
		files += len(glob.Files)
		for i, p := range glob.Files {
			if glob.Leafs[i] {
				metricsCount++
			}
			m = make(map[string]interface{})
			m["metric_path"] = p
			m["isLeaf"] = glob.Leafs[i]

			metrics = append(metrics, m)
		}
		var buf bytes.Buffer
		pEnc := pickle.NewEncoder(&buf)
		pEnc.Encode(metrics)
		return &findResponse{buf.Bytes(), httpHeaders.ContentTypePickle, files}, nil
	}
	return nil, nil
}
