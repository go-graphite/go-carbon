package carbonserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	pickle "github.com/lomik/og-rek"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type findResponse struct {
	data        []byte
	contentType string
	files       int
	lookups     uint32
}

func (listener *CarbonserverListener) findHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/find/?local=1&format=pickle&query=the.metric.path.with.glob

	t0 := time.Now()
	ctx := req.Context()
	span := trace.SpanFromContext(ctx)

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
		body, err := io.ReadAll(req.Body)
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
		pv3Request.UnmarshalVT(body)
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

	span.SetAttributes(
		kv.String("graphite.query", strings.Join(query, ",")),
		kv.String("graphite.format", format),
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
		var result interface{}
		result, fromCache, err = getWithCache(logger, listener.findCache, key, size, 300,
			func() (interface{}, error) {
				return listener.findMetrics(ctx, logger, t0, formatCode, query)
			})
		if err == nil {
			listener.prometheus.cacheRequest("find", fromCache)
			if fromCache {
				atomic.AddUint64(&listener.metrics.FindCacheHit, 1)
			} else {
				atomic.AddUint64(&listener.metrics.FindCacheMiss, 1)
			}
			response = result.(*findResponse)
			if response.files == 0 {
				err = errorNotFound{}
			}
		}
	} else {
		response, err = listener.findMetrics(ctx, logger, t0, formatCode, query)
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
		zap.Uint32("lookups", response.lookups),
	)
	span.SetAttributes(
		kv.Int("graphite.files", response.files),
		kv.Bool("graphite.from_cache", fromCache),
	)

}

type errorNotFound struct{}

// skipcq: RVV-B0013
func (err errorNotFound) Error() string {
	return "Not Found"
}

type globs struct {
	Name      string
	Files     []string
	Leafs     []bool
	TrieNodes []*trieNode
	Lookups   uint32
}

func getProtoV2FindResponse(expandedGlob globs, query string) *protov2.GlobResponse {
	res := &protov2.GlobResponse{
		Name:    query,
		Matches: make([]*protov2.GlobMatch, 0),
	}

	for i, p := range expandedGlob.Files {
		res.Matches = append(res.Matches, &protov2.GlobMatch{Path: p, IsLeaf: expandedGlob.Leafs[i]})
	}

	return res
}

func (listener *CarbonserverListener) findMetrics(ctx context.Context, logger *zap.Logger, t0 time.Time, format responseFormat, names []string) (*findResponse, error) {
	var result findResponse
	metricsCount := uint64(0)
	expandedGlobs, err := listener.getExpandedGlobs(ctx, logger, t0, names)
	if expandedGlobs == nil {
		return nil, err
	}
	atomic.AddUint64(&listener.metrics.MetricsFound, metricsCount)
	switch format {
	case protoV3Format, jsonFormat:
		var err error
		multiResponse := protov3.MultiGlobResponse{}
		for _, glob := range expandedGlobs {
			result.files += len(glob.Files)
			result.lookups += glob.Lookups
			response := protov3.GlobResponse{
				Name:    glob.Name,
				Matches: make([]*protov3.GlobMatch, 0),
			}

			for i, p := range glob.Files {
				if glob.Leafs[i] {
					metricsCount++
				}
				response.Matches = append(response.Matches, &protov3.GlobMatch{Path: p, IsLeaf: glob.Leafs[i]})
			}
			multiResponse.Metrics = append(multiResponse.Metrics, &response)
		}

		logger.Debug("will send out response",
			//skipcq: VET-V0008
			//nolint:govet
			zap.Any("response", multiResponse),
		)

		switch format {
		case jsonFormat:
			result.contentType = httpHeaders.ContentTypeJSON
			//skipcq: VET-V0008
			//nolint:govet
			result.data, err = json.Marshal(multiResponse)
		case protoV3Format:
			result.contentType = httpHeaders.ContentTypeCarbonAPIv3PB
			result.data, err = multiResponse.MarshalVT()
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
		response := getProtoV2FindResponse(expandedGlobs[0], names[0])
		result.files += len(expandedGlobs[0].Files)
		result.lookups += expandedGlobs[0].Lookups
		for i := range expandedGlobs[0].Files {
			if expandedGlobs[0].Leafs[i] {
				metricsCount++
			}
		}
		result.data, err = response.MarshalVT()
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
		var lookups uint32

		glob := expandedGlobs[0]
		files += len(glob.Files)
		lookups += glob.Lookups
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
		return &findResponse{buf.Bytes(), httpHeaders.ContentTypePickle, files, lookups}, nil
	}
	return nil, nil
}

func (listener *CarbonserverListener) getExpandedGlobs(ctx context.Context, logger *zap.Logger, t0 time.Time, names []string) ([]globs, error) {
	var expandedGlobs []globs
	var errors []error
	var err error
	expGlobResultChan := make(chan *ExpandedGlobResponse, len(names))

	for _, name := range names {
		go listener.expandGlobs(ctx, name, expGlobResultChan)
	}
	responseCount := 0
GATHER:
	for {
		select {
		case expandedResult := <-expGlobResultChan:
			responseCount++
			glob := globs{
				Name: expandedResult.Name,
			}
			glob.Files, glob.Leafs, glob.TrieNodes, glob.Lookups, err = expandedResult.Files, expandedResult.Leafs, expandedResult.TrieNodes, expandedResult.Lookups, expandedResult.Err
			if err != nil {
				errors = append(errors, fmt.Errorf("%s: %s", expandedResult.Name, err))
			}
			expandedGlobs = append(expandedGlobs, glob)
			if responseCount == len(names) {
				break GATHER
			}

		case <-ctx.Done():
			switch ctx.Err() {
			case context.DeadlineExceeded:
				listener.prometheus.timeoutRequest()
			case context.Canceled:
				listener.prometheus.cancelledRequest()
			}
			return nil, fmt.Errorf("could not expand globs - %s", ctx.Err().Error())
		}
	}

	if len(errors) > 0 {
		atomic.AddUint64(&listener.metrics.FindErrors, uint64(len(errors)))

		if len(errors) == len(names) {
			logger.Error("find failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "can't expand globs"),
				zap.Errors("errors", errors),
			)
			return nil, fmt.Errorf("find failed, can't expand globs: %v", errors)
		} else {
			logger.Warn("find partly failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "can't expand globs for some metrics"),
				zap.Errors("errors", errors),
			)
		}
	}

	logger.Debug("expandGlobs result",
		zap.String("action", "expandGlobs"),
		zap.Strings("metrics", names),
		// zap.String("format", format.String()),
		zap.Any("result", expandedGlobs),
	)
	return expandedGlobs, nil
}

func (listener *CarbonserverListener) Find(ctx context.Context, req *protov2.GlobRequest) (*protov2.GlobResponse, error) {
	t0 := time.Now()
	span := trace.SpanFromContext(ctx)

	atomic.AddUint64(&listener.metrics.FindRequests, 1)

	query := req.Query
	format := protoV2Format.String()
	var reqPeer string
	if p, ok := peer.FromContext(ctx); ok {
		reqPeer = p.Addr.String()
	}

	logger := TraceContextToZap(ctx, listener.logger.With(
		zap.String("handler", "grpc-find"),
		zap.String("request_payload", req.String()),
		zap.String("peer", reqPeer),
	))

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "grpc-find"),
		zap.String("request_payload", req.String()),
		zap.String("peer", reqPeer),
	))

	logger = logger.With(
		zap.Strings("query", []string{query}),
		zap.String("format", format),
	)

	accessLogger = accessLogger.With(
		zap.Strings("query", []string{query}),
		zap.String("format", format),
	)

	span.SetAttributes(
		kv.String("graphite.query", query),
		kv.String("graphite.format", format),
	)

	var err error
	fromCache := false
	var finalRes *protov2.GlobResponse
	var lookups uint32
	if listener.findCacheEnabled {
		key := query + "&" + format + "grpc"
		size := uint64(100 * 1024 * 1024)
		var result interface{}
		result, fromCache, err = getWithCache(logger, listener.findCache, key, size, 300,
			func() (interface{}, error) {
				expandedGlobs, err := listener.getExpandedGlobs(ctx, logger, t0, []string{query})
				if err != nil {
					return nil, err
				}
				finalRes = getProtoV2FindResponse(expandedGlobs[0], query)
				lookups = expandedGlobs[0].Lookups
				return finalRes, nil
			})
		if err == nil {
			listener.prometheus.cacheRequest("find", fromCache)
			if fromCache {
				atomic.AddUint64(&listener.metrics.FindCacheHit, 1)
			} else {
				atomic.AddUint64(&listener.metrics.FindCacheMiss, 1)
			}
			finalRes = result.(*protov2.GlobResponse)
			if len(finalRes.Matches) == 0 {
				err = errorNotFound{}
			}
		}
	} else {
		var expandedGlobs []globs
		expandedGlobs, err = listener.getExpandedGlobs(ctx, logger, t0, []string{query})
		if err == nil {
			finalRes = getProtoV2FindResponse(expandedGlobs[0], query)
			lookups = expandedGlobs[0].Lookups
		}
	}

	if err != nil || finalRes == nil {
		var code codes.Code
		var reason string
		if _, ok := err.(errorNotFound); ok {
			reason = "Not Found"
			code = codes.NotFound
		} else {
			reason = "Internal error while processing request"
			code = codes.Internal
		}

		accessLogger.Error("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", reason),
			zap.Error(err),
			zap.Int("grpc_code", int(code)),
		)

		return nil, status.Error(code, reason)
	}

	if len(finalRes.Matches) == 0 {
		// to get an idea how often we search for nothing
		atomic.AddUint64(&listener.metrics.FindZero, 1)
	}

	accessLogger.Info("find success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("Files", len(finalRes.Matches)),
		zap.Bool("find_cache_enabled", listener.findCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("grpc_code", int(codes.OK)),
		zap.Uint32("lookups", lookups),
	)
	span.SetAttributes(
		kv.Int("graphite.files", len(finalRes.Matches)),
		kv.Bool("graphite.from_cache", fromCache),
	)
	return finalRes, nil
}
