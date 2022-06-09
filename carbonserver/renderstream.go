package carbonserver

import (
	"context"
	"github.com/go-graphite/go-carbon/helper/streamingpb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

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

func (listener *CarbonserverListener) prepareDataProtoStream(ctx context.Context, targets map[timeRange][]target, expandedGlobs []globs, responseChan chan protov3.FetchResponse) {
	defer close(responseChan)
	if len(expandedGlobs) == 0 {
		return
	}
	metricGlobMap := make(map[string]globs)
	for _, expandedGlob := range expandedGlobs {
		metricGlobMap[strings.ReplaceAll(expandedGlob.Name, "/", ".")] = expandedGlob
	}

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
				listener.accessLogger.Debug("expandGlobs result",
					zap.String("handler", "render"),
					zap.String("action", "expandGlobs"),
					zap.String("metric", metric.Name),
					zap.Int("metrics_count", metricsCount),
					zap.Int32("from", fromTime),
					zap.Int32("until", untilTime),
				)
				res, err := listener.fetchDataPB3(metric.Name, files, leafs, fromTime, untilTime)
				if err != nil {
					atomic.AddUint64(&listener.metrics.RenderErrors, 1)
					listener.accessLogger.Error("error while fetching the data",
						zap.Error(err),
					)
					continue
				}
				for i := range res.Metrics {
					res.Metrics[i].PathExpression = metric.PathExpression
					responseChan <- res.Metrics[i]
				}
			} else {
				listener.accessLogger.Debug("expand globs returned an error",
					zap.String("metricName", metric.Name))
				continue
			}
		}
	}
}

func (listener *CarbonserverListener) RenderStream(req *protov3.MultiFetchRequest, stream streamingpb.CarbonStream_RenderStreamServer) (rpcErr error) {
	t0 := time.Now()
	ctx := context.Background()

	atomic.AddUint64(&listener.metrics.RenderRequests, 1)

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "renderStream"),
		zap.String("request_payload", req.String()),
	))

	format := knownFormats["carbonapi_v3_pb"]
	accessLogger = accessLogger.With(
		zap.String("format", format.String()),
	)

	targets := getTargetsFromProtoV3FormatReq(*req)
	tgs := getTargetNames(targets)
	accessLogger = accessLogger.With(
		zap.Strings("targets", tgs),
	)

	logger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "render"),
		zap.String("request_payload", req.String()),
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
			rpcErr = status.New(codes.Internal, "Panic occured, see logs for more information").Err()
		}
	}()

	// TODO: implementing cache?
	fromCache := false

	// TODO: chan buffer should be configurable
	responseChan := make(chan protov3.FetchResponse, 1000)

	metricNames := getUniqueMetricNames(targets)
	expandedGlobs, err := listener.getExpandedGlobs(ctx, logger, time.Now(), metricNames)
	if expandedGlobs == nil {
		if err != nil {
			return status.New(codes.InvalidArgument, err.Error()).Err()
		}
	}

	go listener.prepareDataProtoStream(ctx, targets, expandedGlobs, responseChan)

	metricsFetched := 0
	valuesFetched := 0
	// TODO: configurable?
	metricAccessBatchSize := 100
	metricAccessBatch := make([]string, 0, metricAccessBatchSize)
	for renderResponse := range responseChan {
		err := stream.Send(&renderResponse)
		if err != nil {
			atomic.AddUint64(&listener.metrics.RenderErrors, 1)
			accessLogger.Error("fetch failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "stream send failed"),
				zap.Error(err),
			)
			return err
		}
		metricsFetched++
		valuesFetched += len(renderResponse.Values)
		if listener.internalStatsDir != "" {
			metricAccessBatch = append(metricAccessBatch, renderResponse.Name)
			if len(metricAccessBatch) >= metricAccessBatchSize {
				listener.UpdateMetricsAccessTimesByRequest(metricAccessBatch)
				metricAccessBatch = metricAccessBatch[:0]
			}
		}
	}

	if metricsFetched == 0 && !listener.emptyResultOk {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "no metrics found"),
			zap.Error(err),
		)
		return status.New(codes.NotFound, "no metrics found").Err()
	}

	logger.Info("fetch served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Bool("query_cache_enabled", listener.queryCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("metrics_fetched", metricsFetched),
		zap.Int("values_fetched", valuesFetched),
		zap.Int("http_code", http.StatusOK),
	)

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		kv.Bool("graphite.from_cache", fromCache),
		kv.Int("graphite.metrics", metricsFetched),
		kv.Int("graphite.values", valuesFetched),
	)

	return nil
}
