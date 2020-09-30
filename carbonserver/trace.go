/*
  Package trace defined functions that we use for collecting traces.
*/

package carbonserver

import (
	"context"
	"net/http"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/lomik/zapwriter"
	otelglobal "go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/propagation"
	"go.opentelemetry.io/otel/api/standard"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/exporters/trace/stdout"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

var TraceHeaders = map[headerName]string{
	{"X-CTX-CarbonAPI-UUID"}:    "carbonapi_uuid",
	{"X-CTX-CarbonZipper-UUID"}: "carbonzipper_uuid",
	{"X-Request-ID"}:            "request_id",
}

type headerName struct {
	string
}

// initTracer creates a new trace provider instance and registers it as global trace provider.
func (c *CarbonserverListener) InitTracing(jaegerEndpoint string, sendtoStdout bool, version string, timeout time.Duration) func() {
	logger := zapwriter.Logger("app")

	propagator := trace.B3{}
	// Grafana propagates traces over b3 headers
	oldProps := otelglobal.Propagators()
	props := propagation.New(
		propagation.WithExtractors(propagator),
		propagation.WithExtractors(oldProps.HTTPExtractors()...),
		propagation.WithInjectors(oldProps.HTTPInjectors()...),
	)
	otelglobal.SetPropagators(props)
	switch {
	case jaegerEndpoint != "":
		logger.Info("Traces", zap.String("jaegerEndpoint", jaegerEndpoint))
		client := &http.Client{
			Transport: &http.Transport{
				Proxy: nil,
			},
			Timeout: timeout,
		}
		fqdn, _ := os.Hostname()
		_, flush, err := jaeger.NewExportPipeline(
			jaeger.WithCollectorEndpoint(jaegerEndpoint, jaeger.WithHTTPClient((client))),
			jaeger.WithProcess(jaeger.Process{
				ServiceName: "go-carbon",
				Tags: []kv.KeyValue{
					kv.String("exporter", "jaeger"),
					kv.String("host.hostname", fqdn),
					kv.String("service.version", version),
				},
			}),
			jaeger.RegisterAsGlobal(),
			jaeger.WithSDK(&sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
		)
		if err != nil {
			logger.Fatal("can't initialize jaeger tracing exporter", zap.Error(err))
		}
		return flush
	case sendtoStdout:
		exporter, err := stdout.NewExporter(stdout.Options{PrettyPrint: true})
		if err != nil {
			logger.Fatal("can't initialize stdout tracing exporter", zap.Error(err))
		}
		provider, err := sdktrace.NewProvider(sdktrace.WithSyncer(exporter),
			sdktrace.WithConfig(sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}))
		if err != nil {
			logger.Fatal("failed to initialize trace provider", zap.Error(err))
		}

		otelglobal.SetTraceProvider(provider)
		return func() {}
	default:
		// create and register NoopTracer
		provider := trace.NoopProvider{}
		otelglobal.SetTraceProvider(provider)
		return func() {} // Nothing to flush
	}
}

func TraceContextToZap(ctx context.Context, logger *zap.Logger) *zap.Logger {

	for header, field := range TraceHeaders {
		v := ctx.Value(header)
		if v == nil {
			continue
		}

		if value, ok := v.(string); ok {
			logger = logger.With(zap.String(field, value))
		}
	}

	return logger
}

func TraceHandler(h http.HandlerFunc, globalStatusCodes, handlerStatusCodes []uint64, promRequest func(string, int)) http.HandlerFunc {
	tracer := otelglobal.Tracer("go-carbon")

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		for header := range TraceHeaders {
			v := req.Header.Get(header.string)
			if v != "" {
				ctx = context.WithValue(ctx, header, v)
			}
		}
		propagators := otelglobal.Propagators()
		ctx = propagation.ExtractHTTP(ctx, propagators, req.Header)
		spanName := req.URL.Path
		traceOpts := []trace.StartOption{
			trace.WithAttributes(standard.NetAttributesFromHTTPRequest("tcp", req)...),
			trace.WithAttributes(standard.EndUserAttributesFromHTTPRequest(req)...),
			trace.WithAttributes(standard.HTTPServerAttributesFromHTTPRequest("", "", req)...),
			trace.WithSpanKind(trace.SpanKindServer),
		}

		ctx, span := tracer.Start(ctx, spanName, traceOpts...)
		defer span.End()

		lrw := newResponseWriterWithStatus(rw)

		h.ServeHTTP(lrw, req.WithContext(ctx))

		if lrw.statusCodeMajor() < len(globalStatusCodes) {
			atomic.AddUint64(&globalStatusCodes[lrw.statusCodeMajor()], 1)
			atomic.AddUint64(&handlerStatusCodes[lrw.statusCodeMajor()], 1)
		}

		endpoint := path.Clean(req.URL.Path)
		promRequest(endpoint, lrw.statusCode)

		attrs := standard.HTTPAttributesFromHTTPStatusCode(lrw.statusCode)
		spanStatus, spanMessage := standard.SpanStatusFromHTTPStatusCode(lrw.statusCode)
		span.SetAttributes(attrs...)
		span.SetStatus(spanStatus, spanMessage)
	})
}
