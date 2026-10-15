/*
  Package trace defined functions that we use for collecting traces.
*/

package carbonserver

import (
	"context"
	"net/http"
	"path"
	"sync/atomic"

	"google.golang.org/grpc/metadata"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var TraceHeaders = map[headerName]string{
	{"X-CTX-CarbonAPI-UUID"}:    "carbonapi_uuid",
	{"X-CTX-CarbonZipper-UUID"}: "carbonzipper_uuid",
	{"X-Request-ID"}:            "request_id",
}

var TraceGrpcMetadata = map[string]string{
	"carponapi_uuid": "carbonapi_uuid",
}

type headerName struct {
	string
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

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for mdKey, field := range TraceGrpcMetadata {
			vals := md.Get(mdKey)
			if len(vals) == 1 {
				logger = logger.With(zap.String(field, vals[0]))
			} else if len(vals) > 1 {
				logger = logger.With(zap.Strings(field, vals))
			}
		}
	}
	return logger
}

func TraceHandler(h http.HandlerFunc, globalStatusCodes, handlerStatusCodes []uint64, promRequest func(string, int)) http.HandlerFunc {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		for header := range TraceHeaders {
			v := req.Header.Get(header.string)
			if v != "" {
				ctx = context.WithValue(ctx, header, v)
			}
		}
		lrw := newResponseWriterWithStatus(rw)
		h.ServeHTTP(lrw, req.WithContext(ctx))
		if lrw.statusCodeMajor() < len(globalStatusCodes) {
			atomic.AddUint64(&globalStatusCodes[lrw.statusCodeMajor()], 1)
			atomic.AddUint64(&handlerStatusCodes[lrw.statusCodeMajor()], 1)
		}
		endpoint := path.Clean(req.URL.Path)
		promRequest(endpoint, lrw.statusCode)
	})
}

// traceFields returns the trace identifiers attached to the request context.
func traceFields(ctx context.Context) []zap.Field {
	var fields []zap.Field
	for header, field := range TraceHeaders {
		if value, ok := ctx.Value(header).(string); ok {
			fields = append(fields, zap.String(field, value))
		}
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for mdKey, field := range TraceGrpcMetadata {
			vals := md.Get(mdKey)
			if len(vals) == 1 {
				fields = append(fields, zap.String(field, vals[0]))
			} else if len(vals) > 1 {
				fields = append(fields, zap.Strings(field, vals))
			}
		}
	}
	return fields
}

// requestLogger adds a request's fields only to messages that are written.
// Building With loggers per request clones and encodes those fields for every
// request, although sampling drops most of the messages.
type requestLogger struct {
	logger *zap.Logger
	fields []zap.Field
}

func newRequestLogger(ctx context.Context, logger *zap.Logger, fields ...zap.Field) *requestLogger {
	return &requestLogger{logger: logger, fields: append(fields, traceFields(ctx)...)}
}

func (l *requestLogger) with(fields ...zap.Field) {
	l.fields = append(l.fields, fields...)
}

// check runs the logger's level and sampling decision before any field is
// built. Pass the message's fields to write.
func (l *requestLogger) check(level zapcore.Level, msg string) *zapcore.CheckedEntry {
	return l.logger.Check(level, msg)
}

func (l *requestLogger) write(ce *zapcore.CheckedEntry, fields ...zap.Field) {
	ce.Write(append(l.fields[:len(l.fields):len(l.fields)], fields...)...)
}

func (l *requestLogger) Info(msg string, fields ...zap.Field) {
	if ce := l.check(zap.InfoLevel, msg); ce != nil {
		l.write(ce, fields...)
	}
}

func (l *requestLogger) Error(msg string, fields ...zap.Field) {
	if ce := l.check(zap.ErrorLevel, msg); ce != nil {
		l.write(ce, fields...)
	}
}

// zap returns a logger with the request's fields for callees, encoded only
// if they log.
func (l *requestLogger) zap() *zap.Logger {
	return l.logger.WithLazy(l.fields...)
}
