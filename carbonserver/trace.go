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
