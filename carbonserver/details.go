package carbonserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"
	"unsafe"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func metricDetailsV2toV3(ptr *map[string]*protov2.MetricDetails) *map[string]*protov3.MetricDetails {
	return (*map[string]*protov3.MetricDetails)(unsafe.Pointer(ptr))
}

func (listener *CarbonserverListener) detailsHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/details/?format=json
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.DetailsRequests, 1)

	format := req.FormValue("format")

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "details"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("format", format),
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
	if !ok || formatCode == pickleFormat {
		atomic.AddUint64(&listener.metrics.DetailsErrors, 1)
		accessLogger.Error("details failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	var err error

	fidx := listener.CurrentFileIndex()
	if fidx == nil {
		atomic.AddUint64(&listener.metrics.DetailsErrors, 1)
		accessLogger.Error("details failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "can't fetch metrics list"),
			zap.Error(errMetricsListEmpty),
		)
		http.Error(wr, fmt.Sprintf("Can't fetch metrics details: %s", err), http.StatusInternalServerError)
		return
	}

	var b []byte

	contentType := ""
	switch formatCode {
	case jsonFormat:
		contentType = httpHeaders.ContentTypeJSON
		response := jsonMetricDetailsResponse{
			FreeSpace:  fidx.freeSpace,
			TotalSpace: fidx.totalSpace,
		}
		listener.fileIdxMutex.Lock()
		for m, v := range fidx.details {
			response.Metrics = append(response.Metrics, metricDetailsFlat{
				Name:          m,
				MetricDetails: v,
			})
		}
		b, err = json.Marshal(response)
		listener.fileIdxMutex.Unlock()
	case protoV2Format, protoV3Format:
		if formatCode == protoV3Format {
			contentType = httpHeaders.ContentTypeCarbonAPIv3PB
		} else {
			contentType = httpHeaders.ContentTypeCarbonAPIv2PB
		}
		listener.fileIdxMutex.Lock()
		response := &protov3.MetricDetailsResponse{
			Metrics:    fidx.details,
			FreeSpace:  fidx.freeSpace,
			TotalSpace: fidx.totalSpace,
		}
		b, err = response.Marshal()
		listener.fileIdxMutex.Unlock()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Error("details failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "response encode failed"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("An internal error has occured: %s", err), http.StatusInternalServerError)
		return
	}
	wr.Header().Set("Content-Type", contentType)
	wr.Write(b)

	accessLogger.Info("details served",
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
	return

}
