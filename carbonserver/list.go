package carbonserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

var errMaxGlobsExhausted = fmt.Errorf("maxGlobs in request exhausted, kindly refusing to perform the request")
var errMetricsListEmpty = fmt.Errorf("File index is empty or disabled")

func (listener *CarbonserverListener) getMetricsList() ([]string, error) {
	fidx := listener.CurrentFileIndex()
	var metrics []string

	if fidx == nil {
		return nil, errMetricsListEmpty
	}

	for _, p := range fidx.files {
		if !strings.HasSuffix(p, ".wsp") {
			continue
		}
		p = p[1 : len(p)-4]
		metrics = append(metrics, strings.Replace(p, "/", ".", -1))
	}
	return metrics, nil
}

func (listener *CarbonserverListener) listHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/list/?format=json
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.ListRequests, 1)

	format := req.FormValue("format")

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "list"),
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
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Error("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	var err error

	metrics, err := listener.getMetricsList()
	if err != nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Error("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "can't fetch metrics list"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Can't fetch metrics list: %s", err), http.StatusInternalServerError)
		return
	}

	contentType := ""
	var b []byte
	response := &protov3.ListMetricsResponse{Metrics: metrics}
	switch formatCode {
	case jsonFormat:
		b, err = json.Marshal(response)
	case protoV3Format, protoV2Format:
		if formatCode == protoV3Format {
			contentType = httpHeaders.ContentTypeCarbonAPIv3PB
		} else {
			contentType = httpHeaders.ContentTypeCarbonAPIv2PB
		}
		b, err = response.Marshal()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Error("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "response encode failed"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("An internal error has occured: %s", err), http.StatusInternalServerError)
		return
	}
	wr.Header().Set("Content-Type", contentType)
	wr.Write(b)

	accessLogger.Info("list served",
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
	return

}
