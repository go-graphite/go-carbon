package carbonserver

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"go.uber.org/zap"

	"github.com/go-graphite/go-whisper"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func retentionsV3toV2(ptr *[]protov3.Retention) *[]protov2.Retention {
	return (*[]protov2.Retention)(unsafe.Pointer(ptr))
}

func (listener *CarbonserverListener) infoHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /info/?target=the.metric.Name&format=json
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.InfoRequests, 1)
	req.ParseForm()
	metrics := req.Form["target"]
	format := req.FormValue("format")

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "info"),
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
	if !ok || formatCode == pickleFormat {
		atomic.AddUint64(&listener.metrics.InfoErrors, 1)
		accessLogger.Error("info failed",
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
			accessLogger.Error("info failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", err.Error()),
				zap.Int("http_code", http.StatusBadRequest),
			)
			http.Error(wr, "Bad request (unsupported format)",
				http.StatusBadRequest)
		}

		var pv3Request protov3.MultiGlobRequest
		pv3Request.Unmarshal(body)

		metrics = pv3Request.Metrics
	}

	accessLogger.With(
		zap.Strings("targets", metrics),
		zap.String("format", format),
	)

	response := protov3.MultiMetricsInfoResponse{}
	for _, metric := range metrics {
		path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
		w, err := whisper.Open(path)

		if err != nil {
			atomic.AddUint64(&listener.metrics.NotFound, 1)
			accessLogger.Error("info served",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "metric not found"),
				zap.Int("http_code", http.StatusNotFound),
			)
			http.Error(wr, "Metric not found", http.StatusNotFound)
			return
		}

		defer w.Close()

		aggr := w.AggregationMethod()
		maxr := int64(w.MaxRetention())
		xfiles := float32(w.XFilesFactor())

		rets := make([]protov3.Retention, 0, 4)
		for _, retention := range w.Retentions() {
			spp := int64(retention.SecondsPerPoint())
			nop := int64(retention.NumberOfPoints())
			rets = append(rets, protov3.Retention{
				SecondsPerPoint: spp,
				NumberOfPoints:  nop,
			})
		}

		response.Metrics = append(response.Metrics, protov3.MetricsInfoResponse{
			Name:              metric,
			ConsolidationFunc: aggr,
			MaxRetention:      maxr,
			XFilesFactor:      xfiles,
			Retentions:        rets,
		})
	}

	var b []byte
	var err error
	contentType := ""
	switch formatCode {
	case jsonFormat:
		contentType = httpHeaders.ContentTypeJSON
		b, err = json.Marshal(response)
	case protoV2Format, protoV3Format:
		if formatCode == protoV3Format {
			contentType = httpHeaders.ContentTypeCarbonAPIv3PB
		} else {
			contentType = httpHeaders.ContentTypeCarbonAPIv2PB
		}
		r := response.Metrics[0]
		response := protov2.InfoResponse{
			Name:              r.Name,
			AggregationMethod: r.ConsolidationFunc,
			MaxRetention:      int32(r.MaxRetention),
			XFilesFactor:      r.XFilesFactor,
			Retentions:        *retentionsV3toV2(&r.Retentions),
		}
		b, err = response.Marshal()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("info failed",
			zap.String("reason", "response encode failed"),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(wr, "Failed to encode response: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	wr.Header().Set("Content-Type", contentType)
	wr.Write(b)

	accessLogger.Info("info served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
	return
}
