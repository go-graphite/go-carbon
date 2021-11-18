package carbonserver

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-whisper"

	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func (listener *CarbonserverListener) infoHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /info/?target=the.metric.Name&format=json
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.InfoRequests, 1)

	format := req.FormValue("format")
	metrics := req.Form["target"]

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
	var retentionsV2 []protov2.Retention
	for i, metric := range metrics {
		path := listener.whisperData + "/" + strings.ReplaceAll(metric, ".", "/") + ".wsp"
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

		aggr := strings.Title(w.AggregationMethod().String())
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
			// only one metric is enough - first metric
			// TODO include support for multiple metrics
			if i == 0 && formatCode == protoV2Format {
				retentionsV2 = append(retentionsV2, protov2.Retention{
					SecondsPerPoint: int32(retention.SecondsPerPoint()),
					NumberOfPoints:  int32(retention.NumberOfPoints()),
				})
			}
		}

		response.Metrics = append(response.Metrics, protov3.MetricsInfoResponse{
			Name:              metric,
			ConsolidationFunc: aggr,
			MaxRetention:      maxr,
			XFilesFactor:      xfiles,
			Retentions:        rets,
		})
	}

	if len(response.Metrics) == 0 {
		atomic.AddUint64(&listener.metrics.InfoErrors, 1)
		accessLogger.Error("info failed",
			zap.String("reason", "Not Found"),
			zap.Int("http_code", http.StatusNotFound),
			zap.Error(errorNotFound{}),
		)
		http.Error(wr, "Not Found", http.StatusNotFound)
		return
	}

	var b []byte
	var err error
	contentType := ""
	switch formatCode {
	case jsonFormat:
		contentType = httpHeaders.ContentTypeJSON
		b, err = json.Marshal(response)
	case protoV2Format:
		contentType = httpHeaders.ContentTypeCarbonAPIv2PB

		var r protov3.MetricsInfoResponse
		if len(response.Metrics) > 0 {
			r = response.Metrics[0]
		}

		response := protov2.InfoResponse{
			Name:              r.Name,
			AggregationMethod: r.ConsolidationFunc,
			MaxRetention:      int32(r.MaxRetention),
			XFilesFactor:      r.XFilesFactor,
			Retentions:        retentionsV2,
		}
		b, err = response.Marshal()
	case protoV3Format:
		contentType = httpHeaders.ContentTypeCarbonAPIv3PB
		b, err = response.Marshal()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.InfoErrors, 1)
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
}
