package carbonserver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
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

	if listener.trieIndex {
		return fidx.trieIdx.allMetrics('.'), nil
	}

	for _, p := range fidx.files {
		if !strings.HasSuffix(p, ".wsp") {
			continue
		}
		p = p[1 : len(p)-4]
		metrics = append(metrics, strings.ReplaceAll(p, "/", "."))
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
		contentType = "application/json"
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
}

// TODO: move it to protocol/carbaonapi_v{2,3}
type ListMetricInfo struct {
	Name         string
	PhysicalSize int64
	LogicalSize  int64
}

type ListQueryResult struct {
	Count        int
	PhysicalSize int64
	LogicalSize  int64

	Metrics []ListMetricInfo
}

// limit doesn't affect statsOnly queries
// skipcq: RVV-A0005
func (listener *CarbonserverListener) queryMetricsList(query string, limit int, leafOnly, statsOnly bool) (*ListQueryResult, error) {
	fidx := listener.CurrentFileIndex()
	var result ListQueryResult

	if fidx == nil || !listener.trieIndex {
		return nil, errMetricsListEmpty
	}

	log.Println(strings.ReplaceAll(query, ".", "/"))
	names, isFiles, nodes, err := fidx.trieIdx.query(strings.ReplaceAll(query, ".", "/"), limit, nil)
	if err != nil {
		return nil, err
	}

	for i, name := range names {
		if len(result.Metrics) >= limit {
			break
		}

		if isFiles[i] {
			result.Count += 1

			if meta, ok := nodes[i].meta.(*fileMeta); ok && meta != nil {
				result.PhysicalSize += meta.physicalSize
				result.LogicalSize += meta.logicalSize

				if !statsOnly {
					result.Metrics = append(result.Metrics, ListMetricInfo{
						Name:         names[i],
						PhysicalSize: meta.physicalSize,
						LogicalSize:  meta.logicalSize,
					})
				}
			}

			continue
		}

		if leafOnly {
			continue
		}

		pmetrics, pnodes, pcount, pphysical, plogical := fidx.trieIdx.allMetricsNode(nodes[i], '.', name, limit-len(result.Metrics), statsOnly)
		result.Count += pcount
		result.PhysicalSize += pphysical
		result.LogicalSize += plogical

		for i := 0; i < len(pmetrics) && len(result.Metrics) < limit; i++ {
			result.Metrics = append(result.Metrics, ListMetricInfo{
				Name:         pmetrics[i],
				PhysicalSize: pnodes[i].meta.(*fileMeta).physicalSize,
				LogicalSize:  pnodes[i].meta.(*fileMeta).logicalSize,
			})
		}
	}

	return &result, nil
}

func (listener *CarbonserverListener) listQueryHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/list_query/?format=json&target=sys.app.*&stats_only=true&leaf_only=false
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.ListQueryRequests, 1)

	format := req.FormValue("format")
	target := req.FormValue("target")
	statsOnly := req.FormValue("stats_only") == "true"
	leafOnly := req.FormValue("leaf_only") == "true"

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "list_query"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("format", format),
	))

	limit := 65536
	if req.FormValue("limit") != "" {
		var err error
		limit, err = strconv.Atoi(req.FormValue("limit"))
		if err != nil {
			atomic.AddUint64(&listener.metrics.ListQueryErrors, 1)
			accessLogger.Error("list failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "faield to parse limit"),
				zap.Error(err),
			)
			http.Error(wr, fmt.Sprintf("Can't fetch metrics list: %s", err), http.StatusInternalServerError)
			return
		}
	}

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
	if !ok || formatCode != jsonFormat {
		atomic.AddUint64(&listener.metrics.ListQueryErrors, 1)
		accessLogger.Error("list_query failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	result, err := listener.queryMetricsList(target, limit, leafOnly, statsOnly)
	if err != nil {
		atomic.AddUint64(&listener.metrics.ListQueryErrors, 1)
		accessLogger.Error("list_query failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "can't fetch metrics list"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Can't fetch metrics list: %s", err), http.StatusInternalServerError)
		return
	}

	contentType := ""
	var b []byte
	switch formatCode {
	case jsonFormat:
		b, err = json.Marshal(result)
		contentType = "application/json"
	case protoV3Format, protoV2Format:
		// TODO
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.ListQueryErrors, 1)
		accessLogger.Error("list_query failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "response encode failed"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("An internal error has occured: %s", err), http.StatusInternalServerError)
		return
	}
	wr.Header().Set("Content-Type", contentType)
	wr.Write(b)

	accessLogger.Info("list_query served",
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
}
