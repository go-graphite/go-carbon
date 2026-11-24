package buckyd

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// Keep this allowlist independent of the general router: adding a management
// endpoint must never grant a stopped source a new operation. Releasing a pin
// changes only service memory, not the store.
func readOnlySourceRoute(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet:
		return r.URL.Path == "/v1/native/capabilities" || r.URL.Path == "/v1/transfer/status" || r.URL.Path == "/v1/transfer/list"
	case http.MethodPost:
		switch r.URL.Path {
		case "/v1/native/source", "/v1/native/export", "/v1/transfer/export", "/v1/transfer/digest", "/info/", "/render/":
			return true
		}
	case http.MethodDelete:
		return strings.HasPrefix(r.URL.Path, "/v1/native/source/") && len(strings.TrimPrefix(r.URL.Path, "/v1/native/source/")) > 0
	}
	return false
}

// These exact-name v3 adapters provide sync's metadata/point reads on the same
// authenticated listener. They use the store's readers, without a carbon App,
// file index, ingestion, expiration, or cache checkpoint lifecycle.
func (s *Service) sourceBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.URL.Query().Get("format") != "carbonapi_v3_pb" {
		http.Error(w, "carbonapi_v3_pb required", http.StatusBadRequest)
		return nil, false
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err != nil {
		writeError(w, err, bodyErrorStatus(err))
		return nil, false
	}
	if !s.acquireTransfer() {
		http.Error(w, "too many concurrent reads", http.StatusTooManyRequests)
		return nil, false
	}
	return data, true
}

func (s *Service) sourceInfo(w http.ResponseWriter, r *http.Request) {
	data, ok := s.sourceBody(w, r)
	if !ok {
		return
	}
	defer s.releaseTransfer()
	var request protov3.MultiGlobRequest
	if err := request.UnmarshalVT(data); err != nil || len(request.Metrics) > chunkstore.MaxTransferMetrics {
		http.Error(w, "invalid metadata request", http.StatusBadRequest)
		return
	}
	response := &protov3.MultiMetricsInfoResponse{}
	for _, name := range request.Metrics {
		m, err := s.store.Metadata(r.Context(), name)
		if errors.Is(err, chunkstore.ErrNotFound) {
			continue
		}
		if err != nil {
			writeError(w, err, http.StatusInternalServerError)
			return
		}
		info := &protov3.MetricsInfoResponse{Name: name, XFilesFactor: float32(m.XFilesFactor), ConsolidationFunc: strings.ToUpper(m.AggregationMethod.String()[:1]) + m.AggregationMethod.String()[1:]}
		for _, retention := range m.Retentions {
			info.MaxRetention = max(info.MaxRetention, int64(retention.Step)*int64(retention.Count))
			info.Retentions = append(info.Retentions, &protov3.Retention{SecondsPerPoint: int64(retention.Step), NumberOfPoints: int64(retention.Count)})
		}
		response.Metrics = append(response.Metrics, info)
	}
	data, err := response.MarshalVT()
	if err != nil {
		writeError(w, err, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-carbonapi-v3-pb")
	_, _ = w.Write(data)
}

func (s *Service) sourceFetch(w http.ResponseWriter, r *http.Request) {
	data, ok := s.sourceBody(w, r)
	if !ok {
		return
	}
	defer s.releaseTransfer()
	var request protov3.MultiFetchRequest
	if err := request.UnmarshalVT(data); err != nil || len(request.Metrics) > chunkstore.MaxTransferMetrics {
		http.Error(w, "invalid point request", http.StatusBadRequest)
		return
	}
	response := &protov3.MultiFetchResponse{}
	for _, metric := range request.Metrics {
		if metric == nil || metric.StartTime <= 0 || metric.StopTime <= metric.StartTime || metric.StopTime > 1<<31-1 {
			http.Error(w, "invalid point range", http.StatusBadRequest)
			return
		}
		series, err := s.store.Fetch(r.Context(), metric.Name, int(metric.StartTime), int(metric.StopTime))
		if errors.Is(err, chunkstore.ErrNotFound) {
			continue
		}
		if err != nil {
			writeError(w, err, http.StatusInternalServerError)
			return
		}
		if series == nil {
			continue
		}
		response.Metrics = append(response.Metrics, &protov3.FetchResponse{Name: metric.Name, PathExpression: metric.PathExpression, StartTime: int64(series.FromTime), StopTime: int64(series.UntilTime), StepTime: int64(series.Step), Values: series.Values})
	}
	data, err := response.MarshalVT()
	if err != nil {
		writeError(w, err, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-carbonapi-v3-pb")
	_, _ = w.Write(data)
}
