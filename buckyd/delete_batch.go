package buckyd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

const (
	deleteBatchEndpoint = "/v1/delete"
	// MaxDeleteBatch is the store's atomic delete batch, shared with expiration.
	MaxDeleteBatch     = 512
	maxDeleteBatchBody = 8 << 20
)

// DeleteBatchRequest names the metrics one POST /v1/delete removes. A set
// Version must equal the metric's StorageVersion, as DELETE
// /metrics/{name}?version= requires.
type DeleteBatchRequest struct {
	Metrics []DeleteBatchMetric `json:"metrics"`
}

type DeleteBatchMetric struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// DeleteBatchResult reports one requested metric, in request order, with the
// status DELETE /metrics/{name} returns for it: 200, 403, 404, 409 or 500.
type DeleteBatchResult struct {
	Name   string `json:"name"`
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

type DeleteBatchResponse struct {
	Results []DeleteBatchResult `json:"results"`
}

// deleteBatch deletes up to MaxDeleteBatch metrics in one atomic store commit,
// instead of one request and one commit per metric. Deletes add compaction
// work, so the request backs off with 429 under the pressure that pauses
// transfers: a write stall, L0 near stop-writes or transfer-max-compaction-debt.
func (s *Service) deleteBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errors.New("bad request method"), http.StatusMethodNotAllowed)
		return
	}
	s.copyMu.RLock()
	defer s.copyMu.RUnlock()
	if s.storeCopy != nil && s.storeCopy.Active() {
		writeError(w, errors.New("administrative metric mutations are fenced by whole-store copy"), http.StatusConflict)
		return
	}
	var claims *ACL
	if len(s.secret) > 0 {
		var err error
		if claims, err = s.authorizationClaims(r); err != nil {
			writeError(w, err, http.StatusForbidden)
			return
		}
	}
	var req DeleteBatchRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxDeleteBatchBody)).Decode(&req); err != nil {
		writeError(w, err, bodyErrorStatus(err))
		return
	}
	if err := validDeleteBatch(req.Metrics); err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	if s.storagePressure(s.store.Stats()) {
		w.Header().Set("Retry-After", "1")
		writeError(w, errors.New("storage is under compaction pressure; retry later"), http.StatusTooManyRequests)
		return
	}
	if !s.acquireTransfer() {
		writeError(w, errors.New("too many concurrent transfers"), http.StatusTooManyRequests)
		return
	}
	defer s.releaseTransfer()

	results := make([]DeleteBatchResult, len(req.Metrics))
	expected := make([]chunkstore.Metadata, 0, len(req.Metrics))
	pending := make([]int, 0, len(req.Metrics))
	for i, want := range req.Metrics {
		results[i] = DeleteBatchResult{Name: want.Name, Status: http.StatusOK}
		if claims != nil {
			if err := claims.allows(want.Name, "delete"); err != nil {
				results[i].Status, results[i].Error = http.StatusForbidden, err.Error()
				continue
			}
		}
		m, err := s.store.Metadata(r.Context(), want.Name)
		if err != nil {
			results[i].Status, results[i].Error = deleteErrorStatus(err), err.Error()
			continue
		}
		if want.Version != "" && want.Version != version(m) {
			results[i].Status, results[i].Error = http.StatusConflict, "storage version conflict"
			continue
		}
		expected = append(expected, m)
		pending = append(pending, i)
	}
	if len(expected) > 0 {
		outcomes, err := s.store.DeleteMetricsIfUnchanged(r.Context(), expected)
		if err != nil {
			writeError(w, err, http.StatusInternalServerError)
			return
		}
		deleted := false
		for j, i := range pending {
			if outcomes[j] == nil {
				deleted = true
				continue
			}
			results[i].Status, results[i].Error = deleteErrorStatus(outcomes[j]), outcomes[j].Error()
		}
		if deleted {
			s.changed()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(DeleteBatchResponse{Results: results})
}

func validDeleteBatch(metrics []DeleteBatchMetric) error {
	if len(metrics) == 0 || len(metrics) > MaxDeleteBatch {
		return fmt.Errorf("a delete batch names 1 to %d metrics, not %d", MaxDeleteBatch, len(metrics))
	}
	seen := make(map[string]bool, len(metrics))
	for _, m := range metrics {
		if m.Name == "" || strings.Contains(m.Name, "/") {
			return fmt.Errorf("invalid metric name %q", m.Name)
		}
		if seen[m.Name] {
			return fmt.Errorf("metric %q is named twice", m.Name)
		}
		seen[m.Name] = true
	}
	return nil
}

func deleteErrorStatus(err error) int {
	switch {
	case errors.Is(err, chunkstore.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, chunkstore.ErrConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
