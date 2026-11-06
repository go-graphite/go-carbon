package buckyd

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// archiveAnchors inspects at most 32 explicitly named metrics. Pagination and
// durable per-name progress belong to the caller; repeating a successful name
// is a no-op. A read token can only dry-run. No cache drain is requested.
func (s *Service) archiveAnchors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), 405)
		return
	}
	var request struct {
		Names []string `json:"names"`
		Apply bool     `json:"apply"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, err, 400)
		return
	}
	if len(request.Names) == 0 || len(request.Names) > 32 {
		writeError(w, errors.New("anchor maintenance needs 1..32 explicit names"), 400)
		return
	}
	op := "read"
	if request.Apply {
		op = "update"
	}
	if err := s.allowedRoot(op, r); err != nil {
		writeError(w, err, 403)
		return
	}
	if request.Apply {
		s.copyMu.RLock()
		defer s.copyMu.RUnlock()
		if s.StoreCopyActive() {
			writeError(w, errors.New("maintenance fenced by whole-store copy"), 409)
			return
		}
	}
	// Resize controllers can occupy every transfer slot continuously. Keep
	// repair admission independent and bounded, including while replying.
	select {
	case s.maintenance <- struct{}{}:
	default:
		writeError(w, errors.New("too many concurrent maintenance requests"), 429)
		return
	}
	defer func() { <-s.maintenance }()
	type row struct {
		chunkstore.AnchorRepairResult
		Error string `json:"error,omitempty"`
	}
	rows := make([]row, 0, len(request.Names))
	for _, name := range request.Names {
		result, err := s.store.RepairArchiveAnchors(r.Context(), name, request.Apply)
		result.Name = name
		item := row{AnchorRepairResult: result}
		if err != nil {
			item.Error = err.Error()
		}
		rows = append(rows, item)
		if r.Context().Err() != nil {
			break
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rows)
}
