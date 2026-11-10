package buckyd

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// SetFiniteCacheFence is installed before Start by the daemon that owns both
// intake and persistence. Standalone services without it cannot apply patches.
func (s *Service) SetFiniteCacheFence(f chunkstore.FiniteCacheFence) { s.finiteCacheFence = f }

func (s *Service) finitePoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), 405)
		return
	}
	var p chunkstore.FinitePatch
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		writeError(w, err, 400)
		return
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		writeError(w, errors.New("one JSON request required"), 400)
		return
	}
	if err := p.Validate(); err != nil {
		writeError(w, err, 400)
		return
	}
	op := "read"
	if p.Apply {
		op = "update"
	}
	if err := s.allowedRoot(op, r); err != nil {
		writeError(w, err, 403)
		return
	}
	s.copyMu.RLock()
	defer s.copyMu.RUnlock()
	if s.StoreCopyActive() {
		writeError(w, errors.New("finite patch fenced by whole-store copy"), 409)
		return
	}
	select {
	case s.maintenance <- struct{}{}:
	default:
		writeError(w, errors.New("maintenance busy"), 429)
		return
	}
	defer func() { <-s.maintenance }()
	if s.storagePressure(s.store.Stats()) {
		writeError(w, errors.New("finite patch refused under storage pressure"), 429)
		return
	}
	result, err := s.store.PatchFinite(r.Context(), p, s.finiteCacheFence)
	// Preserve the receipt even after an ambiguous durability/hold-time error:
	// Applied=true means reconcile, never replay the write.
	status := 200
	message := ""
	if err != nil {
		status = 409
		message = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		chunkstore.FinitePatchResult
		Error string `json:"error,omitempty"`
	}{result, message})
}
