package buckyd

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func (s *Service) missingOnly(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), 405)
		return
	}
	var p chunkstore.MissingPatch
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
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
		writeError(w, errors.New("MissingOnly fenced by whole-store copy"), 409)
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
		writeError(w, errors.New("MissingOnly storage pressure"), 429)
		return
	}
	result, err := s.store.PatchMissing(r.Context(), p, s.finiteCacheFence)
	status, message := 200, ""
	if err != nil {
		status, message = 409, err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		chunkstore.MissingPatchResult
		Error string `json:"error,omitempty"`
	}{result, message})
}
