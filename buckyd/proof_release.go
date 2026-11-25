package buckyd

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// mutationProofs is an explicit maintenance boundary. Unlike legacy APIs it
// requires a configured secret even for standalone services.
func (s *Service) mutationProofs(w http.ResponseWriter, r *http.Request) {
	fail := func(err error, status int) {
		zapwriter.Logger("buckyd").Error("mutation-proof maintenance failed", zap.Error(err))
		writeError(w, err, status)
	}
	if r.Method != http.MethodPost {
		fail(errors.New("POST required"), 405)
		return
	}
	if len(s.secret) == 0 {
		fail(errors.New("mutation-proof maintenance requires JWT authentication"), 403)
		return
	}
	if err := s.allowedRoot("update", r); err != nil {
		fail(err, 403)
		return
	}
	action := r.URL.Query().Get("action")
	if action != "release" && action != "reset" {
		fail(errors.New("action must be release or reset"), 400)
		return
	}
	var request struct{ Names []chunkstore.ProofRelease }
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&request); err != nil {
		fail(err, bodyErrorStatus(err))
		return
	}
	if err := d.Decode(new(any)); err != io.EOF {
		fail(errors.New("trailing JSON"), 400)
		return
	}
	if action == "reset" && len(request.Names) != 0 || action == "release" && (len(request.Names) == 0 || len(request.Names) > 1000) {
		fail(errors.New("release requires 1..1000 names; reset requires no names"), 400)
		return
	}
	for _, name := range request.Names {
		if err := name.Validate(); err != nil {
			fail(err, 400)
			return
		}
	}
	s.copyMu.RLock()
	defer s.copyMu.RUnlock()
	if s.StoreCopyActive() {
		fail(errors.New("maintenance fenced by whole-store copy"), 409)
		return
	}
	select {
	case s.maintenance <- struct{}{}:
	default:
		fail(errors.New("too many maintenance requests"), 429)
		return
	}
	defer func() { <-s.maintenance }()
	var result any
	if action == "reset" {
		receipt, err := s.store.ResetMutationProofs(r.Context())
		if err != nil {
			fail(err, 500)
			return
		}
		zapwriter.Logger("buckyd").Warn("all mutation-proof history reset", zap.Time("finished", receipt.Finished))
		result = receipt
	} else {
		results := s.store.ReleaseMutationProofs(r.Context(), request.Names)
		for _, v := range results {
			if v.Status == "error" || v.Status == "conflict" {
				zapwriter.Logger("buckyd").Error("mutation-proof release failed", zap.String("name", v.Name), zap.Any("result", v))
			}
		}
		result = results
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
