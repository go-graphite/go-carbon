package buckyd

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
)

// sidecars keeps remote sidecar maintenance on the authenticated store boundary.
// The store retains per-metric stripes, atomic batches and inventory locking.
func (s *Service) sidecars(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		writeError(w, errors.New("GET or POST required"), 405)
		return
	}
	q := r.URL.Query()
	dry := false
	var err error
	if q.Has("dry-run") {
		dry, err = strconv.ParseBool(q.Get("dry-run"))
		if err != nil {
			writeError(w, err, 400)
			return
		}
	}
	op := "read"
	if r.Method == http.MethodPost && !dry {
		op = "update"
	}
	if err = s.allowedRoot(op, r); err != nil {
		writeError(w, err, 403)
		return
	}
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.store.Sidecars())
		return
	}
	limit := 1000
	if q.Has("limit") {
		limit, err = strconv.Atoi(q.Get("limit"))
		if err != nil {
			writeError(w, err, 400)
			return
		}
	}
	rate := float64(0)
	if q.Has("rate") {
		rate, err = strconv.ParseFloat(q.Get("rate"), 64)
		if err != nil {
			writeError(w, err, 400)
			return
		}
	}
	if limit < 1 || limit > 10000 || rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		writeError(w, errors.New("invalid limit or rate"), 400)
		return
	}
	if q.Get("action") != "scan" && q.Get("action") != "fold" {
		writeError(w, errors.New("action must be scan or fold"), 400)
		return
	}
	if !dry {
		s.copyMu.RLock()
		defer s.copyMu.RUnlock()
		if s.StoreCopyActive() {
			writeError(w, errors.New("maintenance fenced by whole-store copy"), 409)
			return
		}
	}
	select {
	case s.maintenance <- struct{}{}:
	default:
		writeError(w, errors.New("too many maintenance requests"), 429)
		return
	}
	defer func() { <-s.maintenance }()
	var result any
	if q.Get("action") == "scan" {
		result, err = s.store.ScanSidecars(r.Context(), q.Get("after"), limit, !dry)
	} else {
		result, err = s.store.FoldSidecars(r.Context(), q.Get("after"), limit, dry, rate)
	}
	if err != nil {
		writeError(w, err, 409)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
