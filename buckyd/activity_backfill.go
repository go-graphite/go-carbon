package buckyd

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type ActivityBackfillRequest struct {
	Start, End time.Time
	After      string
	Limit      int
	Apply      bool
	Baselines  []chunkstore.ActivityBaseline
}

type ActivityBackfillResult struct {
	Scanned, WouldChange, Changed, Unchanged, Conflicts int
	Next                                                string
	Done                                                bool
	AgeBuckets                                          map[string]int
	Candidates                                          []chunkstore.ActivityBaseline
}

// activityBackfill is deliberately page bounded. The resumable client owns
// pacing, while the endpoint fences each mutation against its historical proof.
func (s *Service) activityBackfill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), 405)
		return
	}
	if err := s.allowedRoot("read", r); err != nil {
		writeError(w, err, 403)
		return
	}
	var request ActivityBackfillRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, err, 400)
		return
	}
	if request.Start.IsZero() || !request.End.After(request.Start) || request.End.After(time.Now()) || len(request.Baselines) > 128 || request.Limit < 0 || request.Limit > 128 {
		writeError(w, errors.New("require a past conversion window and at most 128 records"), 400)
		return
	}
	for _, baseline := range request.Baselines {
		if baseline.Name == "" || baseline.ID == 0 || baseline.Generation == 0 || baseline.Revision == 0 || baseline.LastUpdate.Before(request.Start) || baseline.LastUpdate.After(request.End) {
			writeError(w, errors.New("invalid conversion-era baseline"), 400)
			return
		}
	}
	if request.Apply {
		if err := s.allowedRoot("update", r); err != nil {
			writeError(w, err, 403)
			return
		}
		if len(request.Baselines) == 0 {
			writeError(w, errors.New("apply requires conversion-era baselines"), 400)
			return
		}
		s.copyMu.RLock()
		defer s.copyMu.RUnlock()
		if s.StoreCopyActive() {
			writeError(w, errors.New("whole-store copy is active"), 409)
			return
		}
		l0, debt, stalled := s.store.ExpirationPressure()
		if stalled || l0 >= 6 || debt >= 1<<30 {
			writeError(w, errors.New("storage pressure; retry this page"), 429)
			return
		}
	}
	if !s.acquireTransfer() {
		writeError(w, errors.New("too many concurrent maintenance requests"), 429)
		return
	}
	defer s.releaseTransfer()
	result := ActivityBackfillResult{AgeBuckets: map[string]int{}}
	baselines := request.Baselines
	if len(baselines) == 0 {
		limit := request.Limit
		if limit == 0 {
			limit = 128
		}
		page, err := s.store.ListPage(r.Context(), "", request.After, limit)
		if err != nil {
			writeError(w, err, 500)
			return
		}
		result.Done = len(page) < limit
		result.Next = request.After
		for _, m := range page {
			result.Next = m.Name
			result.Scanned++
			if m.FileMigration != chunkstore.FileReclaimed || m.LastUpdate.Before(request.Start) || m.LastUpdate.After(request.End) {
				result.Unchanged++
				continue
			}
			baselines = append(baselines, chunkstore.ActivityBaseline{Name: m.Name, ID: m.ID, Generation: m.Generation, Revision: m.Revision, LastUpdate: m.LastUpdate})
		}
	} else {
		result.Scanned = len(baselines)
		result.Done = true
	}
	for _, baseline := range baselines {
		newest, err := s.store.BackfillActivity(r.Context(), baseline, request.Start, request.End, request.Apply)
		if errors.Is(err, chunkstore.ErrConflict) || errors.Is(err, chunkstore.ErrNotFound) {
			result.Conflicts++
			continue
		}
		if err != nil {
			writeError(w, err, 500)
			return
		}
		if newest.IsZero() {
			result.Unchanged++
			continue
		}
		result.WouldChange++
		if request.Apply {
			result.Changed++
		}
		result.Candidates = append(result.Candidates, baseline)
		bucket := "under_1d"
		age := time.Since(newest)
		switch {
		case age >= 731*24*time.Hour:
			bucket = "731d_plus"
		case age >= 32*24*time.Hour:
			bucket = "32d_731d"
		case age >= 15*24*time.Hour:
			bucket = "15d_32d"
		case age >= 8*24*time.Hour:
			bucket = "8d_15d"
		case age >= 4*24*time.Hour:
			bucket = "4d_8d"
		case age >= 24*time.Hour:
			bucket = "1d_4d"
		}
		result.AgeBuckets[bucket]++
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
