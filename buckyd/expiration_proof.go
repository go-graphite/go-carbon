package buckyd

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// ExpirationEvidence is fresh per-name evidence. Missing or pending responses
// cannot justify exclusion. Latest includes stored activity as a conservative
// upper bound as well as every finite archive point.
type ExpirationEvidence struct {
	Pending    bool
	Latest     int64
	Policy     string
	TTLSeconds int64
	Receipt    *chunkstore.ExpirationReceipt `json:",omitempty"`
}

func (s *Service) SetExpirationPolicy(fn func(string) (string, time.Duration, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirationPolicy = fn
}

func (s *Service) expirationEvidence(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), 405)
		return
	}
	if err := s.allowedRoot("read", r); err != nil {
		writeError(w, err, 403)
		return
	}
	var request struct {
		Names        []string
		ReceiptsOnly bool
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, err, 400)
		return
	}
	if len(request.Names) == 0 || len(request.Names) > 128 {
		writeError(w, errors.New("require 1..128 names"), 400)
		return
	}
	s.mu.Lock()
	policy := s.expirationPolicy
	s.mu.Unlock()
	if policy == nil {
		writeError(w, errors.New("expiration evidence unavailable"), 501)
		return
	}
	if !s.acquireTransfer() {
		writeError(w, errors.New("too many concurrent evidence requests"), 429)
		return
	}
	defer s.releaseTransfer()
	// Reloads may replace the callback while this request is reading storage.
	// Resolve it on each use so the final policy check sees that change.
	policy = func(name string) (string, time.Duration, bool) {
		s.mu.Lock()
		current := s.expirationPolicy
		s.mu.Unlock()
		if current == nil {
			return "", 0, true
		}
		return current(name)
	}
	out := map[string]ExpirationEvidence{}
	for _, name := range request.Names {
		e := ExpirationEvidence{}
		var ttl time.Duration
		e.Policy, ttl, e.Pending = policy(name)
		e.TTLSeconds = int64(ttl.Seconds())
		if request.ReceiptsOnly {
			meta, err := s.store.Metadata(r.Context(), name)
			if err == nil {
				e.Latest = max(int64(1), meta.LastUpdate.Unix())
				out[name] = e
				continue
			}
			if !errors.Is(err, chunkstore.ErrNotFound) {
				writeError(w, err, 500)
				return
			}
		}
		snapshot, err := s.store.Snapshot(r.Context(), name)
		if err != nil && !errors.Is(err, chunkstore.ErrNotFound) {
			writeError(w, err, 500)
			return
		}
		if err == nil {
			e.Latest = snapshot.Metadata.LastUpdate.Unix()
			for _, a := range snapshot.Archives {
				for _, p := range a.Points {
					if !math.IsNaN(p.Value) && !math.IsInf(p.Value, 0) {
						e.Latest = max(e.Latest, int64(p.Timestamp))
					}
				}
			}
		} else if e.Policy != "" && !e.Pending {
			receipt, err := s.store.ExpirationReceiptFor(r.Context(), name)
			if err == nil && receipt.Policy == e.Policy {
				e.Receipt = &receipt
			} else if err != nil && !errors.Is(err, chunkstore.ErrNotFound) && !errors.Is(err, chunkstore.ErrConflict) {
				writeError(w, err, 500)
				return
			}
		}
		fresh, freshErr := s.store.Metadata(r.Context(), name)
		if freshErr != nil && !errors.Is(freshErr, chunkstore.ErrNotFound) {
			writeError(w, freshErr, 500)
			return
		}
		if err == nil {
			old := snapshot.Metadata
			if freshErr != nil || fresh.ID != old.ID || fresh.Generation != old.Generation || fresh.Revision != old.Revision || !fresh.LastUpdate.Equal(old.LastUpdate) {
				e.Pending = true
				e.Receipt = nil
			}
		} else if freshErr == nil {
			e.Pending = true
			e.Receipt = nil
		}
		// Cache activity and policy can change during the storage read. Never
		// publish a positive proof from a stale policy or a newly queued write.
		p, t, pending := policy(name)
		if p != e.Policy || t != ttl || pending {
			e.Pending = true
			e.Receipt = nil
		}
		out[name] = e
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
