package buckyd

import (
	"net/http"

	"github.com/go-graphite/go-carbon/internal/storecopy"
)

func (s *Service) copyHandler(w http.ResponseWriter, r *http.Request) {
	if err := s.allowed("*", "admin", r); err != nil {
		writeError(w, err, http.StatusForbidden)
		return
	}
	if s.storeCopy == nil {
		http.Error(w, "whole-store copy is unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.URL.Path != storecopy.Endpoint+"/file" {
		// Drain already admitted administrative mutations before checkpointing.
		s.copyMu.Lock()
		defer s.copyMu.Unlock()
	}
	s.storeCopy.ServeHTTP(w, r)
}
