package downsize

import (
	"fmt"
	"sort"
)

// Keep the entire named frontier in ready.json without expanding an error log
// line to potentially tens of thousands of names.
type syncWindowSelectionError struct{ Residuals []syncResidual }

func (e *syncWindowSelectionError) Error() string {
	if len(e.Residuals) == 0 {
		return ErrSyncResidual.Error() + ": terminal window selection has no named evidence"
	}
	r := e.Residuals[0]
	return fmt.Sprintf("%s: %d terminal window selections; metric=%s holder=%s reason=%s; see pending-selections.json", ErrSyncResidual, len(e.Residuals), r.Name, r.Source, r.Reason)
}
func (e *syncWindowSelectionError) Unwrap() error { return ErrSyncResidual }

func (s *syncer) windowSelectionFailure(residuals []syncResidual, persist bool) error {
	seen := map[syncKey]bool{}
	var terminal []syncResidual
	names := map[string]bool{}
	for _, r := range residuals {
		if r.Reason != syncWindowSelectionReason || seen[syncKey{r.Name, r.Source}] {
			continue
		}
		seen[syncKey{r.Name, r.Source}] = true
		terminal = append(terminal, r)
		names[r.Name] = true
	}
	sort.Slice(terminal, func(i, j int) bool {
		if terminal[i].Name != terminal[j].Name {
			return terminal[i].Name < terminal[j].Name
		}
		return terminal[i].Source < terminal[j].Source
	})
	if len(terminal) == 0 {
		return fmt.Errorf("%w: window retry failed without a named frontier", ErrSyncResidual)
	}
	if persist {
		holes := make([]syncHole, len(terminal))
		for i, r := range terminal {
			holes[i] = r.syncHole
		}
		if err := s.j.append(syncRow{Kind: "window-retry-terminal", Holes: holes}); err != nil {
			return err
		}
	}
	s.j.mu.Lock()
	s.j.progress.RepairFailedNames = int64(len(names))
	s.j.mu.Unlock()
	for _, r := range terminal {
		event("sync_repair_failed", map[string]any{"level": "ERROR", "metric": r.Name, "holder": r.Source, "target": s.o.to, "reason": r.Reason, "terminal": true})
	}
	// Restore the frontier on process resume for the ordinary summary artifacts.
	kept := s.sum.residual[:0]
	for _, r := range s.sum.residual {
		if r.Reason != syncWindowSelectionReason {
			kept = append(kept, r)
		}
	}
	s.sum.residual = append(kept, terminal...)
	return &syncWindowSelectionError{Residuals: terminal}
}

// Check before discovery or checkpoint adoption as well as before a retry.
// With a quarantine ledger, an older terminal stop is superseded: its names
// are retried on fresh pins and quarantined again only if still unresolved.
func (s *syncer) savedWindowSelectionFailure() error {
	if s.o.issues != nil {
		return nil
	}
	rows := s.j.find(func(r syncRow) bool { return r.Kind == "window-retry-terminal" })
	if len(rows) == 0 {
		return nil
	}
	var residuals []syncResidual
	for _, row := range rows {
		for _, hole := range row.Holes {
			residuals = append(residuals, syncResidual{hole, syncWindowSelectionReason})
		}
	}
	return s.windowSelectionFailure(residuals, false)
}
