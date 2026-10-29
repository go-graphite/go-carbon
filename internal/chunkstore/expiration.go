package chunkstore

import (
	"context"
	"errors"
	"slices"
)

// DeleteBatchIfUnchanged commits at most 128 expiration candidates together.
// Results correspond to input order; a conflict leaves that metric untouched.
func (s *Store) DeleteBatchIfUnchanged(ctx context.Context, expected []Metadata) ([]error, error) {
	if len(expected) > 128 {
		return nil, errors.New("delete batch exceeds 128 metrics")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return nil, err
	}
	stripes := make([]uint64, 0, len(expected))
	names := make(map[string]bool, len(expected))
	for _, m := range expected {
		if names[m.Name] {
			return nil, errors.New("duplicate delete candidate")
		}
		names[m.Name] = true
		stripes = append(stripes, s.metricStripe(m.Name))
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	for _, stripe := range stripes {
		s.metricMu[stripe].Lock()
	}
	for _, m := range expected {
		s.beginStateMutation(m.Name)
	}
	defer func() {
		for _, m := range expected {
			s.finishStateMutation(m.Name)
		}
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
	}()
	results := make([]error, len(expected))
	b := s.db.NewBatch()
	defer b.Close()
	removed := make([]Metadata, 0, len(expected))
	for i, want := range expected {
		m, err := s.metadataLocked(want.Name)
		if errors.Is(err, ErrNotFound) {
			results[i] = err
			continue
		}
		if err != nil {
			return nil, err
		}
		if m.ID != want.ID || m.Generation != want.Generation || m.Revision != want.Revision || m.FileMigration == FileCleanupPending {
			results[i] = ErrConflict
			continue
		}
		if err := s.appendMetricDelete(ctx, b, m); err != nil {
			return nil, err
		}
		removed = append(removed, m)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(removed) == 0 {
		return results, nil
	}
	records := make([]SidecarRecord, len(removed))
	for i, m := range removed {
		records[i].Name = m.Name
	}
	if err := s.commitSidecarRecords(b, records, s.commit); err != nil {
		return nil, err
	}
	for _, m := range removed {
		s.catalogChanged(m.Name)
	}
	s.invalidateReadCache()
	return results, nil
}

// ExpirationPressure samples only the pressure needed by maintenance work.
// Full Stats also gathers filesystem and continuation accounting.
func (s *Store) ExpirationPressure() (l0 int64, debt uint64, stalled bool) {
	m := s.db.Metrics()
	s.stallMu.Lock()
	stalled = !s.stallSince.IsZero()
	s.stallMu.Unlock()
	return int64(m.Levels[0].Sublevels), m.Compact.EstimatedDebt, stalled
}
