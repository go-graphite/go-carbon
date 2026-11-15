package chunkstore

import (
	"context"
	"errors"
)

var ErrFrozenMetric = errors.New("metadata does not belong to this read-only store")

// FrozenMetric binds immutable metadata to one open read-only store. Its owner
// must retain that store until all readers drain. Returned metadata is never
// exposed to callers or accepted for a writable store.
type FrozenMetric struct {
	store    *Store
	metadata Metadata
}

func (s *Store) ResolveFrozen(ctx context.Context, name string) (*FrozenMetric, error) {
	if !s.ReadOnly() {
		return nil, ErrFrozenMetric
	}
	m, err := s.Metadata(ctx, name)
	if err != nil {
		return nil, err
	}
	return &FrozenMetric{store: s, metadata: m}, nil
}

// FetchFrozen preserves normal admission, snapshot and close handling while
// reusing metadata resolved at immutable catalog publication.
func (s *Store) FetchFrozen(ctx context.Context, m *FrozenMetric, from, until int) (*Series, error) {
	if m == nil || m.store != s || !s.ReadOnly() {
		return nil, ErrFrozenMetric
	}
	return s.fetchMetric(ctx, m.metadata.Name, from, until, nil, m)
}

// Config returns an owned copy for publication-time archive-policy validation.
func (m *FrozenMetric) Config() MetricConfig {
	if m == nil {
		return MetricConfig{}
	}
	return cloneConfig(m.metadata.MetricConfig)
}
