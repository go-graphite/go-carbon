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
	return s.fetchMetric(ctx, m.metadata.Name, from, until, nil, m, false)
}

// Config returns an owned copy for publication-time archive-policy validation.
func (m *FrozenMetric) Config() MetricConfig {
	if m == nil {
		return MetricConfig{}
	}
	return cloneConfig(m.metadata.MetricConfig)
}

// FetchFrozenValues passes only the render shape and owned values to consume.
// Immutable metadata stays inside the store, avoiding a retention-slice clone.
// It retains the same admission, cancellation and read-view lifetime as Fetch.
// Values belong to the consumer and remain valid after consume returns.
func (s *Store) FetchFrozenValues(ctx context.Context, m *FrozenMetric, from, until int, consume func(from, until, step int, values []float64) error) error {
	if m == nil || m.store != s || !s.ReadOnly() {
		return ErrFrozenMetric
	}
	series, err := s.fetchMetric(ctx, m.metadata.Name, from, until, nil, m, true)
	if err != nil || series == nil {
		return err
	}
	return consume(series.FromTime, series.UntilTime, series.Step, series.Values)
}
