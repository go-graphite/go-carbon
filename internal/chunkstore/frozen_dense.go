package chunkstore

import "context"

// FrozenDenseReadDecoder decodes into caller-owned output. Values may be reused
// by the next call; no decoder or storage view may retain the destination.
type FrozenDenseReadDecoder interface {
	FetchDense(context.Context, int, int, *Series) (bool, error)
}

// FetchFrozenDenseWithWorkspace lends output until the next call or Reset on w.
// The caller keeps w exclusive until it has finished consuming those values.
// Cancellation, ownership, admission and snapshot lifetime match ordinary Fetch.
func (s *Store) FetchFrozenDenseWithWorkspace(ctx context.Context, m *FrozenMetric, from, until int, w *FrozenReadWorkspace, consume func(int, int, int, []float64) error) error {
	return s.fetchFrozenWorkspace(ctx, m, from, until, w, true, consume)
}
