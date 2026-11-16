package chunkstore

import (
	"context"
	"errors"
	"time"
)

// ErrFrozenWindowUnsupported leaves the ordinary frozen Fetch path available
// for codecs without selective archive reads.
var ErrFrozenWindowUnsupported = errors.New("selective frozen archive read unsupported")

// ContinuationWindowCodec reads aligned slots of an explicitly selected archive.
// Covered slots in dst must remain untouched, including occupied NaN payloads.
// Every uncovered slot must be initialized, even if no legacy record exists.
type ContinuationWindowCodec interface {
	FetchWindowInto(context.Context, ContinuationIO, Metadata, int, int, int, []float64, []bool, func() time.Time) error
}

// FetchFrozenWindowInto reads only uncovered aligned slots into caller storage.
// The caller selects the archive from its ORIGINAL query bounds; shortening a
// window must never change retention selection. dst and covered are borrowed
// only until return and must not alias codec storage. A nil mask selects all.
func (s *Store) FetchFrozenWindowInto(ctx context.Context, m *FrozenMetric, archive, from, until int, dst []float64, covered []bool) error {
	if m == nil || m.store != s || !s.ReadOnly() {
		return ErrFrozenMetric
	}
	if archive < 0 || archive >= len(m.metadata.Retentions) {
		return ErrFormat
	}
	a := m.metadata.Retentions[archive]
	if a.Step <= 0 || from < 0 || until <= from || from%a.Step != 0 || until%a.Step != 0 ||
		(until-from)/a.Step != len(dst) || len(dst) > a.Count || covered != nil && len(covered) != len(dst) {
		return ErrFormat
	}
	codec, ok := s.continuation.(ContinuationWindowCodec)
	if !m.metadata.Continuation || !ok {
		return ErrFrozenWindowUnsupported
	}
	ctx, finish, err := s.beginRead(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if err := s.fileOwnershipError(); err != nil {
		return err
	}
	if err := s.checkFileOwnership(m.metadata); err != nil {
		return err
	}
	release, err := s.acquireReadSlot(ctx)
	if err != nil {
		return err
	}
	defer release()
	view := s.db.NewReadView()
	defer view.Close()
	var state *metricState
	cache := s.stateCache
	var stripe, epoch uint64
	if cache != nil {
		stripe = s.metricStripe(m.metadata.Name)
		epoch = cache.epoch(stripe)
		state = cache.get(stripe, m.metadata.Name, epoch)
	}
	io := s.newContinuationIO(ctx, view, nil, m.metadata)
	io.state = s.newStateView(state, m.metadata)
	if s.fetchSharedIterator {
		io.withIterator()
	}
	err = codec.FetchWindowInto(ctx, io, m.metadata, archive, from, until, dst, covered, s.now)
	err = errors.Join(err, io.Close())
	if err == nil && state == nil && cache != nil {
		if io.state != nil {
			cache.fill(stripe, m.metadata.Name, epoch, io.state.final(m.metadata))
		} else {
			cache.fill(stripe, m.metadata.Name, epoch, newMetricState(m.metadata).seal())
		}
	}
	return errors.Join(err, ctx.Err())
}
