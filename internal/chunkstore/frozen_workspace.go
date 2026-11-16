package chunkstore

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
	"weak"
)

// FrozenReadDecoder caches virtual-file decoding state for one immutable metric.
// The supplied IO is usable only during Fetch; retaining bytes returned by Get
// and Visit is safe, but the decoder must not retain an active iterator or view.
type FrozenReadDecoder interface {
	Fetch(context.Context, int, int) (*Series, error)
	Close() error
}

// ContinuationFrozenReadCodec optionally amortizes immutable decoder setup.
// Ordinary Fetch remains the fallback for other codecs and writable stores.
type ContinuationFrozenReadCodec interface {
	NewFrozenReadDecoder(ContinuationIO, Metadata, func() time.Time) (FrozenReadDecoder, error)
}

// FrozenReadWorkspace belongs to one caller at a time. It retains no store,
// active read view, read admission or request context between Fetch calls.
// Reset releases cached decoder memory; it is not required before Store.Close.
type FrozenReadWorkspace struct {
	busy    atomic.Bool
	metric  weak.Pointer[FrozenMetric]
	io      continuationIO
	decoder FrozenReadDecoder
	output  Series
}

var ErrFrozenWorkspaceBusy = errors.New("frozen read workspace already in use")

func (w *FrozenReadWorkspace) reset() error {
	var err error
	if w.decoder != nil {
		err = w.decoder.Close()
	}
	w.metric = weak.Pointer[FrozenMetric]{}
	w.decoder = nil
	w.output = Series{}
	w.io = continuationIO{}
	return err
}

func (w *FrozenReadWorkspace) Reset() error {
	if !w.busy.CompareAndSwap(false, true) {
		return ErrFrozenWorkspaceBusy
	}
	defer w.busy.Store(false)
	return w.reset()
}

// FetchFrozenValuesWithWorkspace preserves per-call cancellation, admission,
// file ownership and view lifetime. Results remain owned by the consumer.
func (s *Store) FetchFrozenValuesWithWorkspace(ctx context.Context, m *FrozenMetric, from, until int, w *FrozenReadWorkspace, consume func(int, int, int, []float64) error) error {
	return s.fetchFrozenWorkspace(ctx, m, from, until, w, false, consume)
}

func (s *Store) fetchFrozenWorkspace(ctx context.Context, m *FrozenMetric, from, until int, w *FrozenReadWorkspace, dense bool, consume func(int, int, int, []float64) error) error {
	if m == nil || m.store != s || !s.ReadOnly() {
		return ErrFrozenMetric
	}
	codec, ok := s.continuation.(ContinuationFrozenReadCodec)
	if w == nil || !m.metadata.Continuation || !ok {
		return s.FetchFrozenValues(ctx, m, from, until, consume)
	}
	if !w.busy.CompareAndSwap(false, true) {
		return ErrFrozenWorkspaceBusy
	}
	defer w.busy.Store(false)
	if w.metric.Value() != m {
		if err := w.reset(); err != nil {
			return err
		}
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
	cache := s.stateCache
	var state *metricState
	var stripe, epoch uint64
	if cache != nil {
		stripe = s.metricStripe(m.metadata.Name)
		epoch = cache.epoch(stripe)
		state = cache.get(stripe, m.metadata.Name, epoch)
	}
	w.io = *s.newContinuationIO(ctx, view, nil, m.metadata)
	w.io.state = s.newStateView(state, m.metadata)
	if s.fetchSharedIterator {
		w.io.withIterator()
	}
	// Clear every request/generation handle before releasing admission/view.
	defer func() { _ = w.io.Close(); w.io = continuationIO{} }()
	if w.decoder == nil {
		w.decoder, err = codec.NewFrozenReadDecoder(&w.io, m.metadata, s.now)
		if err != nil {
			w.decoder = nil
			return err
		}
		w.metric = weak.Make(m)
	}
	var series *Series
	if decoder, ok := w.decoder.(FrozenDenseReadDecoder); dense && ok {
		var present bool
		present, err = decoder.FetchDense(ctx, from, until, &w.output)
		if present {
			series = &w.output
		}
	} else {
		series, err = w.decoder.Fetch(ctx, from, until)
	}
	err = errors.Join(err, w.io.Close(), ctx.Err())
	if err != nil {
		// A canceled/failed virtual-file read may leave partial lazy state.
		return errors.Join(err, w.reset())
	}
	if state == nil && cache != nil {
		if w.io.state != nil {
			cache.fill(stripe, m.metadata.Name, epoch, w.io.state.final(m.metadata))
		} else {
			cache.fill(stripe, m.metadata.Name, epoch, newMetricState(m.metadata).seal())
		}
	}
	if series == nil {
		return nil
	}
	return consume(series.FromTime, series.UntilTime, series.Step, series.Values)
}
