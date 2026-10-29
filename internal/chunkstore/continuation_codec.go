package chunkstore

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// ContinuationCodec owns source-writer compatibility. The engine retains its
// exact point encoding, atomic transactions and ownership; the adapter retains
// CWhisper buffers/watermarks and its read/write rules without importing its
// filesystem representation into the storage engine.
type ContinuationCodec interface {
	Create(context.Context, ContinuationIO, Metadata, func() time.Time) error
	Import(context.Context, ContinuationIO, string, Metadata, func() time.Time) error
	Verify(context.Context, ContinuationIO, string, Metadata) error
	Update(context.Context, ContinuationIO, Metadata, []Point, int, func() time.Time, *ContinuationMaintenance) error
	Fetch(context.Context, ContinuationIO, Metadata, int, int, func() time.Time) (*Series, error)
	Snapshot(context.Context, ContinuationIO, Metadata, func() time.Time) (Snapshot, error)
}

// ContinuationVerifier verifies physical writer state and returns its visible
// snapshot. It may reuse a source view only after proving full physical equality
// against independently decoded kv, never the import's in-memory output. kv is
// one immutable database snapshot.
type ContinuationVerifier interface {
	VerifySnapshot(context.Context, ContinuationIO, string, Metadata, func() time.Time) (Snapshot, error)
}

// ContinuationMaintenance shares the persister's existing nonblocking OOO
// compaction budget with metrics that still use original files.
type ContinuationMaintenance struct {
	MinBytes                     int64
	MinPoints                    int
	MaxPointAge, RetentionMargin time.Duration
	Allow                        func() bool
	Result                       func(bool, error)
}

// updateContinuation updates m without the metadata cache: the mutation ends
// by forgetting m's cached state.
func (s *Store) updateContinuation(ctx context.Context, m Metadata, input []Point, retention int, maintenance *ContinuationMaintenance) error {
	return s.updateContinuationFrom(ctx, m, nil, false, input, retention, maintenance)
}

// updateContinuationFrom updates m, whose stripe the caller holds in a
// mutation. With cache, it starts from m's cached state (nil when unknown) and
// records the state it commits; the caller must commit nothing else in that
// mutation.
func (s *Store) updateContinuationFrom(ctx context.Context, m Metadata, state *metricState, cache bool, input []Point, retention int, maintenance *ContinuationMaintenance) error {
	if s.continuation == nil {
		return ErrFormat
	}
	batch := s.db.NewIndexedBatch()
	defer batch.Close()
	m, view, sidecar, changed, err := s.prepareContinuationUpdate(ctx, batch, m, state, cache, input, retention, maintenance)
	if err != nil {
		return err
	}
	if changed {
		stagetime.FromContext(ctx).Enter(stagetime.Commit)
		if err := s.commitSidecars(batch, m.Name, sidecar, s.commit); err != nil {
			return err
		}
	}
	if cache {
		s.installState(view, m)
	}
	return nil
}

// prepareContinuationUpdate stages one metric without publishing its cache
// state. The caller holds its stripe until the enclosing batch commits.
func (s *Store) prepareContinuationUpdate(ctx context.Context, batch *pebble.Batch, m Metadata, state *metricState, cache bool, input []Point, retention int, maintenance *ContinuationMaintenance) (Metadata, *stateView, *SidecarSummary, bool, error) {
	io := s.newContinuationIO(ctx, batch, batch, m)
	if cache {
		io.state = s.newStateView(state, m)
	}
	defer io.Close()
	if !s.updatePointLookups.Load() {
		io.withIterator()
	}
	if err := s.continuation.Update(ctx, io, m, input, retention, s.now, maintenance); err != nil {
		return m, nil, nil, false, err
	}
	if err := io.Close(); err != nil {
		return m, nil, nil, false, err
	}
	if io.changed {
		if m.Revision == math.MaxUint64 {
			return m, nil, nil, false, errors.New("metric revision exhausted")
		}
		m.Revision++
		m.LastUpdate = s.nextActivity(m.LastUpdate)
		if err := batch.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
			return m, nil, nil, false, err
		}
	}
	return m, io.state, io.sidecar, io.changed, nil
}

func (s *Store) continuationSnapshot(ctx context.Context, reader pebble.Reader, m Metadata) (Snapshot, error) {
	if s.continuation == nil {
		return Snapshot{}, ErrFormat
	}
	return s.continuation.Snapshot(ctx, s.newContinuationIO(ctx, reader, nil, m), m, s.now)
}

// VerifyContinuationSource checks physical writer state as well as values.
// The caller must fence or supply an immutable source pair, and keep replay
// paused while taking this database snapshot and verifying it.
func (s *Store) VerifyContinuationSource(ctx context.Context, name, path string) error {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return err
	}
	view := s.db.NewSnapshot()
	defer view.Close()
	m, err := metadataFrom(view, name)
	if err != nil {
		return err
	}
	if err := s.checkFileOwnership(m); err != nil {
		return err
	}
	if !m.Continuation || s.continuation == nil {
		return ErrFormat
	}
	return s.continuation.Verify(ctx, s.newContinuationIO(ctx, view, nil, m), path, m)
}
