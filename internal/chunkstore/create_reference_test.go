package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
)

// Frozen pre-optimization Create, used as a byte-for-byte catalog/state oracle.
func (s *Store) createWithCatalogRead(ctx context.Context, config MetricConfig) (Metadata, error) {
	if err := s.fileOwnershipError(); err != nil {
		return Metadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := errors.Join(validateName(config.Name), validate(config)); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetricMutation(config.Name)
	defer unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, closer, err := s.db.Get(catalogKey(config.Name)); err == nil {
		closer.Close()
		return Metadata{}, ErrExists
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, err
	}
	id := uint64(1)
	if v, closer, err := s.db.Get(sequenceKey()); err == nil {
		if len(v) != 8 {
			closer.Close()
			return Metadata{}, errors.New("invalid catalog sequence")
		}
		previous := binary.BigEndian.Uint64(v)
		closer.Close()
		if previous == math.MaxUint64 {
			return Metadata{}, errors.New("metric IDs exhausted")
		}
		id = previous + 1
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, err
	}
	m := Metadata{MetricConfig: cloneConfig(config), ID: id, Generation: 1, Revision: 1, LastUpdate: s.nextActivity(time.Time{})}
	m.Continuation = s.continuation != nil
	if s.incrementalSource != "" {
		m.FileMigration = FileReclaimed // New metrics never had an original file.
	}
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		return Metadata{}, err
	}
	b := s.newStateBatch()
	defer b.Close()
	var state *stateView
	if m.Continuation {
		io := s.newFreshContinuationIO(ctx, b, m)
		io.state = s.newStateView(nil, m)
		state = io.state
		if err := s.continuation.Create(ctx, io, m, s.now); err != nil {
			return Metadata{}, err
		}
	}
	for _, kv := range []struct{ k, v []byte }{{catalogKey(config.Name), encoded}, {sequenceKey(), uint64Bytes(id)}, {revisionKey(m), revisionBytes(m)}} {
		if err := b.Set(kv.k, kv.v, nil); err != nil {
			return Metadata{}, err
		}
	}
	if err := s.commit(b); err != nil {
		return Metadata{}, fmt.Errorf("commit create: %w", err)
	}
	s.catalogChanged(m.Name)
	s.installState(state, m)
	return m, nil
}
