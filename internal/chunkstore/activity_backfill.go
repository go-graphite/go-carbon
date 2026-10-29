package chunkstore

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
)

// ActivityBaseline must come from a conversion-era catalog snapshot, before
// shared writes resumed. Current revision numbers alone are not provenance:
// orphan reclaim and normal conversion increment them differently.
type ActivityBaseline struct {
	Name                     string
	ID, Generation, Revision uint64
	LastUpdate               time.Time
}

func (b ActivityBaseline) matches(m Metadata) bool {
	return b.Name == m.Name && b.ID == m.ID && b.Generation == m.Generation && b.Revision == m.Revision && b.LastUpdate.Equal(m.LastUpdate)
}

// BackfillActivity only lowers activity for an unchanged, reclaimed conversion.
// Reading points uses a short-lived snapshot; the final revision check is under
// the writer lock. Empty finest archives stay unchanged, never epoch-expired.
func (s *Store) BackfillActivity(ctx context.Context, baseline ActivityBaseline, start, end time.Time, apply bool) (time.Time, error) {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return time.Time{}, err
	}
	if start.IsZero() || !end.After(start) || baseline.LastUpdate.Before(start) || baseline.LastUpdate.After(end) {
		return time.Time{}, errors.New("activity must be within the conversion window")
	}
	view := s.db.NewSnapshot()
	m, err := metadataFrom(view, baseline.Name)
	if err == nil && (!baseline.matches(m) || m.FileMigration != FileReclaimed) {
		err = ErrConflict
	}
	var newest time.Time
	if err == nil {
		newest, err = s.newestFinestPoint(ctx, view, m)
	}
	err = errors.Join(err, view.Close())
	if err != nil {
		return time.Time{}, err
	}
	if newest.IsZero() || !newest.Before(m.LastUpdate) {
		return time.Time{}, nil
	}
	if !apply {
		return newest, nil
	}
	unlock := s.lockMetricMutation(m.Name)
	defer unlock()
	current, err := s.metadataLocked(m.Name)
	if err != nil {
		return time.Time{}, err
	}
	if !baseline.matches(current) || current.FileMigration != FileReclaimed {
		return time.Time{}, ErrConflict
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if current.Revision == math.MaxUint64 {
		return time.Time{}, errors.New("metric revision exhausted")
	}
	current.Revision++
	current.LastUpdate = newest
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(revisionKey(current), revisionBytes(current), nil); err != nil {
		return time.Time{}, err
	}
	s.canonicalRun(m.ID).Clear(m.ID)
	if err := s.commit(batch); err != nil {
		return time.Time{}, err
	}
	return newest, s.syncWAL()
}

func (s *Store) newestFinestPoint(ctx context.Context, reader pebble.Reader, m Metadata) (time.Time, error) {
	if m.Continuation {
		codec, ok := s.continuation.(interface {
			NewestFinestPoint(context.Context, ContinuationIO, Metadata) (time.Time, error)
		})
		if !ok {
			return time.Time{}, errors.New("continuation codec cannot read finest activity")
		}
		return codec.NewestFinestPoint(ctx, s.newContinuationIO(ctx, reader, nil, m), m)
	}
	prefix := chunkPrefixArchive(m.ID, m.Generation, 0)
	it, err := reader.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return time.Time{}, err
	}
	defer it.Close()
	var newest int64
	var c chunk
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		if err := decodeChunk(it.Value(), &c); err != nil {
			return time.Time{}, err
		}
		for i, p := range c.Points {
			if c.has(i) && p.Timestamp > newest && !math.IsNaN(p.Value) {
				newest = p.Timestamp
			}
		}
	}
	if newest == 0 {
		return time.Time{}, it.Error()
	}
	return time.Unix(newest, 0), it.Error()
}
