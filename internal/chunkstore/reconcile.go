package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"math"

	"github.com/cockroachdb/pebble"
)

// ReconcileSnapshot publishes an authoritative physical archive image. With an
// unchanged policy it preserves the generation and writes only changed chunks.
// Readers see one atomic batch. Replace retains its unconditional replacement
// contract; missing, incompatible or undecodable images use that same path.
func (s *Store) ReconcileSnapshot(ctx context.Context, source Snapshot) (Metadata, error) {
	return s.reconcileSnapshot(ctx, source, nil)
}

// ReconcileSnapshotStatus also certifies an unchanged canonical archive image.
// True means every source point and stored chunk matched without any write;
// changed images still require readback when the caller requests verification.
func (s *Store) ReconcileSnapshotStatus(ctx context.Context, source Snapshot) (Metadata, bool, error) {
	var unchanged bool
	metadata, err := s.reconcileSnapshot(ctx, source, &unchanged)
	return metadata, unchanged, err
}

func (s *Store) reconcileSnapshot(ctx context.Context, source Snapshot, unchanged *bool) (Metadata, error) {
	if s.continuation != nil || source.Metadata.Continuation {
		return Metadata{}, ErrFormat
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validateSnapshot(source); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetricMutation(source.Metadata.Name)
	defer unlock()
	m, err := s.metadataLocked(source.Metadata.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Metadata{}, err
	}
	full := func() (Metadata, error) {
		if e := ctx.Err(); e != nil {
			return Metadata{}, e
		}
		if err == nil && (m.Generation == math.MaxUint64 || m.Revision == math.MaxUint64) {
			return Metadata{}, errors.New("metric generation or revision exhausted")
		}
		chunks, e := encodeSnapshotChunksWith(source.Metadata.Retentions, source.Archives, s.encodeChunk)
		if e != nil {
			return Metadata{}, e
		}
		if e = ctx.Err(); e != nil {
			return Metadata{}, e
		}
		return s.replaceCatalog(source.Metadata, chunks, false)
	}
	if errors.Is(err, ErrNotFound) || !samePolicy(m.MetricConfig, source.Metadata.MetricConfig) {
		return full()
	}
	b := s.db.NewBatch()
	defer b.Close()
	var compared, written, deleted uint64
	canonical := true
	for archive, a := range source.Archives {
		desired, ordered, e := snapshotArchiveChunksStatus(m.Retentions[archive], a)
		canonical = canonical && ordered
		if e != nil {
			return Metadata{}, e
		}
		prefix := chunkPrefixArchive(m.ID, m.Generation, archive)
		it, e := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if e != nil {
			return Metadata{}, e
		}
		var current chunk
		fallback := false
		for it.First(); it.Valid(); it.Next() {
			if e = ctx.Err(); e != nil {
				break
			}
			key := it.Key()
			if len(key) != 23 {
				fallback = true
				break
			}
			index := int(binary.BigEndian.Uint32(key[19:]))
			want := desired[index]
			compared++
			if want == nil {
				e = b.Delete(key, nil)
				deleted++
			} else {
				if e = decodeChunk(it.Value(), &current); e != nil {
					fallback = true
					break
				}
				if !sameChunkValues(&current, want) {
					e = b.Set(key, s.encodeChunk(want), nil)
					written++
				}
				delete(desired, index)
			}
			if e != nil {
				break
			}
		}
		iterationErr := it.Error()
		closeErr := it.Close()
		if fallback || errors.Is(iterationErr, errInvalidChunk) || errors.Is(iterationErr, errTooManyDeltas) {
			// A bad logical chunk can be repaired from the authoritative image.
			// Storage I/O errors still abort instead of being hidden by repair.
			for _, e := range []error{iterationErr, closeErr} {
				if e != nil && !errors.Is(e, errInvalidChunk) && !errors.Is(e, errTooManyDeltas) {
					return Metadata{}, e
				}
			}
			return full()
		}
		if e = errors.Join(e, iterationErr, closeErr); e != nil {
			return Metadata{}, e
		}
		for index, want := range desired {
			if e = b.Set(chunkKey(m, archive, index), s.encodeChunk(want), nil); e != nil {
				return Metadata{}, e
			}
			written++
		}
	}
	s.reconcileCompared.Add(compared)
	if written+deleted == 0 {
		if unchanged != nil {
			*unchanged = canonical
		}
		s.reconcileUnchanged.Add(1)
		return m, nil
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if m.Revision == math.MaxUint64 {
		return Metadata{}, errors.New("metric revision exhausted")
	}
	m.Revision++
	m.LastUpdate = s.nextActivity(m.LastUpdate)
	if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return Metadata{}, err
	}
	s.canonicalRun(m.ID).Clear(m.ID)
	if err := s.commit(b); err != nil {
		return Metadata{}, err
	}
	s.reconcileWritten.Add(written)
	s.reconcileDeleted.Add(deleted)
	s.invalidateReadCache()
	return m, nil
}

// Delta counters and unused slots are encoding details; visible slots retain
// exact timestamps and Float64 bits, including signed zero and NaN payloads.
func sameChunkValues(a, b *chunk) bool {
	if a.Present != b.Present {
		return false
	}
	for slot := range a.Points {
		if a.has(slot) && (a.Points[slot].Timestamp != b.Points[slot].Timestamp || math.Float64bits(a.Points[slot].Value) != math.Float64bits(b.Points[slot].Value)) {
			return false
		}
	}
	return true
}
