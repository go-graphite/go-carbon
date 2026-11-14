package chunkstore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"
)

// NativeCombineClass describes logical archive work, separately from the native
// wire/SST byte counters. Slot aliases count as intersections even when their
// timestamps do not overlap. Normalization is required by fillArchive for NaNs
// and off-grid timestamps, including otherwise disjoint archives.
type NativeCombineClass struct {
	Archives, IntersectingArchives, NormalizedArchives int
	MaterializedArchives                               int
	CopiedBlocks, CombinedBlocks                       int
	CopiedBytes, CombinedBytes                         uint64
	StaleEvictedPoints                                 int // Older out-of-window ring owners displaced by a window copy.
}

type ContinuationNativeClassic interface {
	NativeClassicBlocks(context.Context, ContinuationIO, Metadata, func() time.Time) ([]RelayoutKV, bool, error)
	RestoreNativeClassicBlocks(context.Context, ContinuationIO, Metadata, []RelayoutKV, func() time.Time) (bool, error)
}

type ContinuationNativeCombiner interface {
	CombineNative(context.Context, ContinuationIO, ContinuationIO, Metadata, func() time.Time) ([]RelayoutKV, NativeCombineClass, bool, error)
}

// nativeCombineRecord runs entirely outside writer locks. Keeping the exact
// reference operations here is deliberate: coarse archives are independently
// authoritative, and cannot be recreated by replaying the finest archive.
func (s *Store) nativeCombineRecord(ctx context.Context, source, target TransferMetric) (TransferMetric, NativeCombineClass, error) {
	var class NativeCombineClass
	if !samePolicy(source.Config, target.Config) || source.Continuation != target.Continuation {
		return TransferMetric{}, class, ErrConflict
	}
	if err := s.admitNativeCombineRecords(source, target, false); err != nil {
		return TransferMetric{}, class, err
	}
	if !source.Continuation {
		result, class, canonical, err := s.nativeCombineChunks(ctx, source, target)
		if err != nil || canonical {
			return result, class, err
		}
	} else {
		if codec, ok := s.continuation.(ContinuationNativeCombiner); ok {
			keys, c, handled, err := codec.CombineNative(ctx, transferRecordIO(source), transferRecordIO(target), Metadata{MetricConfig: source.Config}, s.now)
			if err != nil || handled {
				return TransferMetric{Config: source.Config, Version: source.Version, Continuation: true, Keys: keys}, c, err
			}
		}
	}
	if err := s.admitNativeCombineRecords(source, target, true); err != nil {
		return TransferMetric{}, class, err
	}
	if source.Continuation {
		codec, ok := s.continuation.(ContinuationNativeClassic)
		if ok {
			a, ac, err := codec.NativeClassicBlocks(ctx, transferRecordIO(source), Metadata{MetricConfig: source.Config}, s.now)
			if err != nil {
				return TransferMetric{}, class, err
			}
			if ac {
				b, bc, err := codec.NativeClassicBlocks(ctx, transferRecordIO(target), Metadata{MetricConfig: target.Config}, s.now)
				if err != nil {
					return TransferMetric{}, class, err
				}
				if bc {
					left, right := source, target
					left.Keys, right.Keys = a, b
					result, c, canonical, err := s.nativeCombineChunks(ctx, left, right)
					if err != nil {
						return result, c, err
					}
					if canonical {
						memory := nativeRecordMemory{values: make(map[string][]byte)}
						compatible, err := codec.RestoreNativeClassicBlocks(ctx, &memory, Metadata{MetricConfig: source.Config}, result.Keys, s.now)
						if err != nil {
							return TransferMetric{}, c, err
						}
						if compatible {
							result.Keys, result.Continuation = nil, true
							if err := memory.Visit(nil, func(k, v []byte) error {
								result.Keys = append(result.Keys, RelayoutKV{bytes.Clone(k), v})
								return nil
							}); err != nil {
								return result, c, err
							}
							return result, c, nil
						}
					}
				}
			}
		}
	}
	var leases transferSnapshotLeases
	defer leases.close()
	unionScratch := transferUnionPool.Get().(*transferUnionScratch)
	defer unionScratch.release()
	a, err := s.nativeRecordSnapshotLeased(ctx, source, &leases)
	if err != nil {
		return TransferMetric{}, class, fmt.Errorf("native donor view %s: %w", source.Config.Name, err)
	}
	b, err := s.nativeRecordSnapshotLeased(ctx, target, &leases)
	if err != nil {
		return TransferMetric{}, class, fmt.Errorf("native survivor view %s: %w", target.Config.Name, err)
	}
	want := a
	class.MaterializedArchives = len(a.Archives)
	want.Archives = make([]Archive, len(a.Archives))
	for i, archive := range a.Archives {
		if err := ctx.Err(); err != nil {
			return TransferMetric{}, class, err
		}
		class.Archives++
		points, intersects, normalizes, err := nativeArchiveUnionInto(archive, b.Archives[i], unionScratch.buffer(i), true)
		if err != nil {
			return TransferMetric{}, class, err
		}
		unionScratch.archives[i] = points
		if intersects {
			class.IntersectingArchives++
		}
		if normalizes {
			class.NormalizedArchives++
		}
		want.Archives[i] = Archive{archive.Retention, retainTransferSlots(points, archive.Retention)}
	}
	result := TransferMetric{Config: source.Config, Version: source.Version, Continuation: source.Continuation}
	if source.Continuation {
		restorer, ok := s.continuation.(ContinuationArchiveRestorer)
		if !ok {
			return result, class, ErrFormat
		}
		memory := nativeRecordMemory{values: make(map[string][]byte)}
		if err := restorer.RestoreArchives(ctx, &memory, want, s.now); err != nil {
			return result, class, err
		}
		if err := memory.Visit(nil, func(k, v []byte) error {
			result.Keys = append(result.Keys, RelayoutKV{bytes.Clone(k), v})
			return nil
		}); err != nil {
			return result, class, err
		}
	} else {
		chunks, err := encodeSnapshotChunksWith(source.Config.Retentions, want.Archives, s.encodeChunk)
		if err != nil {
			return result, class, err
		}
		for archive, values := range chunks {
			for index, value := range values {
				key := binary.BigEndian.AppendUint16(nil, uint16(archive))
				key = binary.BigEndian.AppendUint32(key, uint32(index))
				result.Keys = append(result.Keys, RelayoutKV{key, value})
			}
		}
		slices.SortFunc(result.Keys, func(a, b RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	}
	// Reuse byte-identical encoded groups. This is also a useful diagnostic of
	// the blocks that a codec-native assembler can avoid materializing; do not
	// count headers/descriptors as copied point blocks.
	for i, kv := range result.Keys {
		if source.Continuation && !nativeClassicPointKey(kv.Key) {
			continue
		}
		copied := false
		for _, original := range []TransferMetric{source, target} {
			j, found := slices.BinarySearchFunc(original.Keys, kv.Key, func(a RelayoutKV, key []byte) int { return bytes.Compare(a.Key, key) })
			if found && bytes.Equal(original.Keys[j].Value, kv.Value) {
				result.Keys[i].Value = original.Keys[j].Value
				copied = true
				break
			}
		}
		if copied {
			class.CopiedBlocks++
			class.CopiedBytes += uint64(len(kv.Value))
		} else {
			class.CombinedBlocks++
			class.CombinedBytes += uint64(len(kv.Value))
		}
	}
	return result, class, nil
}

func (s *Store) nativeRecordSnapshotLeased(ctx context.Context, record TransferMetric, leases *transferSnapshotLeases) (Snapshot, error) {
	if !record.Continuation {
		return s.nativeRecordSnapshot(ctx, record)
	}
	m := Metadata{MetricConfig: record.Config, ID: record.Version.ID, Generation: record.Version.Generation, Continuation: true}
	return leases.snapshot(ctx, s.continuation, transferRecordIO(record), m, s.now)
}

func nativeClassicPointKey(key []byte) bool { return len(key) == 20 && key[0] == 'f' && key[9] == 'p' }

// TransferSnapshot decodes a streamed record with the same archive semantics as
// native combine. The returned snapshot must be consumed before the stream advances.
func TransferSnapshot(ctx context.Context, record TransferMetric, codec ContinuationCodec, now func() time.Time) (Snapshot, error) {
	if now == nil {
		now = time.Now
	}
	if record.Continuation && codec == nil {
		return Snapshot{}, ErrFormat
	}
	return (&Store{continuation: codec, now: now}).nativeRecordSnapshot(ctx, record)
}

func (s *Store) nativeRecordSnapshot(ctx context.Context, record TransferMetric) (Snapshot, error) {
	m := Metadata{MetricConfig: record.Config, ID: record.Version.ID, Generation: record.Version.Generation, Continuation: record.Continuation}
	if record.Continuation {
		return s.continuation.Snapshot(ctx, transferRecordIO(record), m, s.now)
	}
	result := Snapshot{Metadata: m, Archives: make([]Archive, len(m.Retentions))}
	for i, r := range m.Retentions {
		result.Archives[i].Retention = r
	}
	var c chunk
	for _, kv := range record.Keys {
		if len(kv.Key) != 6 {
			return result, ErrFormat
		}
		archive := int(binary.BigEndian.Uint16(kv.Key))
		if archive >= len(result.Archives) {
			return result, ErrFormat
		}
		if err := decodeChunk(kv.Value, &c); err != nil {
			return result, err
		}
		for slot, point := range c.Points {
			if c.has(slot) {
				result.Archives[archive].Points = append(result.Archives[archive].Points, point)
			}
		}
	}
	for i := range result.Archives {
		slices.SortFunc(result.Archives[i].Points, func(a, b Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	}
	return result, ctx.Err()
}

// One metric's writable codec scratch. Its lifetime is bounded by a streamed
// metric, rather than the size of the ordinal range.
type nativeRecordMemory struct{ values map[string][]byte }

func (m *nativeRecordMemory) Get(k []byte) ([]byte, error) {
	if v, ok := m.values[string(k)]; ok {
		return bytes.Clone(v), nil
	}
	return nil, ErrNotFound
}
func (m *nativeRecordMemory) Set(k, v []byte) error {
	m.values[string(k)] = bytes.Clone(v)
	return nil
}
func (m *nativeRecordMemory) Delete(k []byte) error { delete(m.values, string(k)); return nil }
func (m *nativeRecordMemory) DeletePrefix(prefix []byte) error {
	for k := range m.values {
		if bytes.HasPrefix([]byte(k), prefix) {
			delete(m.values, k)
		}
	}
	return nil
}
func (m *nativeRecordMemory) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	keys := make([]string, 0, len(m.values))
	for k := range m.values {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := visit([]byte(k), m.values[k]); err != nil {
			return err
		}
	}
	return nil
}

func nativeCompatibleTarget(s *Store, old Metadata, source TransferMetric) error {
	if !samePolicy(old.MetricConfig, source.Config) || old.Continuation != source.Continuation || old.FileMigration == FileCleanupPending {
		return ErrConflict
	}
	return errors.Join(s.checkFileOwnership(old))
}
