package chunkstore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"slices"
)

// NativeCombineClass describes logical archive work, separately from the native
// wire/SST byte counters. Slot aliases count as intersections even when their
// timestamps do not overlap. Normalization is required by fillArchive for NaNs
// and off-grid timestamps, including otherwise disjoint archives.
type NativeCombineClass struct {
	Archives, IntersectingArchives, NormalizedArchives int
	CopiedBlocks, CombinedBlocks                       int
	CopiedBytes, CombinedBytes                         uint64
}

// nativeCombineRecord runs entirely outside writer locks. Keeping the exact
// reference operations here is deliberate: coarse archives are independently
// authoritative, and cannot be recreated by replaying the finest archive.
func (s *Store) nativeCombineRecord(ctx context.Context, source, target TransferMetric) (TransferMetric, NativeCombineClass, error) {
	var class NativeCombineClass
	if !samePolicy(source.Config, target.Config) || source.Continuation != target.Continuation {
		return TransferMetric{}, class, ErrConflict
	}
	a, err := s.nativeRecordSnapshot(ctx, source)
	if err != nil {
		return TransferMetric{}, class, err
	}
	b, err := s.nativeRecordSnapshot(ctx, target)
	if err != nil {
		return TransferMetric{}, class, err
	}
	want := a
	want.Archives = make([]Archive, len(a.Archives))
	for i, archive := range a.Archives {
		if err := ctx.Err(); err != nil {
			return TransferMetric{}, class, err
		}
		class.Archives++
		intersects, normalizes := nativeArchiveIntersection(archive, b.Archives[i])
		if intersects {
			class.IntersectingArchives++
		}
		if normalizes {
			class.NormalizedArchives++
		}
		points, err := fillArchive(archive.Points, b.Archives[i].Points, archive.Retention.Step)
		if err != nil {
			return TransferMetric{}, class, err
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

func nativeClassicPointKey(key []byte) bool { return len(key) == 20 && key[0] == 'f' && key[9] == 'p' }

func nativeArchiveIntersection(source, target Archive) (intersects, normalizes bool) {
	r := source.Retention
	slots := make(map[int64]struct{}, len(source.Points))
	for _, p := range source.Points {
		normalizes = normalizes || math.IsNaN(p.Value) || p.Timestamp%int64(r.Step) != 0
		slots[p.Timestamp/int64(r.Step)%int64(r.Count)] = struct{}{}
	}
	for _, p := range target.Points {
		normalizes = normalizes || math.IsNaN(p.Value) || p.Timestamp%int64(r.Step) != 0
		_, found := slots[p.Timestamp/int64(r.Step)%int64(r.Count)]
		intersects = intersects || found
	}
	return
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
