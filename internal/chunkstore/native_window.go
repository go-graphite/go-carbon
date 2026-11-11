package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
)

// NativeWindow selects archive timestamps in [From, Until), in Unix seconds.
// Nil retains the original full-retention protocol and range identities.
type NativeWindow struct{ From, Until int64 }

func (w *NativeWindow) Validate() error {
	if w != nil && (w.From < 0 || w.Until <= w.From || w.Until > math.MaxUint32+1) {
		return fmt.Errorf("invalid native time window: %w", ErrFormat)
	}
	return nil
}
func (w *NativeWindow) Contains(ts int64) bool { return w == nil || ts >= w.From && ts < w.Until }
func SameNativeWindow(a, b *NativeWindow) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// Boundary skips are part of the checksummed input and durable receipt. A
// leading bucket has an out-of-window timestamp; a trailing bucket requires
// all immediate finer contributors and an elapsed bucket before it is copied.
type NativeWindowSkip struct {
	Ordinal   int
	Archive   int
	Timestamp int64
	Reason    string
}

// ContinuationWindowReader may avoid decoding history outside the window.
// Its result must equal Snapshot normalized and ring-selected, then filtered.
type ContinuationWindowReader interface {
	NativeWindowSnapshot(context.Context, ContinuationIO, Metadata, func() time.Time, NativeWindow) (Snapshot, []NativeWindowSkip, error)
}

// SelectNativeWindow applies the full copy's normalization and ring ownership
// before filtering. Independently authoritative coarse archives are never
// reconstructed from the fine archive.
func SelectNativeWindow(snapshot Snapshot, w NativeWindow, now int64) (Snapshot, []NativeWindowSkip, error) {
	if err := w.Validate(); err != nil {
		return Snapshot{}, nil, err
	}
	result := snapshot
	result.Archives = make([]Archive, len(snapshot.Archives))
	normalized := make([]Archive, len(snapshot.Archives))
	for ai, a := range snapshot.Archives {
		points, _, err := nativeNormalizeArchive(slices.Clone(a.Points), a.Retention.Step)
		if err != nil {
			return Snapshot{}, nil, err
		}
		points = retainTransferSlots(points, a.Retention)
		if len(points) > 0 && points[len(points)-1].Timestamp >= w.From+int64(a.Retention.Step)*int64(a.Retention.Count) {
			return Snapshot{}, nil, fmt.Errorf("source archive %d has newer ring owners beyond window retention: %w", ai, ErrConflict)
		}
		normalized[ai] = Archive{a.Retention, points}
	}
	var skipped []NativeWindowSkip
	for ai, a := range normalized {
		result.Archives[ai].Retention = a.Retention
		for _, p := range a.Points {
			end := p.Timestamp + int64(a.Retention.Step)
			if p.Timestamp < w.From && end > w.From {
				skipped = append(skipped, NativeWindowSkip{Archive: ai, Timestamp: p.Timestamp, Reason: "bucket timestamp before window"})
				continue
			}
			if !w.Contains(p.Timestamp) {
				continue
			}
			if ai > 0 && end > w.Until && !completeNativeBucket(normalized[ai-1], p.Timestamp, end, now) {
				skipped = append(skipped, NativeWindowSkip{Archive: ai, Timestamp: p.Timestamp, Reason: "incomplete source boundary bucket"})
				continue
			}
			result.Archives[ai].Points = append(result.Archives[ai].Points, p)
		}
	}
	return result, skipped, nil
}
func completeNativeBucket(finer Archive, from, until, now int64) bool {
	if until > now || (until-from)%int64(finer.Retention.Step) != 0 {
		return false
	}
	i, _ := slices.BinarySearchFunc(finer.Points, from, func(p Point, ts int64) int {
		if p.Timestamp < ts {
			return -1
		}
		if p.Timestamp > ts {
			return 1
		}
		return 0
	})
	for ts := from; ts < until; ts += int64(finer.Retention.Step) {
		if i >= len(finer.Points) || finer.Points[i].Timestamp != ts || math.IsNaN(finer.Points[i].Value) {
			return false
		}
		i++
	}
	return true
}

func (s *Store) nativeWindowTransfer(ctx context.Context, view pebble.Reader, m Metadata, w NativeWindow) (TransferMetric, error) {
	var snap Snapshot
	var skips []NativeWindowSkip
	var err error
	if m.Continuation {
		kv := s.newContinuationIO(ctx, view, nil, m).withIterator()
		defer kv.Close()
		if codec, ok := s.continuation.(ContinuationWindowReader); ok {
			snap, skips, err = codec.NativeWindowSnapshot(ctx, kv, m, s.now, w)
		} else {
			return TransferMetric{}, ErrFormat
		}
	} else {
		raw, e := s.transferFromLimit(ctx, view, m, nativeMetricBytes)
		if e != nil {
			return TransferMetric{}, e
		}
		snap, err = s.nativeRecordSnapshot(ctx, raw)
		if err == nil {
			snap, skips, err = SelectNativeWindow(snap, w, s.now().Unix())
		}
	}
	if err != nil {
		return TransferMetric{}, err
	}
	record, err := s.nativeSnapshotRecord(ctx, snap, transferVersion(m), m.Continuation)
	record.WindowSkipped = skips
	if err == nil {
		err = validateNativeMemory(record)
	}
	return record, err
}

func (s *Store) nativeSnapshotRecord(ctx context.Context, snap Snapshot, version TransferVersion, continuation bool) (TransferMetric, error) {
	record := TransferMetric{Config: snap.Metadata.MetricConfig, Version: version, Continuation: continuation}
	if continuation {
		restorer, ok := s.continuation.(ContinuationArchiveRestorer)
		if !ok {
			return record, ErrFormat
		}
		memory := nativeRecordMemory{values: make(map[string][]byte)}
		if err := restorer.RestoreArchives(ctx, &memory, snap, s.now); err != nil {
			return record, err
		}
		err := memory.Visit(nil, func(k, v []byte) error { record.Keys = append(record.Keys, RelayoutKV{bytes.Clone(k), v}); return nil })
		return record, err
	}
	chunks, err := encodeSnapshotChunksWith(record.Config.Retentions, snap.Archives, s.encodeChunk)
	if err != nil {
		return record, err
	}
	for ai, values := range chunks {
		for index, value := range values {
			key := binary.BigEndian.AppendUint16(nil, uint16(ai))
			key = binary.BigEndian.AppendUint32(key, uint32(index))
			record.Keys = append(record.Keys, RelayoutKV{key, value})
		}
	}
	slices.SortFunc(record.Keys, func(a, b RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	return record, nil
}

type ContinuationWindowCombiner interface {
	CombineNativeWindow(context.Context, ContinuationIO, ContinuationIO, Metadata, func() time.Time, NativeWindow) ([]RelayoutKV, NativeCombineClass, bool, error)
}

// A patch is prepared against an immutable view and published atomically with
// the name's piece, after checking that its generation and revision still match.
type ContinuationWindowPatcher interface {
	PlanNativeWindowPatch(context.Context, ContinuationIO, Snapshot, func() time.Time, NativeWindow) ([]RelayoutKV, bool, error)
}

// Optional indexes are bound to the owning revision. An older writer may
// preserve unknown keys, but its revision advance makes their bounds unusable.
type ContinuationIndexBinder interface {
	BindIndex(key, value []byte, revision uint64) []byte
	CheckIndex(key, value []byte, revision uint64) []byte
}

func (s *Store) checkContinuationIndexes(record *TransferMetric) {
	if !record.Continuation {
		return
	}
	if codec, ok := s.continuation.(ContinuationIndexBinder); ok {
		for i := range record.Keys {
			kv := &record.Keys[i]
			kv.Value = codec.CheckIndex(kv.Key, kv.Value, record.Version.Revision)
		}
	}
}

type ContinuationRevision interface{ ContinuationRevision() uint64 }

func (s *Store) bindContinuationIndex(key, value []byte, revision uint64) []byte {
	if codec, ok := s.continuation.(ContinuationIndexBinder); ok {
		return codec.BindIndex(key, value, revision)
	}
	return value
}

// Window combine never normalizes a survivor point outside the window, even
// an explicit NaN. A ring alias that would evict such a point fails closed.
func (s *Store) nativeCombineWindowRecord(ctx context.Context, source, target TransferMetric, w NativeWindow) (TransferMetric, NativeCombineClass, error) {
	var class NativeCombineClass
	if !samePolicy(source.Config, target.Config) || source.Continuation != target.Continuation {
		return TransferMetric{}, class, ErrConflict
	}
	if err := s.admitNativeCombineRecords(source, target, false); err != nil {
		return TransferMetric{}, class, err
	}
	if source.Continuation {
		if codec, ok := s.continuation.(ContinuationWindowCombiner); ok {
			keys, c, handled, err := codec.CombineNativeWindow(ctx, transferRecordIO(source), transferRecordIO(target), Metadata{MetricConfig: source.Config}, s.now, w)
			if err != nil || handled {
				return TransferMetric{Config: source.Config, Version: source.Version, Continuation: true, Keys: keys}, c, err
			}
		}
	}
	if err := s.admitNativeCombineRecords(source, target, true); err != nil {
		return TransferMetric{}, class, err
	}
	a, err := s.nativeRecordSnapshot(ctx, source)
	if err != nil {
		return TransferMetric{}, class, err
	}
	b, err := s.nativeRecordSnapshot(ctx, target)
	if err != nil {
		return TransferMetric{}, class, err
	}
	for ai := range a.Archives {
		r := a.Archives[ai].Retention
		var inside []Point
		outside := make([]Point, 0)
		for _, p := range b.Archives[ai].Points {
			if w.Contains(p.Timestamp) {
				inside = append(inside, p)
			} else {
				outside = append(outside, p)
			}
		}
		union, _, _, err := nativeArchiveUnion(a.Archives[ai], Archive{r, inside})
		if err != nil {
			return TransferMetric{}, class, err
		}
		union = retainTransferSlots(union, r)
		union = slices.DeleteFunc(union, func(p Point) bool { return !w.Contains(p.Timestamp) })
		slots := make(map[int64]Point, len(union)+len(outside))
		for _, p := range union {
			slots[p.Timestamp/int64(r.Step)%int64(r.Count)] = p
		}
		outsideSlots := make(map[int64]bool, len(outside))
		for _, p := range outside {
			slot := p.Timestamp / int64(r.Step) % int64(r.Count)
			if outsideSlots[slot] {
				return TransferMetric{}, class, fmt.Errorf("out-of-window archive %d has ring aliases: %w", ai, ErrConflict)
			}
			outsideSlots[slot] = true
			if incoming, ok := slots[slot]; ok && incoming.Timestamp > p.Timestamp {
				return TransferMetric{}, class, fmt.Errorf("window would evict out-of-window archive %d point %d: %w", ai, p.Timestamp, ErrConflict)
			}
			slots[slot] = p
		}
		union = slices.DeleteFunc(union, func(p Point) bool { return slots[p.Timestamp/int64(r.Step)%int64(r.Count)].Timestamp != p.Timestamp })
		union = append(union, outside...)
		a.Archives[ai].Points = union
		slices.SortFunc(a.Archives[ai].Points, func(a, b Point) int {
			if a.Timestamp < b.Timestamp {
				return -1
			}
			if a.Timestamp > b.Timestamp {
				return 1
			}
			return 0
		})
	}
	class.Archives, class.MaterializedArchives = len(a.Archives), len(a.Archives)
	result, err := s.nativeSnapshotRecord(ctx, a, source.Version, source.Continuation)
	return result, class, err
}

// Reject a full-retention or mis-scoped record before staging any visible data.
func (s *Store) validateNativeWindowRecord(ctx context.Context, record TransferMetric, w *NativeWindow) error {
	if w == nil || record.NativeSkip != nil || record.Version.ID == 0 {
		return nil
	}
	snap, err := s.nativeRecordSnapshot(ctx, record)
	if err != nil {
		return err
	}
	for _, archive := range snap.Archives {
		for _, p := range archive.Points {
			if !w.Contains(p.Timestamp) {
				return fmt.Errorf("native payload point outside window: %w", ErrFormat)
			}
		}
	}
	return nil
}

// Charge only the target groups the patch can read, alongside the bounded
// donor snapshot. Keep the existing worker budget without measuring history.
func (s *Store) admitNativeWindowPatch(source TransferMetric, target MetricConfig, w NativeWindow) error {
	n, err := nativeRecordSize(source)
	if err != nil {
		return err
	}
	memory := 4*n.Bytes + 512*n.PhysicalPoints + (16 << 20)
	for _, r := range target.Retentions {
		slots := min(int64(r.Count), (w.Until-w.From+int64(r.Step)-1)/int64(r.Step)+1)
		memory += ((slots+127)/128 + 1) * 32768
	}
	if memory > s.nativeWorkerMemory() {
		n.MemoryBytes, n.Reason = memory, "window patch worker memory"
		return &n
	}
	return nil
}
