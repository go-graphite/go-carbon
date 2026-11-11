package chunkstore

import (
	"context"
	"fmt"
	"slices"
)

// Frozen pre-fix oracle from upstream bfe8cd566. Keep independent of the production guard
// so non-aliasing behavior is compared byte-for-byte on identical inputs.
func (s *Store) legacyNativeCombineWindowRecord(ctx context.Context, source, target TransferMetric, w NativeWindow) (TransferMetric, NativeCombineClass, error) {
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
