package whisperio

import (
	"cmp"
	"context"
	"slices"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// NativeWindowSnapshot keeps the full copy's ring owners, but decodes only
// groups intersecting the window (plus finer contributors at its boundaries).
// Noncanonical images use the same Snapshot oracle as the full native copy.
func (c *ContinuationCodec) NativeWindowSnapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time, window store.NativeWindow) (store.Snapshot, []store.NativeWindowSkip, error) {
	if snap, skips, handled, err := c.seekNativeWindow(ctx, kv, m, clock, window); handled || err != nil {
		return snap, skips, err
	}
	fragments, ok, err := c.nativeFragments(ctx, kv, m, clock, 0, window)
	if err != nil {
		return store.Snapshot{}, nil, err
	}
	if !ok {
		snapshot, err := c.Snapshot(ctx, kv, m, clock)
		if err != nil {
			return store.Snapshot{}, nil, err
		}
		return store.SelectNativeWindow(snapshot, window, clock().Unix())
	}
	snapshot := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(m.Retentions))}
	maxStep := int64(m.Retentions[len(m.Retentions)-1].Step)
	from := window.From / maxStep * maxStep
	until := (window.Until + maxStep - 1) / maxStep * maxStep
	for ai, r := range m.Retentions {
		snapshot.Archives[ai].Retention = r
		parts, _, _, err := selectSpliceOwnersAt(ctx, fragments[ai], nil, r, clock().Unix())
		if err != nil {
			return store.Snapshot{}, nil, err
		}
		var latest int64
		for _, part := range parts {
			f := part.fragment
			for i := part.ordinal; i < part.ordinal+part.count; i++ {
				latest = max(latest, spliceOrdinalTime(&f, i, r.Step))
			}
		}
		cutoff := min(latest, clock().Unix()) - int64(r.Step)*int64(r.Count)
		for _, part := range parts {
			f := part.fragment
			for i := part.ordinal; i < part.ordinal+part.count; i++ {
				ts := spliceOrdinalTime(&f, i, r.Step)
				if ts <= cutoff || ts < from || ts >= until {
					continue
				}
				point := store.Point{Timestamp: ts, Value: f.value}
				if f.payload != nil {
					points, err := splicePayloadPoints(f.payload)
					if err != nil {
						return store.Snapshot{}, nil, err
					}
					point = points[i]
				}
				snapshot.Archives[ai].Points = append(snapshot.Archives[ai].Points, point)
			}
		}
		// Owner chunks are intentionally unordered; Snapshot's normalization
		// expects timestamp order, including sparse circular wraparound.
		slices.SortFunc(snapshot.Archives[ai].Points, func(a, b store.Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	}
	return store.SelectNativeWindow(snapshot, window, clock().Unix())
}

// CombineNativeWindow reuses the encoded-group assembler when all timestamps
// are canonical. Wider histories additionally prove that every displaced
// outside owner is stale. NaNs, OOO images and ambiguous slot ownership still
// go through the archive-preserving reference path.
func (c *ContinuationCodec) CombineNativeWindow(ctx context.Context, donor, survivor store.ContinuationIO, m store.Metadata, clock func() time.Time, window store.NativeWindow) ([]store.RelayoutKV, store.NativeCombineClass, bool, error) {
	class := store.NativeCombineClass{Archives: len(m.Retentions)}
	a, ok, err := c.nativeFragments(ctx, donor, m, clock, 0)
	if err != nil || !ok {
		return nil, class, false, err
	}
	b, ok, err := c.nativeFragments(ctx, survivor, m, clock, 2)
	if err != nil || !ok {
		return nil, class, false, err
	}
	selected := make([][]splicePart, len(a))
	bases := make([]int64, len(a))
	for ai, r := range m.Retentions {
		var lo, hi int64
		for _, frags := range [][]spliceFragment{a[ai], b[ai]} {
			for _, f := range frags {
				indices := []int{f.ordinal, f.ordinal + f.count - 1}
				if f.irregular {
					indices = make([]int, f.count)
					for i := range indices {
						indices[i] = f.ordinal + i
					}
				}
				for _, i := range indices {
					ts := spliceOrdinalTime(&f, i, r.Step)
					if ts == 0 {
						continue
					}
					if lo == 0 || ts < lo {
						lo = ts
					}
					hi = max(hi, ts)
				}
			}
		}
		if hi-lo >= int64(r.Step)*int64(r.Count) {
			stale, safe, err := spliceWindowAliases(ctx, a[ai], b[ai], r, window, clock().Unix())
			if err != nil || !safe {
				return nil, class, false, err
			}
			class.StaleEvictedPoints += stale
		}
		var intersects bool
		selected[ai], bases[ai], intersects, err = selectSpliceOwnersAt(ctx, a[ai], b[ai], r, clock().Unix())
		if err != nil {
			return nil, class, false, err
		}
		if intersects {
			class.IntersectingArchives++
		}
	}
	keys, err := c.writeReferencedArchives(ctx, m, selected, bases, &class, clock)
	return keys, class, err == nil, err
}

// Prove the reference window union's outside-history guard without decoding
// values or rebuilding the survivor. Each input must have one timestamp per
// slot. Equal timestamps retain the survivor's fragment and float bits. Future
// epochs and multiple owners use the reference path rather than weakening its
// retention or conflict rules.
func spliceWindowAliases(ctx context.Context, donor, survivor []spliceFragment, r store.Retention, window store.NativeWindow, now int64) (int, bool, error) {
	incoming := make(map[int64]int64)
	for _, f := range donor {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		for i := f.ordinal; i < f.ordinal+f.count; i++ {
			ts := spliceOrdinalTime(&f, i, r.Step)
			if ts == 0 {
				continue
			}
			if ts > now || !window.Contains(ts) {
				return 0, false, nil
			}
			slot := ts / int64(r.Step) % int64(r.Count)
			if old, ok := incoming[slot]; ok && old != ts {
				return 0, false, nil
			}
			incoming[slot] = ts
		}
	}
	// Bound allocation independently of sender-controlled retention capacity.
	// Common rings need only a bitmap, not one map entry per historical point.
	var bitmap []uint64
	var sparse map[int64]bool
	if r.Count <= 1<<20 {
		bitmap = make([]uint64, (r.Count+63)/64)
	} else {
		sparse = make(map[int64]bool)
	}
	stale := 0
	for _, f := range survivor {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		for i := f.ordinal; i < f.ordinal+f.count; i++ {
			old := spliceOrdinalTime(&f, i, r.Step)
			if old == 0 {
				continue
			}
			if old > now {
				return 0, false, nil
			}
			slot := old / int64(r.Step) % int64(r.Count)
			if bitmap != nil {
				bit := uint64(1) << uint(slot%64)
				if bitmap[slot/64]&bit != 0 {
					return 0, false, nil
				}
				bitmap[slot/64] |= bit
			} else {
				if sparse[slot] {
					return 0, false, nil
				}
				sparse[slot] = true
			}
			ts, occupied := incoming[slot]
			if !occupied || old == ts || window.Contains(old) {
				continue
			}
			if !store.PreferArchiveTimestamp(ts, old, r, now) {
				return 0, false, nil
			}
			stale++
		}
	}
	return stale, true, nil
}
