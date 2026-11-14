package whisperio

import (
	"cmp"
	"context"
	"fmt"
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
	if snapshot, handled, err := c.boundedWindowSnapshot(ctx, kv, m, clock, window); handled || err != nil {
		if err != nil {
			return store.Snapshot{}, nil, err
		}
		return store.SelectNativeWindow(snapshot, window, clock().Unix())
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
		parts, _, _, err := selectSpliceOwners(ctx, fragments[ai], nil, r)
		if err != nil {
			return store.Snapshot{}, nil, err
		}
		for _, part := range parts {
			f := part.fragment
			for i := part.ordinal; i < part.ordinal+part.count; i++ {
				ts := spliceOrdinalTime(&f, i, r.Step)
				if ts >= window.From+int64(r.Step)*int64(r.Count) {
					return store.Snapshot{}, nil, fmt.Errorf("source archive %d has newer ring owners beyond window retention: %w", ai, store.ErrConflict)
				}
				if ts < from || ts >= until {
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
// fit one ring and are canonical. This proves that owner selection cannot
// evict a different timestamp outside the window. NaNs, OOO images and wider
// histories go through the archive-preserving reference path.
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
			return nil, class, false, nil
		}
		var intersects bool
		selected[ai], bases[ai], intersects, err = selectSpliceOwners(ctx, a[ai], b[ai], r)
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
