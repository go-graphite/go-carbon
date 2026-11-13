package whisperio

import (
	"cmp"
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// mergeClassicArchives merges a newer copy into a staged classic source in
// place. A classic archive's points are exactly its occupied ring slots, and
// want keeps one point per slot, so the merge writes each target point that
// survives in want into its slot unless the slot already holds it. Nothing is
// propagated, decoded or re-encoded beyond the touched 128-slot groups.
//
// The result is checked without reading it back: the source's points minus
// the overwritten slots plus the written points must equal want bit for bit,
// and no write may expose slots on a page that was not allocated. Only then
// are the changed keys written to dst and "" returned; otherwise nothing is
// written and the caller uses RestoreArchives.
//
// source and target are the snapshots want was built from; prepareTransfer
// calls this only when the union normalized nothing, so source holds sorted,
// unique, on-grid, non-NaN points.
func mergeClassicArchives(ctx context.Context, overlay *continuationOverlay, dst store.ContinuationIO, want, source, target store.Snapshot, clock func() time.Time) (string, error) {
	if fallback, err := mergeClassicSlots(ctx, overlay, want, source, target, clock); fallback != "" || err != nil {
		return fallback, err
	}
	return "", overlay.applyTo(dst)
}

// mergeIntoClassicTarget merges a compressed staged source into a copy of
// the classic target, the mirror of mergeClassicArchives: the target's file
// is the base, and each source point that survives in want is written into
// its slot. On success dst, which holds the staged source, is replaced by the
// target's keys plus the merge's changes. Nothing is decoded or re-encoded
// beyond the touched groups, and the same checks apply, against the target's
// points instead of the source's.
func mergeIntoClassicTarget(ctx context.Context, source, target, dst store.ContinuationIO, want, sourceSnapshot, targetSnapshot store.Snapshot, clock func() time.Time) (string, error) {
	overlay := newContinuationOverlay(target)
	if fallback, err := mergeClassicSlots(ctx, overlay, want, targetSnapshot, sourceSnapshot, clock); fallback != "" || err != nil {
		return fallback, err
	}
	// mergeClassicSlots refused any overlay deletes, so the merged file is
	// the target's keys with the overlay's values on top.
	if err := source.Visit(nil, func(key, _ []byte) error { return dst.Delete(key) }); err != nil {
		return "", err
	}
	if err := target.Visit(nil, func(key, value []byte) error {
		if _, changed := overlay.values[string(key)]; changed {
			return nil
		}
		return dst.Set(key, value)
	}); err != nil {
		return "", err
	}
	return "", overlay.applyTo(dst)
}

// mergeClassicSlots does the work of mergeClassicArchives on overlay, a
// private copy of the classic file source was read from, and leaves the
// changes there: "" means the overlay now holds want.
func mergeClassicSlots(ctx context.Context, overlay *continuationOverlay, want, source, target store.Snapshot, clock func() time.Time) (string, error) {
	fs := newContinuationFiles(overlay, false).withClock(clock)
	if err := fs.prefetchNames(); err != nil {
		return "", err
	}
	// A classic snapshot ignores sidecars, but RestoreArchives drops them and
	// later reads would not; only a lone main file merges in place.
	switch {
	case fs.names == nil:
		return store.TransferMergeFallbackClassic + "/namespace", nil
	case fs.names[filepath.Base(fs.main())] == 0:
		return store.TransferMergeFallbackClassic + "/no-main", nil
	case len(fs.names) != 1:
		return store.TransferMergeFallbackClassic + "/sidecar", nil
	}
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return "", err
	}
	defer file.Close()
	f := file.(*continuationFile)
	layout := f.data.layout
	if layout.Compressed {
		return store.TransferMergeFallbackClassic + "/compressed", nil
	}
	if f.data.references {
		return store.TransferMergeFallbackClassic + "/references", nil
	}
	if len(layout.Archives) != len(want.Archives) {
		return store.TransferMergeFallbackLayout + "/classic-archives", nil
	}
	type group struct {
		archive, index int
		block          *store.PointBlock
		written        [2]uint64
	}
	var groups []*group
	touched := map[[2]int]*group{}
	var newPages []int
	for ai, archive := range want.Archives {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		la := layout.Archives[ai]
		step, count := int64(archive.Retention.Step), archive.Retention.Count
		if la.Step != archive.Retention.Step || la.Count != count || step <= 0 || count <= 0 {
			return store.TransferMergeFallbackLayout + "/classic-retention", nil
		}
		load := func(index int) (*group, error) {
			if g := touched[[2]int{ai, index}]; g != nil {
				return g, nil
			}
			block, err := f.classicGroup(ai, la, index, true)
			if err != nil {
				return nil, err
			}
			g := &group{archive: ai, index: index, block: block}
			touched[[2]int{ai, index}] = g
			groups = append(groups, g)
			return g, nil
		}
		// Classic Fetch addresses the ring from physical slot zero.
		first, err := load(0)
		if err != nil {
			return "", err
		}
		// Classic Fetch reads the anchor from the file, so slot zero's page must
		// be allocated; an empty slot zero anchors nothing.
		var base int64
		if first.block.Present[0]&1 != 0 && f.data.pages[uint32(la.Offset/4096)] {
			base = first.block.Points[0].Timestamp
		} else if first.block.Present[0]&1 != 0 {
			return store.TransferMergeFallbackClassic + "/slot0-page", nil
		} else if len(source.Archives[ai].Points) != 0 {
			return store.TransferMergeFallbackClassic + "/slot0-empty", nil
		}
		points := want.Archives[ai].Points
		var removed, added []store.Point
		k := 0
		for _, t := range target.Archives[ai].Points {
			for k < len(points) && points[k].Timestamp < t.Timestamp {
				k++
			}
			if k == len(points) || points[k].Timestamp != t.Timestamp {
				continue // a newer source point owns the slot
			}
			p := points[k]
			if base == 0 {
				base = p.Timestamp // an empty archive anchors on its oldest point, as RestoreArchives does
			}
			if base <= 0 || base%step != 0 {
				return store.TransferMergeFallbackClassic + "/base-grid", nil
			}
			if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%step != 0 {
				return store.TransferMergeFallbackClassic + "/point-grid", nil
			}
			slot := int(((p.Timestamp-base)/step%int64(count) + int64(count)) % int64(count))
			g, err := load(slot / 128)
			if err != nil {
				return "", err
			}
			offset := slot % 128
			bit := uint64(1) << uint(offset%64)
			old := g.block.Points[offset]
			if g.block.Present[offset/64]&bit != 0 {
				if old.Timestamp == p.Timestamp && math.Float64bits(old.Value) == math.Float64bits(p.Value) {
					continue
				}
				removed = append(removed, old)
			}
			g.block.Points[offset] = p
			g.block.Present[offset/64] |= bit
			g.written[offset/64] |= bit
			added = append(added, p)
			start := la.Offset + slot*12
			for page := start / 4096; page <= (start+11)/4096; page++ {
				if !f.data.pages[uint32(page)] && !slices.Contains(newPages, page) {
					newPages = append(newPages, page)
				}
			}
		}
		slices.SortFunc(removed, func(a, b store.Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
		if !classicMergeMatches(source.Archives[ai].Points, removed, added, points) {
			return store.TransferMergeFallbackVerify + "/classic", nil
		}
	}
	// Allocating a page makes every slot on it readable. The snapshot could
	// not see slots on unallocated pages, so any occupied slot there other
	// than a written one would appear. A page can span two archives.
	for _, page := range newPages {
		for ai, la := range layout.Archives {
			lo, hi := max(page*4096, la.Offset), min((page+1)*4096, la.Offset+la.Count*12)
			if lo >= hi {
				continue
			}
			for index := (lo - la.Offset) / 12 / 128; index <= (hi-1-la.Offset)/12/128; index++ {
				var block *store.PointBlock
				var written [2]uint64
				if g := touched[[2]int{ai, index}]; g != nil {
					block, written = g.block, g.written
				} else if block, err = f.pointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, index)); err != nil {
					return "", err
				}
				for i := range 128 {
					slot := index*128 + i
					at := la.Offset + slot*12
					if slot >= la.Count || at+12 <= lo || at >= hi {
						continue
					}
					bit := uint64(1) << uint(i%64)
					if block.Present[i/64]&bit != 0 && written[i/64]&bit == 0 {
						return store.TransferMergeFallbackClassic + "/pages", nil
					}
				}
			}
		}
	}
	for _, g := range groups {
		if g.written == [2]uint64{} {
			continue
		}
		la := layout.Archives[g.archive]
		for i := range 128 {
			if g.written[i/64]&(uint64(1)<<uint(i%64)) != 0 {
				start := la.Offset + (g.index*128+i)*12
				f.allocateClassicPages(start, start+12)
			}
		}
		if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', g.archive, 0, g.index), g.block); err != nil {
			return "", err
		}
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := fs.flush(); err != nil {
		return "", err
	}
	if len(overlay.prefixes) != 0 {
		return store.TransferMergeFallbackClassic + "/prefix-deletes", nil
	}
	if len(overlay.deleted) != 0 {
		return store.TransferMergeFallbackClassic + "/deletes", nil
	}
	return "", nil
}

// classicMergeMatches reports whether source minus removed plus added equals
// want, bit for bit. source, added and want are sorted by timestamp and
// unique; removed is sorted and every removed point must be in source.
func classicMergeMatches(source, removed, added, want []store.Point) bool {
	same := func(a, b store.Point) bool {
		return a.Timestamp == b.Timestamp && math.Float64bits(a.Value) == math.Float64bits(b.Value)
	}
	i, j, k := 0, 0, 0
	drop := func() bool {
		for j < len(removed) && i < len(source) && removed[j].Timestamp <= source[i].Timestamp {
			if !same(removed[j], source[i]) {
				return false
			}
			i, j = i+1, j+1
		}
		return true
	}
	for _, w := range want {
		if !drop() {
			return false
		}
		var p store.Point
		switch {
		case i < len(source) && (k == len(added) || source[i].Timestamp < added[k].Timestamp):
			p, i = source[i], i+1
		case k < len(added) && (i == len(source) || added[k].Timestamp < source[i].Timestamp):
			p, k = added[k], k+1
		default:
			return false // exhausted, or a duplicate timestamp
		}
		if !same(p, w) {
			return false
		}
	}
	return drop() && i == len(source) && j == len(removed) && k == len(added)
}
