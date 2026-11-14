package whisperio

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
	"math"
	"path/filepath"
	"slices"
	"time"
)

// Frozen pre-coalescing export oracle from 6784e90e1.
func (c *ContinuationCodec) legacyExportFragments(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time, priority int, selection ...store.NativeWindow) ([][]spliceFragment, bool, error) {
	fs := newContinuationFiles(kv, true).withClock(clock)
	if err := fs.prefetchNames(); err != nil {
		return nil, false, err
	}
	if len(fs.names) != 1 || fs.names[filepath.Base(fs.main())] == 0 {
		return nil, false, nil
	}
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return nil, false, err
	}
	defer w.Close()
	if err := continuationPolicy(w, m); err != nil {
		return nil, false, err
	}
	f := w.StorageFile().(*continuationFile)
	result := make([][]spliceFragment, len(m.Retentions))
	sequence := 0
	if !w.IsCompressed() {
		return c.nativeClassicFragments(ctx, kv, f, m, result, priority, &sequence)
	}
	physical, err := w.ContinuationLayout()
	if err != nil {
		return nil, false, nil
	}
	if len(selection) != 0 {
		// A newer ring owner may invalidate an old window. Let the complete
		// reference path decide when the physical bounds cannot rule that out
		// (a block's final value could itself normalize away).
		for _, block := range physical.Blocks {
			r := m.Retentions[block.Archive]
			if int64(block.Last) >= selection[0].From+int64(r.Step)*int64(r.Count) {
				return c.legacyExportFragments(ctx, kv, m, clock, priority)
			}
		}
	}
	slices.SortFunc(physical.Blocks, func(a, b whisper.ContinuationBlock) int {
		if order := cmp.Compare(a.First, b.First); order != 0 {
			return order
		}
		return cmp.Compare(a.Index, b.Index)
	})
	var sealed store.SealedPointBuffer
	var group store.PointBlock
	for _, block := range physical.Blocks {
		r := m.Retentions[block.Archive]
		if len(selection) != 0 {
			maxStep := int64(m.Retentions[len(m.Retentions)-1].Step)
			from := selection[0].From / maxStep * maxStep
			until := (selection[0].Until + maxStep - 1) / maxStep * maxStep
			if int64(block.Last) < from || int64(block.First) >= until {
				continue
			}
		}
		desc, err := f.descriptor(block.Archive, block.Index)
		if err != nil {
			return nil, false, err
		}
		if desc.count != block.Count {
			return nil, false, store.ErrFormat
		}
		visited, firstTime, lastTime := 0, int64(0), int64(0)
		err = kv.Visit(desc.groupPrefix(f.data.id, block.Archive, block.Index), func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if visited >= desc.count {
				return nil
			}
			if len(key) < 4 || int(binary.BigEndian.Uint32(key[len(key)-4:])) != visited/desc.groupSize() {
				return store.ErrFormat
			}
			payload := &splicePayload{data: value, sealed: desc.packed > 0}
			expected := min(desc.groupSize(), desc.count-visited)
			lo, hi, n, known := sealed.CanonicalRun(value, r.Step)
			if payload.sealed && known && n == expected {
				if lo <= lastTime {
					return store.ErrFormat
				}
				if firstTime == 0 {
					firstTime = lo
				}
				lastTime = hi
				sequence++
				result[block.Archive] = append(result[block.Archive], spliceFragment{first: lo, count: n, payload: payload, priority: priority, sequence: sequence})
			} else {
				var points []store.Point
				var err error
				if payload.sealed {
					points, err = sealed.Decode(value)
				} else {
					err = store.DecodePointBlock(value, &group)
					points = group.Points[:]
				}
				if err != nil {
					return err
				}
				if payload.sealed && len(points) != expected {
					return store.ErrFormat
				}
				if len(points) < expected {
					return store.ErrFormat
				}
				for _, p := range points[:expected] {
					if p.Timestamp <= lastTime {
						return store.ErrFormat
					}
					if firstTime == 0 {
						firstTime = p.Timestamp
					}
					lastTime = p.Timestamp
				}
				var ok bool
				result[block.Archive], ok = appendSplicePoints(result[block.Archive], points[:expected], payload, priority, r, &sequence)
				if !ok {
					return errSpliceUnsupported
				}
			}
			visited += expected
			return nil
		})
		if errors.Is(err, errSpliceUnsupported) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if visited != block.Count || firstTime != int64(block.First) || lastTime != int64(block.Last) {
			return nil, false, store.ErrFormat
		}
	}
	// Only buffers and the bounded coarse selection they expose need decoding.
	if len(physical.Header) < 63+128*len(result) || physical.Header[18] != 1 {
		return nil, false, nil
	}
	position := 63 + 128*len(result)
	windows := w.BufferedRollupWindows()
	for ai, a := range f.data.layout.Archives {
		position += a.BlockCount * 16
		size := 0
		if ai+1 < len(result) {
			size = m.Retentions[ai+1].Step / a.Step * 24
		}
		if position > len(physical.Header)-size {
			return nil, false, store.ErrFormat
		}
		for at := position; at < position+size; at += 12 {
			ts := int(binary.BigEndian.Uint32(physical.Header[at:]))
			if ts == 0 {
				continue
			}
			if ts%a.Step != 0 {
				return nil, false, nil
			}
			value := math.Float64frombits(binary.BigEndian.Uint64(physical.Header[at+4:]))
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, false, nil
			}
			windows = append(windows, whisper.ArchiveWindow{Archive: ai, From: ts, Until: ts + a.Step})
		}
		position += size
	}
	if position != len(physical.Header) {
		return nil, false, store.ErrFormat
	}
	fs.rangeReads = true
	for _, window := range windows {
		series, err := w.FetchArchiveWindow(window)
		if err != nil {
			return nil, false, err
		}
		if series == nil {
			continue
		}
		for i, value := range series.Values() {
			ts := int64(series.FromTime() + i*series.Step())
			if ts <= 0 || ts > math.MaxUint32 || ts%int64(m.Retentions[window.Archive].Step) != 0 {
				return nil, false, nil
			}
			if math.IsNaN(value) {
				if owner := spliceLatestInSlot(result[window.Archive], m.Retentions[window.Archive], ts); owner > 0 && owner <= ts {
					return nil, false, nil
				}
				continue
			}
			sequence++
			result[window.Archive] = append(result[window.Archive], spliceFragment{first: ts, count: 1, value: value, priority: priority + 1, sequence: sequence})
		}
	}
	return result, true, nil
}

func (c *ContinuationCodec) legacyExportWindowSnapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time, window store.NativeWindow) (store.Snapshot, []store.NativeWindowSkip, error) {
	if snap, skips, handled, err := c.seekNativeWindow(ctx, kv, m, clock, window); handled || err != nil {
		return snap, skips, err
	}
	fragments, ok, err := c.legacyExportFragments(ctx, kv, m, clock, 0, window)
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
