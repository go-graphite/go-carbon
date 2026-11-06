package whisperio

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

var errSpliceUnsupported = errors.New("unsupported native splice group")

func appendSplicePoints(dst []spliceFragment, points []store.Point, payload *splicePayload, priority int, r store.Retention, sequence *int) ([]spliceFragment, bool) {
	dense := payload != nil && len(points) > 0
	first := int64(0)
	for i, p := range points {
		if p.Timestamp > 0 && (math.IsNaN(p.Value) || math.IsInf(p.Value, 0)) {
			return nil, false
		}
		if p.Timestamp == 0 || math.IsNaN(p.Value) {
			dense = false
			continue
		}
		if p.Timestamp < 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(r.Step) != 0 {
			return nil, false
		}
		if first == 0 {
			first = p.Timestamp
		}
		if p.Timestamp != points[0].Timestamp+int64(i)*int64(r.Step) {
			dense = false
		}
	}
	if first == 0 {
		return dst, true
	}
	if payload != nil {
		// Dense timestamps are implicit. Borrow decoder scratch for the
		// validation pass; only sparse groups retain points for slot selection.
		if !dense && payload.points == nil {
			payload.points = slices.Clone(points)
		}
		(*sequence)++
		return append(dst, spliceFragment{first: first, count: len(points), payload: payload, priority: priority, sequence: *sequence, irregular: !dense}), true
	}
	for i, p := range points {
		if p.Timestamp > 0 && !math.IsNaN(p.Value) {
			(*sequence)++
			dst = append(dst, spliceFragment{first: p.Timestamp, ordinal: i, count: 1, value: p.Value, priority: priority, sequence: *sequence})
		}
	}
	return dst, true
}

func splicePayloadPoints(payload *splicePayload) ([]store.Point, error) {
	if payload.points != nil {
		return payload.points, nil
	}
	if payload.sealed {
		points, err := store.DecodeSealedPoints(payload.data)
		if err != nil {
			return nil, err
		}
		payload.points = points
	} else {
		var group store.PointBlock
		if err := store.DecodePointBlock(payload.data, &group); err != nil {
			return nil, err
		}
		payload.points = make([]store.Point, 128)
		for i, p := range group.Points {
			if group.Present[i/64]&(1<<uint(i%64)) != 0 {
				payload.points[i] = p
			}
		}
	}
	return payload.points, nil
}

func (c *ContinuationCodec) nativeFragments(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time, priority int) ([][]spliceFragment, bool, error) {
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
	// Only buffers and the bounded coarse windows they expose need decoding.
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
			if math.IsNaN(value) {
				ts := int64(series.FromTime() + i*series.Step())
				if owner := spliceLatestInSlot(result[window.Archive], m.Retentions[window.Archive], ts); owner > 0 && owner <= ts {
					return nil, false, nil
				}
				continue
			}
			sequence++
			result[window.Archive] = append(result[window.Archive], spliceFragment{first: int64(series.FromTime() + i*series.Step()), count: 1, value: value, priority: priority + 1, sequence: sequence})
		}
	}
	return result, true, nil
}

// A NaN render cannot distinguish an absent rollup from a computed NaN that
// masks an older physical owner. Require the reference snapshot in that case.
func spliceLatestInSlot(fragments []spliceFragment, r store.Retention, timestamp int64) int64 {
	slot := timestamp / int64(r.Step) % int64(r.Count)
	var latest int64
	for _, f := range fragments {
		if f.irregular {
			for _, p := range f.payload.points[f.ordinal : f.ordinal+f.count] {
				if p.Timestamp > latest && p.Timestamp/int64(r.Step)%int64(r.Count) == slot {
					latest = p.Timestamp
				}
			}
			continue
		}
		offset := (slot - f.first/int64(r.Step)%int64(r.Count) + int64(r.Count)) % int64(r.Count)
		if offset >= int64(f.count) {
			continue
		}
		offset += (int64(f.count) - 1 - offset) / int64(r.Count) * int64(r.Count)
		latest = max(latest, f.first+offset*int64(r.Step))
	}
	return latest
}

func (c *ContinuationCodec) nativeClassicFragments(ctx context.Context, kv store.ContinuationIO, f *continuationFile, m store.Metadata, result [][]spliceFragment, priority int, sequence *int) ([][]spliceFragment, bool, error) {
	var scratch store.PointBlock
	for ai, r := range m.Retentions {
		anchor, err := f.classicGroup(ai, f.data.layout.Archives[ai], 0, false)
		if err != nil {
			return nil, false, err
		}
		base := anchor.Points[0].Timestamp
		add := func(index int, payload *splicePayload, ordinal, destination, count int, borrowed bool) error {
			var points []store.Point
			var err error
			if borrowed {
				err = store.DecodePointBlock(payload.data, &scratch)
				points = scratch.Points[:]
			} else {
				points, err = splicePayloadPoints(payload)
			}
			if err != nil {
				return err
			}
			if ordinal+count > len(points) {
				return store.ErrFormat
			}
			for i, p := range points[ordinal : ordinal+count] {
				if p.Timestamp == 0 {
					continue
				}
				physical := index*128 + destination + i
				if physical >= r.Count || base <= 0 || p.Timestamp < 1 || p.Timestamp%int64(r.Step) != 0 || (p.Timestamp-base)/int64(r.Step)%int64(r.Count) != int64(physical) {
					return errSpliceUnsupported
				}
			}
			fragments, ok := appendSplicePoints(nil, points[ordinal:ordinal+count], payload, priority, r, sequence)
			if !ok {
				return errSpliceUnsupported
			}
			for i := range fragments {
				fragments[i].ordinal += ordinal
			}
			result[ai] = append(result[ai], fragments...)
			return nil
		}
		err = kv.Visit(continuationBlockPrefix(f.data.id, 'p', ai, 0), func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(key) != 20 {
				return store.ErrFormat
			}
			index := int(binary.BigEndian.Uint32(key[16:]))
			if !f.classicGroupAllocated(f.data.layout.Archives[ai], index) {
				return nil
			}
			return add(index, &splicePayload{data: value}, 0, 0, 128, true)
		})
		if errors.Is(err, errSpliceUnsupported) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if !f.data.references {
			continue
		}
		payloads := make(map[uint32]*splicePayload)
		err = kv.Visit(continuationBlockPrefix(f.data.id, 'a', ai, 0), func(key, value []byte) error {
			if len(key) != 20 {
				return store.ErrFormat
			}
			index := int(binary.BigEndian.Uint32(key[16:]))
			if _, err := kv.Get(continuationBodyKey(f.data.id, 'p', ai, 0, index)); err == nil {
				return nil
			} else if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			refs, err := decodePointReferences(value)
			if err != nil {
				return err
			}
			for _, ref := range refs {
				payload := payloads[ref.ID]
				if payload == nil {
					data, err := kv.Get(continuationBodyKey(f.data.id, 'r', ai, 0, int(ref.ID)))
					if err != nil {
						return err
					}
					payload = &splicePayload{data: data, sealed: ref.Sealed}
					payloads[ref.ID] = payload
				}
				if err := add(index, payload, int(ref.Source), int(ref.Destination), int(ref.Count), false); err != nil {
					return err
				}
			}
			return nil
		})
		if errors.Is(err, errSpliceUnsupported) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
	}
	return result, true, nil
}
