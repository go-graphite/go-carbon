package chunkstore

import (
	"math"
	"sort"
)

// Both snapshot readers return timestamp-sorted, privately owned arrays.
// Normalization therefore preserves order, allowing the exact fillArchive
// result without its whole-archive hash table and reflection-based sort. This
// is an explicit history fill policy, never the live-write/read merge policy.
func nativeArchiveUnion(source, target Archive) ([]Point, bool, bool, error) {
	return nativeArchiveUnionInto(source, target, nil, true)
}

func nativeArchiveUnionInto(source, target Archive, out []Point, classify bool) ([]Point, bool, bool, error) {
	a, changedA, err := nativeNormalizeArchive(source.Points, source.Retention.Step)
	if err != nil {
		return nil, false, false, err
	}
	b, changedB, err := nativeNormalizeArchive(target.Points, target.Retention.Step)
	if err != nil {
		return nil, false, false, err
	}
	intersects := classify && nativeSlotsIntersect(a, b, source.Retention)
	changed := changedA || changedB
	if len(a) == 0 {
		return append(out[:0], b...), intersects, changedA || changedB, nil
	}
	if len(b) == 0 {
		return append(out[:0], a...), intersects, changedA || changedB, nil
	}
	out = out[:0]
	if cap(out) < len(a)+len(b) {
		out = make([]Point, 0, len(a)+len(b))
	}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].Timestamp < b[j].Timestamp:
			out = append(out, a[i])
			i++
		case a[i].Timestamp > b[j].Timestamp:
			out = append(out, b[j])
			j++
		default:
			winner := b[j] // finite targets win, including signed zero
			if PreferFiniteFill(a[i].Value, b[j].Value) {
				winner = a[i]
				changed = true // an append preserving target buffers is unsafe
			}
			out = append(out, winner)
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out, intersects, changed, nil
}

// PreferFiniteFill agrees with sync's finite-point coverage definition. NaNs
// keep their existing normalization; two infinities still keep the target.
// Call only for equal timestamps in an explicit fill/import, never to choose
// between different ring owners or to merge live writes with stored history.
func PreferFiniteFill(donor, target float64) bool {
	return math.IsInf(target, 0) && !math.IsNaN(donor) && !math.IsInf(donor, 0)
}

func nativeNormalizeArchive(points []Point, step int) ([]Point, bool, error) {
	out := points[:0]
	changed := false
	for _, p := range points {
		if math.IsNaN(p.Value) {
			changed = true
			continue
		}
		timestamp, err := storageTimestamp(p.Timestamp)
		if err != nil {
			return nil, false, err
		}
		aligned := int64(align(timestamp, step))
		changed = changed || aligned != p.Timestamp
		p.Timestamp = aligned
		if len(out) != 0 {
			last := &out[len(out)-1]
			if last.Timestamp > aligned {
				return nil, false, ErrFormat
			}
			if last.Timestamp == aligned {
				*last = p
				changed = true
				continue
			}
		}
		out = append(out, p)
	}
	return out, changed, nil
}

// Within one ring, rotating at the wrap makes circular slots sorted. Sparse
// histories wider than one ring retain the bounded exact map fallback.
func nativeSlotsIntersect(a, b []Point, r Retention) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	step, count := int64(r.Step), int64(r.Count)
	if (a[len(a)-1].Timestamp-a[0].Timestamp)/step >= count || (b[len(b)-1].Timestamp-b[0].Timestamp)/step >= count {
		if len(a) > len(b) {
			a, b = b, a
		}
		// Grow from validated points rather than sender-controlled archive capacity.
		slots := borrowTransferSlots()
		defer releaseTransferSlots(slots)
		for _, p := range a {
			slots[p.Timestamp/step%count] = struct{}{}
		}
		for _, p := range b {
			if _, found := slots[p.Timestamp/step%count]; found {
				return true
			}
		}
		return false
	}
	rotate := func(points []Point) int {
		boundary := (points[0].Timestamp/step/count + 1) * count
		i := sort.Search(len(points), func(i int) bool { return points[i].Timestamp/step >= boundary })
		if i == len(points) {
			return 0
		}
		return i
	}
	i, j := rotate(a), rotate(b)
	for seenA, seenB := 0, 0; seenA < len(a) && seenB < len(b); {
		x, y := a[i].Timestamp/step%count, b[j].Timestamp/step%count
		if x == y {
			return true
		}
		if x < y {
			seenA++
			i++
			if i == len(a) {
				i = 0
			}
		} else {
			seenB++
			j++
			if j == len(b) {
				j = 0
			}
		}
	}
	return false
}
