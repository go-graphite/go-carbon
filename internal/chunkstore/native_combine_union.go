package chunkstore

import (
	"math"
	"sort"
)

// Both snapshot readers return timestamp-sorted, privately owned arrays.
// Normalization therefore preserves order, allowing the exact fillArchive
// result without its whole-archive hash table and reflection-based sort.
func nativeArchiveUnion(source, target Archive) ([]Point, bool, bool, error) {
	a, changedA, err := nativeNormalizeArchive(source.Points, source.Retention.Step)
	if err != nil {
		return nil, false, false, err
	}
	b, changedB, err := nativeNormalizeArchive(target.Points, target.Retention.Step)
	if err != nil {
		return nil, false, false, err
	}
	intersects := nativeSlotsIntersect(a, b, source.Retention)
	if len(a) == 0 {
		return b, intersects, changedA || changedB, nil
	}
	if len(b) == 0 {
		return a, intersects, changedA || changedB, nil
	}
	out := make([]Point, 0, len(a)+len(b))
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
			out = append(out, b[j]) // target wins, including signed zero
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out, intersects, changedA || changedB, nil
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
		slots := make(map[int64]struct{}, min(len(a), r.Count))
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
