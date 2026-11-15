package sealrollup

import "slices"

// Slots is an owner-local sparse physical ring index. A lookup or range is
// immutable while Plan runs; install replacements only after durable ingest.
// Physical ordering makes wrap an in-place replacement, without moving a full
// fine-retention array on every new timestamp. Storage is proportional to
// occupied slots; this index does not provide a bounded sealed-block cache.
type Slots struct {
	archives []Archive
	data     [][]slotSample
}
type slotSample struct {
	slot   int64
	sample Sample
}

func NewSlots(archives []Archive) *Slots {
	return &Slots{archives: slices.Clone(archives), data: make([][]slotSample, len(archives))}
}
func slotCompare(a, b slotSample) int {
	if a.slot < b.slot {
		return -1
	}
	if a.slot > b.slot {
		return 1
	}
	return 0
}
func (s *Slots) Install(d Delta) {
	ps := s.data[d.Archive]
	v := slotSample{d.Slot, d.Sample}
	i, found := slices.BinarySearchFunc(ps, v, slotCompare)
	if found {
		ps[i] = v
	} else {
		ps = slices.Insert(ps, i, v)
	}
	s.data[d.Archive] = ps
}
func (s *Slots) View(a int, physical int64) (Sample, bool) {
	ps := s.data[a]
	i, ok := slices.BinarySearchFunc(ps, slotSample{slot: physical}, slotCompare)
	if !ok {
		return Sample{}, false
	}
	return ps[i].sample, true
}
func (s *Slots) Range(a int, start, end int64, dst []Sample) []Sample {
	ar := s.archives[a]
	lo := slot(start, ar)
	count := (end - start) / ar.Step
	hi := lo + count
	appendRange := func(first, last int64) {
		ps := s.data[a]
		i, _ := slices.BinarySearchFunc(ps, slotSample{slot: first}, slotCompare)
		for ; i < len(ps) && ps[i].slot < last; i++ {
			p := ps[i].sample
			if p.Time >= start && p.Time < end {
				dst = append(dst, p)
			}
		}
	}
	appendRange(lo, min(hi, ar.Count))
	if hi > ar.Count {
		appendRange(0, hi-ar.Count)
	}
	return dst
}
