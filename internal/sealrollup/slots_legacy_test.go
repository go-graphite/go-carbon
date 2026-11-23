package sealrollup

import (
	"slices"
)

// Slots is an owner-local sparse physical ring index. A lookup or range is
// immutable while Plan runs; install replacements only after durable ingest.
// Physical ordering makes wrap an in-place replacement, without moving a full
// fine-retention array on every new timestamp. Storage is proportional to
// occupied slots unless the caller uses InstallRecent/KeepRecent and reloads
// incomplete windows from durable storage.
type Slots struct {
	archives      *[]Archive
	data          []slotSpan
	single        slotSample
	singleArchive int
	hasSingle     bool
}

// Bounds are physical slot numbers, independent of sample timestamps. Keeping
// them beside the slice avoids two dependent sample loads on every lookup.
type slotSpan struct {
	data        []slotSample
	first, last int64
	// Samples before floor may have been evicted, including absent slots.
	floor int64
}

type slotSample struct {
	slot   int64
	sample Sample
}

// NewSlots requires archives from a validated Config.
func NewSlots(archives []Archive) *Slots {
	owned := slices.Clone(archives)
	return &Slots{archives: &owned}
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

// Contiguous occupied slots already encode their position. Keep the sparse
// representation for holes and long retentions, without a second index.
func (s *slotSpan) position(physical int64) (int, bool) {
	n := len(s.data)
	if n == 0 || physical < s.first {
		return 0, false
	}
	if physical > s.last {
		return n, false
	}
	if s.last-s.first == int64(n-1) {
		return int(physical - s.first), true
	}
	return slices.BinarySearchFunc(s.data, slotSample{slot: physical}, slotCompare)
}

func (s *Slots) Install(d Delta) {
	if s.data == nil {
		if !s.hasSingle || (s.singleArchive == d.Archive && s.single.slot == d.Slot) {
			s.single = slotSample{d.Slot, d.Sample}
			s.singleArchive = d.Archive
			s.hasSingle = true
			return
		}
		s.data = make([]slotSpan, len(*s.archives))
		s.data[s.singleArchive] = slotSpan{data: []slotSample{s.single}, first: s.single.slot, last: s.single.slot}
		s.hasSingle = false
	}

	span := &s.data[d.Archive]
	ps := span.data
	v := slotSample{d.Slot, d.Sample}
	i, found := span.position(d.Slot)
	if found {
		ps[i] = v
	} else {
		ps = slices.Insert(ps, i, v)
		span.first, span.last = ps[0].slot, ps[len(ps)-1].slot
	}
	span.data = ps
}

// View, Range and RecentRange read a nil *Slots as empty: a caller that keeps
// no residency needs no Slots value.
func (s *Slots) View(a int, physical int64) (Sample, bool) {
	if s == nil {
		return Sample{}, false
	}
	if s.data == nil {
		if s.hasSingle && s.singleArchive == a && s.single.slot == physical {
			return s.single.sample, true
		}
		return Sample{}, false
	}

	span := &s.data[a]
	i, ok := span.position(physical)
	if !ok {
		return Sample{}, false
	}
	return span.data[i].sample, true
}

// Range requires aligned bounds spanning at most one archive retention.
func (s *Slots) Range(a int, start, end int64, dst []Sample) []Sample {
	if s == nil {
		return dst
	}
	if s.data == nil {
		if s.hasSingle && s.singleArchive == a && s.single.sample.Time >= start && s.single.sample.Time < end {
			return append(dst, s.single.sample)
		}
		return dst
	}

	ar := (*s.archives)[a]
	lo := slot(start, ar)
	count := (end - start) / ar.Step
	hi := lo + count
	appendRange := func(first, last int64) {
		ps := s.data[a].data
		i, _ := s.data[a].position(first)
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

// NewSlots borrows the archives already owned by this immutable prepared schema.
// Slot values remain private to the caller; schema slices never escape.
func (p *Prepared) NewSlots() *Slots {
	return &Slots{archives: &p.config.Archives}
}
