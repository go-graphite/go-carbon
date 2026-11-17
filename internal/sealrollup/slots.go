package sealrollup

import "slices"

// Slots is an owner-local sparse physical ring index. A lookup or range is
// immutable while Plan runs; install replacements only after durable ingest.
// Physical ordering makes wrap an in-place replacement, without moving a full
// fine-retention array on every new timestamp. Storage is proportional to
// occupied slots; this index does not provide a bounded sealed-block cache.
type Slots struct {
	archives      *[]Archive
	data          [][]slotSample
	single        slotSample
	singleArchive int
	hasSingle     bool
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
func (s *Slots) Install(d Delta) {
	if s.data == nil {
		if !s.hasSingle || (s.singleArchive == d.Archive && s.single.slot == d.Slot) {
			s.single = slotSample{d.Slot, d.Sample}
			s.singleArchive = d.Archive
			s.hasSingle = true
			return
		}
		s.data = make([][]slotSample, len(*s.archives))
		s.data[s.singleArchive] = []slotSample{s.single}
		s.hasSingle = false
	}

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
	if s.data == nil {
		if s.hasSingle && s.singleArchive == a && s.single.slot == physical {
			return s.single.sample, true
		}
		return Sample{}, false
	}

	ps := s.data[a]
	i, ok := slices.BinarySearchFunc(ps, slotSample{slot: physical}, slotCompare)
	if !ok {
		return Sample{}, false
	}
	return ps[i].sample, true
}

// Range requires aligned bounds spanning at most one archive retention.
func (s *Slots) Range(a int, start, end int64, dst []Sample) []Sample {
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

// NewSlots borrows the archives already owned by this immutable prepared schema.
// Slot values remain private to the caller; schema slices never escape.
func (p *Prepared) NewSlots() *Slots {
	return &Slots{archives: &p.config.Archives}
}

// Digest returns the immutable schema identity validated by Prepare.
func (p *Prepared) Digest() [32]byte { return p.policy }

// Config borrows this validated immutable schema; callers must not mutate Archives.
func (p *Prepared) Config() Config { return p.config }
