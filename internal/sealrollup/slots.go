package sealrollup

import (
	"math"
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

// InstallRecent installs only source samples inside the resident horizon.
// KeepRecent must establish the horizon before a committed batch is installed.
func (s *Slots) InstallRecent(d Delta) {
	if d.Archive == len(*s.archives)-1 {
		return
	}
	if s.data != nil && d.Sample.Time < s.data[d.Archive].floor {
		return
	}
	s.Install(d)
}

func (s *Slots) View(a int, physical int64) (Sample, bool) {
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

// RecentRange reports whether the complete window is still resident. A miss
// must be resolved from durable physical-slot winners, never partial aggregates.
func (s *Slots) RecentRange(a int, start, end int64, dst []Sample) ([]Sample, bool) {
	if s.data != nil && start < s.data[a].floor {
		return dst, false
	}
	return s.Range(a, start, end, dst), true
}

// KeepRecent retains every fine sample of the current coarse window, plus
// two preceding source intervals. The terminal archive is never a source.
// A floor records completeness even when a window had no occupied samples.
// Compact backing arrays as well as lengths: old retention capacity must die.
func (s *Slots) KeepRecent(maxTime int64) {
	ars := *s.archives
	if len(ars) == 1 {
		s.data, s.hasSingle = nil, false
		return
	}
	if s.data == nil {
		s.data = make([]slotSpan, len(ars))
		if s.hasSingle {
			s.data[s.singleArchive] = slotSpan{data: []slotSample{s.single}, first: s.single.slot, last: s.single.slot}
			s.hasSingle = false
		}
	}
	for a := range ars {
		span := &s.data[a]
		if a == len(ars)-1 {
			if span.floor != math.MaxInt64 {
				*span = slotSpan{floor: math.MaxInt64}
			}
			continue
		}
		step := ars[a+1].Step
		floor := maxTime - maxTime%step - 2*ars[a].Step
		// InstallRecent rejects samples below the established floor. Nothing
		// can need eviction until the next coarse window opens.
		if floor <= span.floor {
			continue
		}
		span.floor = floor
		kept := span.data[:0]
		for _, p := range span.data {
			if p.sample.Time >= span.floor {
				kept = append(kept, p)
			}
		}
		// Release old retention-sized arrays and unusually dense past windows.
		// Normal appends reuse capacity within the current window.
		if cap(kept) > 2*len(kept)+8 {
			compact := make([]slotSample, len(kept), 2*len(kept)+8)
			copy(compact, kept)
			kept = compact
		}
		span.data = kept
		if len(kept) > 0 {
			span.first, span.last = kept[0].slot, kept[len(kept)-1].slot
		}
	}
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
