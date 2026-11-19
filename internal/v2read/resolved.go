package v2read

import (
	"context"
	"slices"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// resolvedSource owns one immutable winner per physical slot. Canonical plans
// already sort by archive/slot; merge their replacements without a timestamp
// Freeze/copy/sort. A wrapped timestamp replaces the old slot before filtering.
type resolvedSource struct {
	id       uint64
	archives []sealrollup.Archive
	records  []sealrollup.Record
	// stored answers HasArchive for a range-limited base: whether the sealed
	// archive holds any slot outside the loaded ranges.
	stored func(context.Context, int) (bool, error)
}

func recordCompare(a, b sealrollup.Record) int {
	if a.Archive < b.Archive {
		return -1
	}
	if a.Archive > b.Archive {
		return 1
	}
	if a.Slot < b.Slot {
		return -1
	}
	if a.Slot > b.Slot {
		return 1
	}
	return 0
}
func newResolvedSource(id uint64, archives []sealrollup.Archive, records []sealrollup.Record) *resolvedSource {
	records = slices.Clone(records)
	slices.SortFunc(records, recordCompare)
	return &resolvedSource{id: id, archives: archives, records: records}
}
func (s *resolvedSource) replace(updates []sealrollup.Record) *resolvedSource {
	out := make([]sealrollup.Record, 0, len(s.records)+len(updates))
	i, j := 0, 0
	for i < len(s.records) && j < len(updates) {
		cmp := recordCompare(s.records[i], updates[j])
		if cmp < 0 {
			out = append(out, s.records[i])
			i++
		} else {
			out = append(out, updates[j])
			j++
			if cmp == 0 {
				i++
			}
		}
	}
	out = append(out, s.records[i:]...)
	out = append(out, updates[j:]...)
	return &resolvedSource{id: s.id, archives: s.archives, records: out, stored: s.stored}
}
func (s *resolvedSource) lower(a int, slot int64) int {
	i, _ := slices.BinarySearchFunc(s.records, sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot}}, recordCompare)
	return i
}
func (s *resolvedSource) view(a int, slot int64) (sealrollup.Sample, bool) {
	i := s.lower(a, slot)
	if i < len(s.records) && s.records[i].Archive == a && s.records[i].Slot == slot {
		return s.records[i].Sample, true
	}
	return sealrollup.Sample{}, false
}
func (s *resolvedSource) rangeSamples(a int, start, end int64, dst []sealrollup.Sample) []sealrollup.Sample {
	ar := s.archives[a]
	rs, n := SlotRanges(Archive{ar.Step, ar.Count}, start, end)
	for _, r := range rs[:n] {
		for i := s.lower(a, r.From); i < len(s.records) && s.records[i].Archive == a && s.records[i].Slot < r.Until; i++ {
			p := s.records[i].Sample
			if p.Time >= start && p.Time < end {
				dst = append(dst, p)
			}
		}
	}
	return dst
}
func (s *resolvedSource) archive(step int64) int {
	for i, a := range s.archives {
		if a.Step == step {
			return i
		}
	}
	return -1
}
func (s *resolvedSource) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	a := s.archive(step)
	if a < 0 || id != s.id {
		return false, ctx.Err()
	}
	i := s.lower(a, 0)
	if i < len(s.records) && s.records[i].Archive == a {
		return true, ctx.Err()
	}
	if s.stored != nil {
		return s.stored(ctx, a)
	}
	return false, ctx.Err()
}
func (s *resolvedSource) Scan(ctx context.Context, id uint64, a Archive, from, until int64, yield func(Sample) error) error {
	archive := s.archive(a.Step)
	if archive < 0 || id != s.id {
		return ctx.Err()
	}
	rs, n := SlotRanges(a, from, until)
	for _, r := range rs[:n] {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := s.lower(archive, r.From); i < len(s.records) && s.records[i].Archive == archive && s.records[i].Slot < r.Until; i++ {
			p := s.records[i]
			if err := yield(Sample{Time: p.Sample.Time, Bits: p.Sample.Bits, Sequence: p.Order}); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
func (s *resolvedSource) RetainedBytes() int64 {
	return 64 + int64(cap(s.records))*64 + int64(len(s.archives))*16
}

// orderSamples returns a stably time/sequence-ordered copy of one immutable
// pending batch. The canonical reducer still owns admission and aggregation,
// and rechecks its input order. Every request sorts only its own name's batches.
func orderSamples(input []sealrollup.Sample) []sealrollup.Sample {
	sorted := slices.Clone(input)
	slices.SortStableFunc(sorted, func(a, b sealrollup.Sample) int {
		if a.Time < b.Time {
			return -1
		}
		if a.Time > b.Time {
			return 1
		}
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	return sorted
}
