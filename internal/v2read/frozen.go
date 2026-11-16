package v2read

import (
	"context"
	"math"
	"slices"
	"sort"
)

// CatalogFuncs adapts an existing immutable catalog and Graphite name index.
// Both callbacks must refer to the same published catalog generation.
type CatalogFuncs struct {
	LookupFunc func(string) (Metric, bool)
	FindFunc   func(context.Context, string, func(string) error) error
}

func (c CatalogFuncs) Lookup(name string) (Metric, bool) { return c.LookupFunc(name) }
func (c CatalogFuncs) Find(ctx context.Context, p string, y func(string) error) error {
	return c.FindFunc(ctx, p, y)
}

type Record struct {
	ID     uint64
	Step   int64
	Sample Sample
}

// Frozen owns a sorted copy of head or sealed archive-slot replacements. Disk
// sources implement Source against a pinned immutable engine snapshot instead.
type Frozen struct{ records []Record }

func Freeze(input []Record) *Frozen {
	f := &Frozen{records: make([]Record, len(input))}
	copy(f.records, input)
	slices.SortFunc(f.records, func(a, b Record) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		if a.Step < b.Step {
			return -1
		}
		if a.Step > b.Step {
			return 1
		}
		if a.Sample.Time < b.Sample.Time {
			return -1
		}
		if a.Sample.Time > b.Sample.Time {
			return 1
		}
		return 0
	})
	return f
}
func (f *Frozen) RetainedBytes() int64 { return 24 + int64(len(f.records))*40 }
func (f *Frozen) metric(id uint64, step int64) []Record {
	start := sort.Search(len(f.records), func(i int) bool { r := f.records[i]; return r.ID > id || r.ID == id && r.Step >= step })
	end := start + sort.Search(len(f.records)-start, func(i int) bool { r := f.records[start+i]; return r.ID > id || r.Step > step })
	return f.records[start:end]
}
func (f *Frozen) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	return len(f.metric(id, step)) > 0, ctx.Err()
}

// SlotRange is a half-open interval of physical archive slots. A source keyed
// by (series, archive, physical slot, version) needs at most two bounded seeks.
type SlotRange struct{ From, Until int64 }

// SlotRanges translates an aligned render interval to physical ring slots.
// Sources must return all versions in these ranges, even timestamps outside
// the render interval, so wrapped replacements still invalidate old values.
// Archive dimensions and alignment are validated by Render before Scan.
func SlotRanges(a Archive, from, until int64) ([2]SlotRange, int) {
	var ranges [2]SlotRange
	n := (until - from) / a.Step
	if n <= 0 {
		return ranges, 0
	}
	if n >= a.Points {
		ranges[0] = SlotRange{0, a.Points}
		return ranges, 1
	}
	first := (from / a.Step) % a.Points
	if first < 0 {
		first += a.Points
	}
	end := first + n
	ranges[0] = SlotRange{first, min(end, a.Points)}
	if end > a.Points {
		ranges[1] = SlotRange{0, end - a.Points}
		return ranges, 2
	}
	return ranges, 1
}

func (f *Frozen) Scan(ctx context.Context, id uint64, a Archive, from, until int64, yield func(Sample) error) error {
	records := f.metric(id, a.Step)
	n := (until - from) / a.Step
	if n <= 0 {
		return ctx.Err()
	}
	first := (from / a.Step) % a.Points
	if first < 0 {
		first += a.Points
	}
	for i := 0; i < len(records); {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := records[i].Sample
		slot := (p.Time / a.Step) % a.Points
		if slot < 0 {
			slot += a.Points
		}
		distance := slot - first
		if distance < 0 {
			distance += a.Points
		}
		if distance < n {
			if err := yield(p); err != nil {
				return err
			}
			i++
			continue
		}
		// The sorted timestamps before the next matching physical interval cannot
		// contribute. Seek past them, across sparse epochs as well as dense tails.
		delta := (first - slot + a.Points) % a.Points * a.Step
		if p.Time > math.MaxInt64-delta {
			break
		}
		next := p.Time + delta
		i++
		i += sort.Search(len(records)-i, func(j int) bool { return records[i+j].Sample.Time >= next })
	}
	return nil
}
