package partsread

import (
	"context"
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
	end := start
	for end < len(f.records) && f.records[end].ID == id && f.records[end].Step == step {
		end++
	}
	return f.records[start:end]
}
func (f *Frozen) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	return len(f.metric(id, step)) > 0, ctx.Err()
}
func (f *Frozen) Scan(ctx context.Context, id uint64, a Archive, from, until int64, yield func(Sample) error) error {
	n := (until - from) / a.Step
	for _, r := range f.metric(id, a.Step) {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := r.Sample
		i := ((p.Time/a.Step)%a.Points - (from/a.Step)%a.Points + a.Points) % a.Points
		if i < 0 {
			i += a.Points
		}
		if i < n {
			if err := yield(p); err != nil {
				return err
			}
		}
	}
	return nil
}
