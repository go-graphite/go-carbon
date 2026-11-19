package v2read

import (
	"context"
	"math"
	"slices"
	"sort"
)

type Record struct {
	ID     uint64
	Step   int64
	Sample Sample
}

// Frozen is the test oracle Source: a sorted copy of archive-slot versions.
// It drives render's generic arbitration path, which disk sources share.
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
func (f *Frozen) metric(id uint64, step int64) []Record {
	start := sort.Search(len(f.records), func(i int) bool { r := f.records[i]; return r.ID > id || r.ID == id && r.Step >= step })
	end := start + sort.Search(len(f.records)-start, func(i int) bool { r := f.records[start+i]; return r.ID > id || r.Step > step })
	return f.records[start:end]
}
func (f *Frozen) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	return len(f.metric(id, step)) > 0, ctx.Err()
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

// sources merges several test sources in order, like a head over parts over
// blocks: render arbitrates their versions by Sequence.
type sources []Source

func (s sources) Scan(ctx context.Context, id uint64, a Archive, from, until int64, yield func(Sample) error) error {
	for _, source := range s {
		if err := source.Scan(ctx, id, a, from, until, yield); err != nil {
			return err
		}
	}
	return nil
}
func (s sources) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	for _, source := range s {
		if ok, err := source.HasArchive(ctx, id, step); ok || err != nil {
			return ok, err
		}
	}
	return false, ctx.Err()
}
