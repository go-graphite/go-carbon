package sealrollup

import (
	"cmp"
	"fmt"
	"math"
	"slices"
)

// RouteInto retains the admission-routed raw versions of each original batch.
// It reads no history and computes no windows; merges own write-down.
// Records uses caller storage and is valid until that storage is reused.
func (p *Prepared) RouteInto(records []Record, prior Frontier, batches []Batch, w *Workspace) (SealPlan, error) {
	if prior.Policy != ([32]byte{}) && prior.Policy != p.policy || prior.Order != 0 && prior.Policy == ([32]byte{}) {
		return SealPlan{}, fmt.Errorf("stored rollup policy changed")
	}
	if w == nil {
		w = &Workspace{sparseOnly: true}
	}
	next := prior
	next.Policy = p.policy
	count := 0
	for _, b := range batches {
		count += len(b.Points)
	}
	w.keep, w.updates, w.versions = true, w.updates[:0], w.versions[:0]
	w.preparePositions(p.config, count)
	for _, b := range batches {
		if b.Order <= next.Order {
			return SealPlan{}, fmt.Errorf("rollup batch is not after frontier")
		}
		if b.Now <= 0 || b.Now > math.MaxUint32 {
			return SealPlan{}, fmt.Errorf("invalid batch clock")
		}
		next.Order = b.Order
		w.points = append(w.points[:0], b.Points...)
		slices.SortStableFunc(w.points, func(a, b Sample) int {
			if c := cmp.Compare(a.Time, b.Time); c != 0 {
				return c
			}
			return cmp.Compare(a.Sequence, b.Sequence)
		})
		for _, point := range w.points {
			if point.Time <= 0 || point.Time > math.MaxUint32 {
				return SealPlan{}, fmt.Errorf("timestamp outside storage range")
			}
			next.MaxTime = max(next.MaxTime, point.Time)
			for archive, a := range p.config.Archives {
				if point.Time < b.Now-a.Step*a.Count {
					continue
				}
				stamp := align(point.Time, a.Step)
				k := key{archive, slot(stamp, a)}
				w.replace(k, Record{Delta: Delta{archive, k.slot, b.Order, Sample{stamp, point.Bits, b.Order}}, Kind: KindRaw})
				break
			}
		}
	}
	bySlot := func(a, b Record) int {
		if c := cmp.Compare(a.Archive, b.Archive); c != 0 {
			return c
		}
		return cmp.Compare(a.Slot, b.Slot)
	}
	slices.SortFunc(w.updates, bySlot)
	slices.SortStableFunc(w.versions, bySlot)
	plan := SealPlan{Next: next, Records: records[:0], Versions: len(w.versions)}
	for _, list := range [][]Record{w.updates, w.versions} {
		for _, r := range list {
			r.Correction = r.Sample.Time <= prior.MaxTime
			plan.Records = append(plan.Records, r)
		}
	}
	return plan, nil
}

// AggregateWindow consumes exact, time-ordered source winners. Failed XFF is
// absence of a replacement, so callers retain any older coarse physical slot.
func AggregateWindow(c Config, archive int, start int64, samples []Sample) (Sample, bool) {
	if len(samples) == 0 || float32(len(samples))/float32(c.Archives[archive].Step/c.Archives[archive-1].Step) < c.XFilesFactor {
		return Sample{}, false
	}
	return Sample{Time: start, Bits: math.Float64bits(aggregateSamples(c.Method, samples))}, true
}
