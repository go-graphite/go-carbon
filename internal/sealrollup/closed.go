package sealrollup

import (
	"fmt"
	"math"
	"slices"
)

// PlanClosedInto stores only closed coarse windows. through is the previously
// committed data/idle watermark. An idle call may advance it with closeThrough;
// input calls advance it only from sample time, preserving late-point routing.
// The caller must persist the returned watermark with Records and Next.
// Points are Raw records, one version per slot per batch (superseded ones and
// Ghosts trail as plan.Versions); closed windows are fallback-only Mat records.
func (p *Prepared) PlanClosedInto(records []Record, prior Frontier, through, closeThrough int64, view View, ranges RangeView, batches []Batch, w *Workspace) (SealPlan, int64, error) {
	if prior.Policy != ([32]byte{}) && prior.Policy != p.policy || prior.Order != 0 && prior.Policy == ([32]byte{}) {
		return SealPlan{}, through, fmt.Errorf("stored rollup policy changed")
	}
	next := prior
	next.Policy = p.policy
	// Within the smallest coarse window there is nothing to aggregate. Keep
	// using the same reducer for fine slots, including its ordering and input
	// validation; old retention targets and closed-window corrections use the
	// full ladder. Larger steps are multiples of this one.
	c := p.config
	watermark := max(through, prior.MaxTime)
	start, end := int64(0), int64(0)
	fineOnly := len(c.Archives) > 1 && len(batches) > 0
	if fineOnly {
		start = align(watermark, c.Archives[1].Step)
		end = start + c.Archives[1].Step
	}
	for _, b := range batches {
		if b.Order <= next.Order {
			return SealPlan{}, through, fmt.Errorf("rollup batch is not after frontier")
		}
		next.Order = b.Order
		for _, sample := range b.Points {
			next.MaxTime = max(next.MaxTime, sample.Time)
			fineOnly = fineOnly && sample.Time >= start && sample.Time < end && sample.Time >= b.Now-c.Archives[0].Step*c.Archives[0].Count
		}
	}
	closing := windowClosure{through: watermark}
	if len(batches) == 0 && prior.Order != 0 && closeThrough > closing.through {
		closing.advance = closeThrough
		batches = []Batch{{Order: prior.Order, Now: closeThrough}}
	}
	closure := &closing
	if fineOnly {
		c.Archives = c.Archives[:1]
		closing.through = max(watermark, next.MaxTime)
		closure = nil
	}
	winners, versions, err := reduceMode(c, view, ranges, batches, w, closure, true)
	if err != nil {
		return SealPlan{}, through, err
	}
	plan := SealPlan{Next: next, Records: records[:0], Versions: len(versions)}
	for _, list := range [][]Record{winners, versions} {
		for _, d := range list {
			d.Correction = d.Sample.Time <= prior.MaxTime
			plan.Records = append(plan.Records, d)
		}
	}
	return plan, closing.through, nil
}

// ProjectOpen adds virtual replacements to an already resolved full metric.
// Ordinary range reads use their bounded source range rather than loading this
// complete image. Full-snapshot visitors use this for recovery/oracle exports.
func ProjectOpen(c Config, maxTime, through int64, records []Record) []Record {
	var samples []Sample
	for a := 1; a < len(c.Archives); a++ {
		coarse := c.Archives[a]
		start := align(maxTime, coarse.Step)
		if start+coarse.Step <= through {
			continue
		}
		samples = samples[:0]
		old := -1
		physical := slot(start, coarse)
		for i, r := range records {
			if r.Archive == a && r.Slot == physical {
				old = i
			}
			if r.Archive == a-1 && r.Sample.Time >= start && r.Sample.Time < start+coarse.Step {
				samples = append(samples, r.Sample)
			}
		}
		slices.SortFunc(samples, func(a, b Sample) int {
			if a.Time < b.Time {
				return -1
			}
			if a.Time > b.Time {
				return 1
			}
			return 0
		})
		if p, ok := AggregateWindow(c, a, start, samples); ok {
			r := Record{Delta: Delta{Archive: a, Slot: physical, Order: p.Sequence, Sample: p}}
			if old < 0 {
				records = append(records, r)
			} else if p.Time > records[old].Sample.Time || p.Time == records[old].Sample.Time && p.Sequence >= records[old].Order {
				records[old] = r
			}
		}
	}
	slices.SortFunc(records, func(a, b Record) int {
		if a.Archive != b.Archive {
			return a.Archive - b.Archive
		}
		if a.Slot < b.Slot {
			return -1
		}
		if a.Slot > b.Slot {
			return 1
		}
		return 0
	})
	return records
}

// AggregateWindow consumes exact, time-ordered source winners. Failed XFF is
// absence of a replacement, so callers retain any older coarse physical slot.
func AggregateWindow(c Config, archive int, start int64, samples []Sample) (Sample, bool) {
	if len(samples) == 0 || float32(len(samples))/float32(c.Archives[archive].Step/c.Archives[archive-1].Step) < c.XFilesFactor {
		return Sample{}, false
	}
	var order uint64
	for _, p := range samples {
		order = max(order, p.Sequence)
	}
	return Sample{Time: start, Bits: math.Float64bits(aggregateSamples(c.Method, samples)), Sequence: order}, true
}
