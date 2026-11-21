package sealrollup

import (
	"fmt"
	"unsafe"
)

// PlanInto is PlanWithWorkspace with caller-owned record storage. The returned
// Records may alias records and remain valid until the caller reuses that storage.
// This permits a synchronous seal consumer to reuse its output between metrics.
func (p *Prepared) PlanInto(records []Record, prior Frontier, view View, ranges RangeView, batches []Batch, w *Workspace) (SealPlan, error) {
	c, policy := p.config, p.policy
	if prior.Policy != ([32]byte{}) && prior.Policy != policy {
		return SealPlan{}, fmt.Errorf("stored rollup policy changed")
	}
	if prior.Order != 0 && prior.Policy == ([32]byte{}) {
		return SealPlan{}, fmt.Errorf("rollup frontier lacks policy")
	}
	next := prior
	next.Policy = policy
	for _, b := range batches {
		if b.Order <= next.Order {
			return SealPlan{}, fmt.Errorf("rollup batch %d is not after frontier %d", b.Order, next.Order)
		}
		next.Order = b.Order
		for _, p := range b.Points {
			next.MaxTime = max(next.MaxTime, p.Time)
		}
	}
	deltas, err := reduce(c, view, ranges, batches, w)
	if err != nil {
		return SealPlan{}, err
	}
	plan := SealPlan{Next: next, Records: records[:0]}
	for _, d := range deltas {
		d.Correction = d.Sample.Time <= prior.MaxTime
		if view != nil {
			p, ok := view(d.Archive, d.Slot)
			d.Correction = d.Correction || (ok && p.Time == d.Sample.Time)
		}
		plan.Records = append(plan.Records, d)
	}
	return plan, nil
}

// RetainedBytes reports private scratch high-water storage, excluding the
// Workspace value itself. Readers can discard oversized workspaces after use.
func (w *Workspace) RetainedBytes() int64 {
	return int64(cap(w.positions))*4 + int64(cap(w.offsets))*int64(unsafe.Sizeof(int(0))) +
		int64(cap(w.updates)+cap(w.versions))*int64(unsafe.Sizeof(Record{})) +
		int64(cap(w.points)+cap(w.current)+cap(w.neighbors))*int64(unsafe.Sizeof(Sample{})) + int64(cap(w.changed))*8
}
