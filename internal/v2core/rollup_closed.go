package v2core

import (
	"cmp"
	"context"
	"math"
	"slices"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func rollupClosureDue(m *rollupMetric, meta metricMetadata, now int64) bool {
	for _, a := range m.schema().Archives[1:] {
		if rollupOpenEnd(meta, a) <= now {
			return true
		}
	}
	return false
}

// rollupOpenEnd is the end of archive a's window holding the frontier while
// that window is open, else math.MaxInt64.
func rollupOpenEnd(meta metricMetadata, a sealrollup.Archive) int64 {
	if meta.Version != 3 || meta.Frontier.Order == 0 || meta.Expired {
		return math.MaxInt64
	}
	end := meta.Frontier.MaxTime - meta.Frontier.MaxTime%a.Step + a.Step
	if meta.ClosedThrough >= end {
		return math.MaxInt64
	}
	return end
}

// scanWindows yields archive's physical slots [first, last), each as its
// newest window at or before the frontier's, computed from stored versions
// (windowRecords). Names with copied parts keep the move-aware open window.
func (s *RollupSnapshot) scanWindows(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	if s.moves[m] != nil {
		return s.scanMovedOpenArchive(ctx, name, archive, first, last, yield)
	}
	c, meta := m.latest.schema(), m.latest.meta
	if meta.Version != 3 || meta.Frontier.Order == 0 {
		return s.scanStoredArchive(ctx, name, archive, first, last, yield)
	}
	var stored []sealrollup.Record
	if err := s.scanStoredArchive(ctx, name, archive, first, last, func(r sealrollup.Record) error {
		stored = append(stored, r)
		return nil
	}); err != nil {
		return err
	}
	return windowRecords(c, meta, archive, first, last, stored, func(l int, first, last int64, from uint64) ([]sealrollup.Record, error) {
		var out []sealrollup.Record
		err := s.eachPartRecord(ctx, m, s.metricParts(m), l, first, last, true, func(part *readPartMetric) bool {
			return from > 0 && part.meta.Frontier.Order != 0 && part.meta.Frontier.Order < from
		}, func(r sealrollup.Record, _ partOrigin) error {
			out = append(out, r)
			return nil
		})
		return out, err
	}, yield)
}

// windowRecords yields archive's physical slots [first, last) in slot order,
// each slot as its newest window at or before the frontier's window, computed
// from stored versions by one lazy descent (sealrollup.OpenSlots): windows a
// merge settled stop at their written-down value, every level's physical
// range is read once. A slot keeps its stored per-slot winner (stored, the
// winners of the range) when that equals the computed value (its order then
// stays the stored one) or when its window has no value (an older epoch).
func windowRecords(c sealrollup.Config, meta metricMetadata, archive int, first, last int64, stored []sealrollup.Record, load func(l int, first, last int64, from uint64) ([]sealrollup.Record, error), yield func(sealrollup.Record) error) error {
	a := c.Archives[archive]
	open := meta.Frontier.MaxTime - meta.Frontier.MaxTime%a.Step
	phys := (open / a.Step) % a.Count
	windows := make([]int64, 0, last-first)
	for p := first; p < last; p++ {
		if w := open - ((phys-p+a.Count)%a.Count)*a.Step; w >= 0 {
			windows = append(windows, w)
		}
	}
	var computed []sealrollup.Sample // values in slot order (windows ascend by slot)
	if err := sealrollup.OpenSlots(c, archive, windows, load, func(_ int64, p sealrollup.Sample, ok bool) {
		if ok {
			computed = append(computed, p)
		}
	}); err != nil {
		return err
	}
	if !slices.IsSortedFunc(stored, func(x, y sealrollup.Record) int { return cmp.Compare(x.Slot, y.Slot) }) {
		slices.SortStableFunc(stored, func(x, y sealrollup.Record) int { return cmp.Compare(x.Slot, y.Slot) })
	}
	i, j := 0, 0
	for p := first; p < last; p++ {
		var old *sealrollup.Record
		for i < len(stored) && stored[i].Slot < p {
			i++
		}
		if i < len(stored) && stored[i].Slot == p {
			old = &stored[i]
		}
		var v sealrollup.Sample
		ok := j < len(computed) && (computed[j].Time/a.Step)%a.Count == p
		if ok {
			v = computed[j]
			j++
		}
		switch {
		case !ok && old == nil:
			continue
		case !ok, old != nil && old.Sample.Time == v.Time && old.Sample.Bits == v.Bits:
			if err := yield(*old); err != nil {
				return err
			}
			continue
		}
		v.Sequence >>= 4 // a tick (batch order, sub-step) back to its batch order
		if err := yield(sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: p, Order: v.Sequence, Sample: v}}); err != nil {
			return err
		}
	}
	return nil
}

// scanMovedOpenArchive is the open window of a name with copied parts: their
// source orders do not compare with the live ones, so the version read does
// not apply, and the slot aggregates the finer per-slot winners (move-aware)
// as before the version read. Moves carrying versions (C2c) delete it.
func (s *RollupSnapshot) scanMovedOpenArchive(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	c := m.latest.schema()
	a := c.Archives[archive]
	start := m.latest.meta.Frontier.MaxTime
	start -= start % a.Step
	physical := (start / a.Step) % a.Count
	if m.latest.meta.Version != 3 || start+a.Step <= m.latest.meta.ClosedThrough || physical < first || physical >= last {
		return s.scanStoredArchive(ctx, name, archive, first, last, yield)
	}
	var old sealrollup.Record
	hasOld := false
	if err := s.scanStoredArchive(ctx, name, archive, first, last, func(r sealrollup.Record) error {
		if r.Slot == physical {
			old, hasOld = r, true
			return nil
		}
		return yield(r)
	}); err != nil {
		return err
	}
	higher := c.Archives[archive-1]
	lo := (start / higher.Step) % higher.Count
	hi := lo + a.Step/higher.Step
	var samples []sealrollup.Sample
	scan := func(first, last int64) error {
		return s.ScanArchive(ctx, name, archive-1, first, last, func(r sealrollup.Record) error {
			if r.Sample.Time >= start && r.Sample.Time < start+a.Step {
				samples = append(samples, r.Sample)
			}
			return nil
		})
	}
	if err := scan(lo, min(hi, higher.Count)); err != nil {
		return err
	}
	if hi > higher.Count {
		if err := scan(0, hi-higher.Count); err != nil {
			return err
		}
	}
	slices.SortFunc(samples, compareHistorySamples)
	if p, ok := sealrollup.AggregateWindow(c, archive, start, samples); ok && (!hasOld || p.Time > old.Sample.Time || p.Time == old.Sample.Time && p.Sequence >= old.Order) {
		return yield(sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: physical, Order: p.Sequence, Sample: p}})
	}
	if hasOld {
		return yield(old)
	}
	return nil
}
