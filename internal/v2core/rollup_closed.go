package v2core

import (
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

// scanWithOpenSlot replaces the physical slot of archive's open window (the
// window holding the frontier, not yet closed) with its value computed from
// stored versions (sealrollup.OpenSlot); every other slot is the stored
// per-slot winner. Only the open window's finer history is read, and only
// where no written-down window settles it.
func (s *RollupSnapshot) scanWithOpenSlot(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	c, meta := m.latest.schema(), m.latest.meta
	start, physical, open := openWindow(c, meta, archive)
	if s.moves[m] != nil {
		return s.scanMovedOpenArchive(ctx, name, archive, first, last, yield)
	}
	if !open || physical < first || physical >= last {
		return s.scanStoredArchive(ctx, name, archive, first, last, yield)
	}
	var old *sealrollup.Record
	if err := s.scanStoredArchive(ctx, name, archive, first, last, func(r sealrollup.Record) error {
		if r.Slot == physical {
			old = &r
			return nil
		}
		return yield(r)
	}); err != nil {
		return err
	}
	r, ok, err := openSlotRecord(c, archive, start, old, func(l int, first, last int64, from uint64) ([]sealrollup.Record, error) {
		var out []sealrollup.Record
		err := s.eachPartRecord(ctx, m, s.metricParts(m), l, first, last, true, func(part *readPartMetric) bool {
			return from > 0 && part.meta.Frontier.Order != 0 && part.meta.Frontier.Order < from
		}, func(r sealrollup.Record, _ partOrigin) error {
			out = append(out, r)
			return nil
		})
		return out, err
	})
	if err != nil || !ok {
		return err
	}
	return yield(r)
}

// openWindow is archive's window holding the frontier and its physical slot;
// open reports that the seal has not closed it yet.
func openWindow(c sealrollup.Config, meta metricMetadata, archive int) (start, physical int64, open bool) {
	a := c.Archives[archive]
	start = meta.Frontier.MaxTime - meta.Frontier.MaxTime%a.Step
	return start, (start / a.Step) % a.Count, meta.Version == 3 && !meta.Expired && start+a.Step > meta.ClosedThrough
}

// openSlotRecord is the open window's slot as a record: the stored per-slot
// winner old when the version read resolves to it, else the computed value
// at its event's batch order.
func openSlotRecord(c sealrollup.Config, archive int, start int64, old *sealrollup.Record, load func(l int, first, last int64, from uint64) ([]sealrollup.Record, error)) (sealrollup.Record, bool, error) {
	p, ok, err := sealrollup.OpenSlot(c, archive, start, load)
	if err != nil || !ok {
		if old != nil {
			return *old, err == nil, err
		}
		return sealrollup.Record{}, false, err
	}
	if old != nil && old.Sample.Time == p.Time && old.Sample.Bits == p.Bits {
		return *old, true, nil
	}
	p.Sequence >>= 4 // a tick (batch order, sub-step) back to its batch order
	return sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: (start / c.Archives[archive].Step) % c.Archives[archive].Count, Order: p.Sequence, Sample: p}}, true, nil
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
