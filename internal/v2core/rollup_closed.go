package v2core

import (
	"cmp"
	"context"
	"slices"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// scanWindows yields archive's physical slots [first, last), each as its
// newest window at or before the frontier's, computed from stored versions
// (windowRecords). Copied orders are translated by the part bindings.
func (s *RollupSnapshot) scanWindows(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	c, meta := m.latest.schema(), m.latest.meta
	if meta.Version != 3 || meta.Frontier.Order == 0 && s.moves[m] == nil {
		return s.scanStoredArchive(ctx, name, archive, first, last, yield)
	}
	var stored []sealrollup.Record
	if err := s.scanStoredArchive(ctx, name, archive, first, last, func(r sealrollup.Record) error {
		stored = append(stored, r)
		return nil
	}); err != nil {
		return err
	}
	if move := s.moves[m]; move != nil && move.authority != nil && meta.ResetOrder <= move.authority.AtOrder {
		for _, r := range move.authority.Coarse {
			if r.Archive != archive || r.Slot < first || r.Slot >= last {
				continue
			}
			kept := stored[:0]
			for _, old := range stored {
				if old.Sample.Time != r.Sample.Time {
					kept = append(kept, old)
				}
			}
			stored = kept
			if r.Kind != sealrollup.KindNone {
				stored = append(stored, r)
			}
		}
	}
	return windowRecords(c, meta, archive, first, last, stored, func(l int, first, last int64, from uint64) ([]sealrollup.Record, error) {
		var out []sealrollup.Record
		err := s.eachPartRecord(ctx, m, s.metricParts(m), l, first, last, true, func(part *readPartMetric) bool {
			return s.moves[m] == nil && from > 0 && part.meta.Frontier.Order != 0 && part.meta.Frontier.Order < from
		}, func(r sealrollup.Record, _ partOrigin) error {
			out = append(out, r)
			return nil
		})
		if move := s.moves[m]; move != nil && move.authority != nil && meta.ResetOrder <= move.authority.AtOrder {
			for _, r := range move.authority.Coarse {
				if r.Archive != l || r.Slot < first || r.Slot >= last {
					continue
				}
				// This Mat is the union at adopt, so its bound covers both
				// classes. Original Mats describe only their source stream.
				kept := out[:0]
				for _, old := range out {
					if old.Sample.Time != r.Sample.Time || old.Kind == sealrollup.KindRaw || old.Kind == sealrollup.KindGhost {
						kept = append(kept, old)
					}
				}
				out = kept
				mapped, e := moveVersionRecord(r, nil)
				if e != nil {
					return nil, e
				}
				out = append(out, mapped)
			}
		}
		return out, err
	}, func(r sealrollup.Record) error {
		if s.moves[m] != nil && r.Order >= moveLiveOrder {
			r.Order -= moveLiveOrder
			r.Sample.Sequence = r.Order
		}
		return yield(r)
	}, s.moves[m] != nil)
}

// windowRecords yields archive's physical slots [first, last) in slot order,
// each slot as its newest window at or before the frontier's window, computed
// from stored versions by one lazy descent (sealrollup.OpenSlots): windows a
// merge settled stop at their written-down value, every level's physical
// range is read once. A slot keeps its stored per-slot winner (stored, the
// winners of the range) when that equals the computed value (its order then
// stays the stored one) or when its window has no value (an older epoch).
func windowRecords(c sealrollup.Config, meta metricMetadata, archive int, first, last int64, stored []sealrollup.Record, load func(l int, first, last int64, from uint64) ([]sealrollup.Record, error), yield func(sealrollup.Record) error, timeFirst ...bool) error {
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
	}, timeFirst...); err != nil {
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
