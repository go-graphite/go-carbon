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

// scanOpenArchive adds at most one virtual slot. Its source scan uses the same
// pinned generation, physical winner resolver and ring ranges as stored reads.
// Recursion only follows the one open child, so work is the sum of step ratios.
func (s *RollupSnapshot) scanOpenArchive(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metric(name)
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
