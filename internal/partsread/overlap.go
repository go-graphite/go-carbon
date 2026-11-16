package partsread

import (
	"context"
	"errors"
	"math"
	"sort"
)

// HasOverlayResolved conservatively checks whether a source can replace any
// physical slot of an already rendered base. Unknown sources keep the ordinary
// render path. Timestamp bounds alone are unsafe: a newer wrapped timestamp can
// invalidate a legacy value outside that timestamp's query interval.
func (h *Handle) HasOverlayResolved(ctx context.Context, token ResolvedMetric, base *Series) (bool, error) {
	if h.v == nil || h.closed.Load() {
		return false, ErrClosed
	}
	if token.generation != h.v {
		return false, ErrMetadataGeneration
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if base == nil || base.Step <= 0 || base.Until < base.From || len(base.Values) <= 1 {
		return true, nil
	}
	return h.hasOverlayRange(ctx, token, base.Step, base.From, base.Until)
}

// HasOverlayRangeResolved checks physical slots before fetching a dense legacy
// result. The caller supplies the already selected and aligned archive shape.
// Degenerate ranges conservatively retain the ordinary shape reconciliation.
func (h *Handle) HasOverlayRangeResolved(ctx context.Context, token ResolvedMetric, step, from, until int64) (bool, error) {
	if h.v == nil || h.closed.Load() {
		return false, ErrClosed
	}
	if token.generation != h.v {
		return false, ErrMetadataGeneration
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if step <= 0 || until < from || (until-from)/step <= 1 {
		return true, nil
	}
	return h.hasOverlayRange(ctx, token, step, from, until)
}

func (h *Handle) hasOverlayRange(ctx context.Context, token ResolvedMetric, step, from, until int64) (bool, error) {
	var archive Archive
	for _, a := range token.metric.Archives {
		if a.Step == step {
			archive = a
			break
		}
	}
	if archive.Points <= 0 {
		return true, nil
	}
	for _, group := range [][]Source{h.v.view.Head, h.v.view.Parts, h.v.view.Blocks} {
		for _, source := range group {
			s, ok := source.(interface {
				MayOverlap(context.Context, uint64, Archive, int64, int64) (bool, error)
			})
			if !ok {
				return true, nil
			}
			overlaps, err := s.MayOverlap(ctx, token.metric.ID, archive, from, until)
			if err != nil || overlaps {
				return overlaps, err
			}
		}
	}
	return false, nil
}

func (s *SlotIndex) MayOverlap(ctx context.Context, id uint64, a Archive, from, until int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ranges, n := SlotRanges(a, from, until)
	for i := s.first(id, a.Step); i < len(s.blocks); i++ {
		b := &s.blocks[i]
		if b.ID != id || b.Archive.Step != a.Step {
			break
		}
		if b.Archive != a {
			continue
		}
		for _, r := range ranges[:n] {
			if max(r.From, b.Slots.From) < min(r.Until, b.Slots.Until) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (f *Frozen) MayOverlap(ctx context.Context, id uint64, a Archive, from, until int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	records := f.metric(id, a.Step)
	n := (until - from) / a.Step
	if n <= 0 {
		return false, nil
	}
	first := (from / a.Step) % a.Points
	if first < 0 {
		first += a.Points
	}
	for i := 0; i < len(records); {
		if err := ctx.Err(); err != nil {
			return false, err
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
			return true, nil
		}
		delta := (first - slot + a.Points) % a.Points * a.Step
		if p.Time > math.MaxInt64-delta {
			break
		}
		next := p.Time + delta
		i++
		i += sort.Search(len(records)-i, func(j int) bool { return records[i+j].Sample.Time >= next })
	}
	return false, nil
}

// JoinLegacyPrefix merges into the legacy reader's owned values, visiting only
// occupied part slots. Pooled workspace values remain untouched so release stays
// proportional to part occupancy rather than the dense legacy prefix.
func (h *Handle) JoinLegacyPrefix(ctx context.Context, out *Series, values []float64) error {
	if h.v == nil || h.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w := h.workspaces
	if w == nil || w.out != out || len(out.Values) != len(w.values) || len(values) > len(out.Values) {
		return errors.New("legacy prefix does not belong to render handle")
	}
	prefix := len(values)
	joined := values
	if prefix < len(out.Values) {
		bytes := int64(len(out.Values)) * 8
		if bytes > h.work {
			return ErrAdmission
		}
		h.work -= bytes
		joined = make([]float64, len(out.Values))
		copy(joined, values)
		copy(joined[prefix:], out.Values[prefix:])
	}
	for _, i := range w.touched {
		if int(i) < prefix {
			joined[i] = out.Values[i]
		}
	}
	out.Values = joined
	return ctx.Err()
}
