package v2read

import (
	"context"
	"errors"
	"slices"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// touchedRanges lists, per archive, the physical slots one request can
// observe: the render window of the archive Render selects (plus one slot for
// its empty-window probe), and every slot the pending reducer reads. For each
// pending point that is its slot in every archive and the finer-archive window
// feeding each coarse bucket it touches. A base holding exactly these slots
// renders the same bits as the full-history base.
func touchedRanges(archives []sealrollup.Archive, batches []sealrollup.Batch, from, until, now int64) [][]SlotRange {
	out := make([][]SlotRange, len(archives))
	add := func(archive int, start, end int64) {
		a := archives[archive]
		rs, n := SlotRanges(Archive{a.Step, a.Count}, start, end)
		out[archive] = append(out[archive], rs[:n]...)
	}
	var retention int64
	for _, a := range archives {
		retention = max(retention, a.Step*a.Count)
	}
	if oldest := now - retention; from <= now && until >= oldest {
		from, until = max(from, oldest), min(until, now)
		selected := len(archives) - 1
		for i, a := range archives {
			if a.Step*a.Count >= now-from {
				selected = i
				break
			}
		}
		step := archives[selected].Step
		add(selected, alignNext(from, step), alignNext(until, step)+step)
	}
	for _, b := range batches {
		for _, p := range b.Points {
			for archive, a := range archives {
				start := p.Time - p.Time%a.Step
				add(archive, start, start+a.Step)
				if archive > 0 {
					add(archive-1, start, start+a.Step)
				}
			}
		}
	}
	for archive, rs := range out {
		slices.SortFunc(rs, func(a, b SlotRange) int { return int(a.From - b.From) })
		merged := rs[:0]
		for _, r := range rs {
			if n := len(merged); n > 0 && r.From <= merged[n-1].Until {
				merged[n-1].Until = max(merged[n-1].Until, r.Until)
				continue
			}
			merged = append(merged, r)
		}
		out[archive] = merged
	}
	return out
}

// rangeBase reads only the touched slots of one name. Physical winners resolve
// per slot, so each slot equals the full-history read. HasArchive falls back to
// the stored archive for its whole-archive question.
func (s *generation) rangeBase(ctx context.Context, m *metricState, ranges [][]SlotRange) (*resolvedSource, error) {
	var records []sealrollup.Record
	for archive, rs := range ranges {
		for _, r := range rs {
			err := s.sealed.ScanArchive(ctx, m.name, archive, r.From, r.Until, func(record sealrollup.Record) error {
				records = append(records, record)
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	source := newResolvedSource(m.meta.ID, m.policy.Config.Archives, records)
	source.stored = func(ctx context.Context, archive int) (bool, error) {
		found := errors.New("archive found")
		err := s.sealed.ScanArchive(ctx, m.name, archive, 0, m.policy.Config.Archives[archive].Count, func(sealrollup.Record) error { return found })
		if errors.Is(err, found) {
			return true, nil
		}
		return false, err
	}
	return source, nil
}
