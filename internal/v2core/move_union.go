package v2core

import (
	"context"
	"slices"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// moveUnionWindows writes only mixed windows. A source-local Mat cannot bound
// the other stream's events. Resolve the retained raw slots first, then apply
// the archive ladder. A retired donor contributes only the fine points it still
// holds, matching the existing coarse fallback rather than inventing history.
func moveUnionWindows(ctx context.Context, donor, receiver *RollupSnapshot, name string, c sealrollup.Config, order uint64, now int64) ([]sealrollup.Record, error) {
	type window struct {
		archive int
		time    int64
	}
	sides := map[window]uint8{}
	rawSides := map[window]uint8{}
	viewRecords := map[[2]int64]moveWinner{}
	var targetRaw []sealrollup.Sample
	for side, index := range []*RollupSnapshot{donor, receiver} {
		m := index.metrics[name]
		if m == nil {
			continue
		}
		for archive, a := range c.Archives {
			winners := map[int64]moveWinner{}
			if err := collectMoveArchive(ctx, index, name, archive, winners); err != nil {
				return nil, err
			}
			for _, v := range winners {
				r := v.record
				// Preparation is an offline donor view; all of its inputs are copied.
				if side == 0 {
					v.origin.Class = CopiedPart
				}
				// A prior attachment is still the donor stream. Counting it as
				// receiver input would turn single-stream retired history into a
				// mixed window whose missing raw can only yield a None marker.
				stream := uint8(1 << side)
				if v.origin.Class == CopiedPart {
					stream = 1
				}
				for parent := max(1, archive); parent < len(c.Archives); parent++ {
					step := c.Archives[parent].Step
					sides[window{parent, r.Sample.Time - r.Sample.Time%step}] |= stream
				}
				key := [2]int64{int64(archive), r.Slot}
				if old, ok := viewRecords[key]; !ok {
					viewRecords[key] = v
				} else {
					newer, err := newerOrigin(v.origin, r, old.origin, old.record)
					if err != nil {
						return nil, err
					}
					if newer {
						viewRecords[key] = v
					}
				}
				if archive != 0 {
					continue
				}
				for parent := 1; parent < len(c.Archives); parent++ {
					step := c.Archives[parent].Step
					rawSides[window{parent, r.Sample.Time - r.Sample.Time%step}] |= stream
				}
			}
			// A TTL Mat need not have a per-slot compatibility winner. Include its
			// window in the mixed-window inventory without treating it as fine input.
			if err := index.eachPartRecord(ctx, name, m, index.metricParts(name, m), archive, 0, a.Count, true, nil, func(r sealrollup.Record, origin partOrigin) error {
				stream := uint8(1 << side)
				if origin.Class == CopiedPart {
					stream = 1
				}
				for parent := max(1, archive); parent < len(c.Archives); parent++ {
					step := c.Archives[parent].Step
					sides[window{parent, r.Sample.Time - r.Sample.Time%step}] |= stream
				}
				// A receiver write-down is its own fallback even when no seal
				// wrote a compatibility value. A donor-only scalar cannot repair
				// a mixed window after its fine inputs have been retired.
				if side == 1 && archive > 0 && origin.Class == LivePart && (r.Kind == sealrollup.KindMat || r.Kind == sealrollup.KindNone) {
					if index.hasCopied && r.Order >= moveLiveOrder {
						r.Order -= moveLiveOrder
					}
					key := [2]int64{int64(archive), r.Slot}
					old, exists := viewRecords[key]
					newer, err := newerOrigin(origin, r, old.origin, old.record)
					if err != nil {
						return err
					}
					if !exists || newer {
						viewRecords[key] = moveWinner{r, origin}
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}
	var samples []sealrollup.Sample
	fallback := map[window]sealrollup.Record{}
	fine := c.Archives[0]
	for key, v := range viewRecords {
		if key[0] > 0 {
			fallback[window{v.record.Archive, v.record.Sample.Time}] = v.record
			continue
		}
		samples = append(samples, v.record.Sample)
		if v.origin.Class == LivePart && v.record.Sample.Time >= now-fine.Step*fine.Count {
			targetRaw = append(targetRaw, v.record.Sample)
		}
	}
	slices.SortFunc(samples, compareHistorySamples)
	// The compatibility fallback is the old boundary plan over its surviving
	// per-slot view: only retained receiver raw triggers a propagation. This
	// preserves historical XFF and rounding when a side has retired its raw.
	slices.SortFunc(targetRaw, compareHistorySamples)
	plan, err := sealrollup.Plan(c, sealrollup.Frontier{}, func(a int, slot int64) (sealrollup.Sample, bool) {
		v, ok := viewRecords[[2]int64{int64(a), slot}]
		return v.record.Sample, ok
	}, []sealrollup.Batch{{Order: max(order, 1), Now: now, Points: targetRaw}})
	if err != nil {
		return nil, err
	}
	for _, r := range plan.Records {
		if r.Archive > 0 {
			fallback[window{r.Archive, r.Sample.Time}] = r
		}
	}
	var out []sealrollup.Record
	for archive := 1; archive < len(c.Archives); archive++ {
		step := c.Archives[archive].Step
		var next []sealrollup.Sample
		// Include single-stream windows in the intermediate ladder, while only
		// persisting union values for windows actually touched by both streams.
		windows := map[int64]bool{}
		for w := range sides {
			if w.archive == archive {
				windows[w.time] = true
			}
		}
		for start := range windows {
			i, _ := slices.BinarySearchFunc(samples, start, func(s sealrollup.Sample, t int64) int {
				if s.Time < t {
					return -1
				}
				if s.Time > t {
					return 1
				}
				return 0
			})
			j := i
			for j < len(samples) && samples[j].Time < start+step {
				j++
			}
			value, ok := sealrollup.AggregateWindow(c, archive, start, samples[i:j])
			if ok {
				next = append(next, value)
			}
			if sides[window{archive, start}] != 3 {
				continue
			}
			r := sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: start / step % c.Archives[archive].Count, Order: order, Sample: sealrollup.Sample{Time: start, Bits: value.Bits, Sequence: order}}, Kind: sealrollup.KindMat, Upto: sealrollup.Tick(order, archive-1)}
			if rawSides[window{archive, start}] != 3 {
				// A missing side's raw cannot be reconstructed from a scalar.
				prior, exists := fallback[window{archive, start}]
				r.Sample.Bits, ok = prior.Sample.Bits, exists && prior.Kind != sealrollup.KindNone
			}
			if !ok {
				r.Kind = sealrollup.KindNone
			}
			out = append(out, r)
		}
		slices.SortFunc(next, compareHistorySamples)
		samples = next
	}
	slices.SortFunc(out, func(a, b sealrollup.Record) int {
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
	return out, nil
}

func compareHistorySamples(a, b sealrollup.Sample) int {
	if a.Time < b.Time {
		return -1
	}
	if a.Time > b.Time {
		return 1
	}
	return 0
}
