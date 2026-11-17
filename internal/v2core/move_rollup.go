package v2core

import (
	"errors"
	"slices"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// PlanMoveRollupRebuild rebuilds touched coarse windows from already resolved
// raw slots, never by adding donor and receiver partial aggregates. Call once
// during installation, then seed the live reducer with the result before read
// ownership switches. This pure planner does not install/adopt that state.
//
// The installer must retain complete fine coverage for boundary windows. It
// must also retain the original coarse slots outside that coverage, and reject
// an installation when it cannot prove coverage. now is the original adoption
// clock; order is a receiver-local projection order, never a donor frontier.
// Raw records keep their exact original orders and IEEE bits. The returned
// mixed-origin records must not be fed to a single-stream order comparator.
func PlanMoveRollupRebuild(policy RollupPolicy, winning []sealrollup.Record, now int64, order uint64) ([]sealrollup.Record, error) {
	return planMoveRollup(policy, winning, nil, now, order)
}

func planMoveRollup(policy RollupPolicy, winning, targetRaw []sealrollup.Record, now int64, order uint64) ([]sealrollup.Record, error) {
	if err := policy.Config.Validate(); err != nil {
		return nil, err
	}
	if order == 0 {
		return nil, errors.New("move rebuild requires receiver projection order")
	}
	type slot struct {
		archive  int
		physical int64
	}
	all := make(map[slot]sealrollup.Record, len(winning))
	var raw []sealrollup.Sample
	for _, r := range winning {
		if r.Archive < 0 || r.Archive >= len(policy.Config.Archives) || r.Slot < 0 || r.Slot >= policy.Config.Archives[r.Archive].Count || r.Order == 0 {
			return nil, errors.New("invalid winning move slot")
		}
		archive := policy.Config.Archives[r.Archive]
		if r.Sample.Time < now-archive.Step*archive.Count {
			continue
		}
		k := slot{r.Archive, r.Slot}
		if _, exists := all[k]; exists {
			return nil, errors.New("unresolved duplicate move slot")
		}
		all[k] = r
		if r.Archive == 0 {
			touched := targetRaw == nil
			for _, local := range targetRaw {
				if local.Sample.Time == r.Sample.Time {
					touched = true
					break
				}
			}
			if touched {
				raw = append(raw, r.Sample)
			}
		}
	}
	slices.SortFunc(raw, func(a, b sealrollup.Sample) int {
		if a.Time < b.Time {
			return -1
		}
		if a.Time > b.Time {
			return 1
		}
		return 0
	})
	view := func(archive int, physical int64) (sealrollup.Sample, bool) {
		r, ok := all[slot{archive, physical}]
		return r.Sample, ok
	}
	plan, err := sealrollup.Plan(policy.Config, sealrollup.Frontier{}, view, []sealrollup.Batch{{Order: order, Now: now, Points: raw}})
	if err != nil {
		return nil, err
	}
	for _, r := range plan.Records {
		if r.Archive != 0 {
			k := slot{r.Archive, r.Slot}
			if old, exists := all[k]; exists && old.Sample.Time != r.Sample.Time {
				newer, err := newerOrigin(partOrigin{Class: LivePart}, r, partOrigin{Class: LivePart}, old)
				if err != nil {
					return nil, err
				}
				if !newer {
					continue
				}
			}
			// For the same logical window this is a recomputation from resolved
			// raw input, replacing a partial aggregate rather than competing
			// with its unrelated original source order.
			all[k] = r
		}
	}
	out := make([]sealrollup.Record, 0, len(all))
	for _, r := range all {
		out = append(out, r)
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

// PlanMoveRollupBoundary retains mature coarse slots and recomputes only windows
// touched by the receiver's retained raw. Old coarse history needs no raw proof.
func PlanMoveRollupBoundary(policy RollupPolicy, winning, targetRaw []sealrollup.Record, now int64, order uint64) ([]sealrollup.Record, error) {
	return planMoveRollup(policy, winning, targetRaw, now, order)
}
