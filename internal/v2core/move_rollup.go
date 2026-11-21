package v2core

import (
	"errors"
	"fmt"
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
	touched := make(map[int64]struct{}, len(targetRaw))
	for _, local := range targetRaw {
		touched[local.Sample.Time] = struct{}{}
	}
	var raw []sealrollup.Sample
	for _, r := range winning {
		if r.Archive < 0 || r.Archive >= len(policy.Config.Archives) || r.Slot < 0 || r.Slot >= policy.Config.Archives[r.Archive].Count || r.Order == 0 {
			return nil, errors.New("invalid winning move slot")
		}
		k := slot{r.Archive, r.Slot}
		if _, exists := all[k]; exists {
			return nil, errors.New("unresolved duplicate move slot")
		}
		all[k] = r
		fine := policy.Config.Archives[0]
		if r.Archive == 0 && r.Sample.Time >= now-fine.Step*fine.Count {
			_, local := touched[r.Sample.Time]
			if targetRaw == nil || local {
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
	// A window straddling a retention floor needs its surviving older
	// neighbors too. Never silently replace a complete coarse value with a
	// partial aggregate when those neighbors have already been overwritten.
	checked := map[slot]bool{}
	for _, point := range raw {
		for archive := 1; archive < len(policy.Config.Archives); archive++ {
			higher, coarse := policy.Config.Archives[archive-1], policy.Config.Archives[archive]
			start := point.Time - point.Time%coarse.Step
			k := slot{archive, start}
			if checked[k] {
				continue
			}
			checked[k] = true
			cutoff := now - higher.Step*higher.Count
			for ts := start; ts < min(start+coarse.Step, cutoff); ts += higher.Step {
				p, ok := view(archive-1, ts/higher.Step%higher.Count)
				if !ok || p.Time != ts {
					return nil, fmt.Errorf("move boundary archive %d at %d lacks expired fine slot %d", archive, start, ts)
				}
			}
		}
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
		a := policy.Config.Archives[r.Archive]
		if r.Sample.Time >= now-a.Step*a.Count {
			out = append(out, r)
		}
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
	if targetRaw == nil {
		targetRaw = []sealrollup.Record{}
	}
	return planMoveRollup(policy, winning, targetRaw, now, order)
}
