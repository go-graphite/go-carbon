package v2core

import (
	"math/rand/v2"
	"testing"
)

// TestRollupRenumberMovesSlotColumns is the differential across renumbers for
// the rank-ordered slot arrays. Random installs, replacements of ranked and
// unranked metrics (adoption), same-metric reinstalls and hints overwritten
// by another planner run between renumbers; after every step each installed
// ID's committed scalars, read through slotOf, must equal a map model, and
// after every renumber each slot must be its metric's rank minus one.
func TestRollupRenumberMovesSlotColumns(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		r := newCoreRollups()
		model := map[uint64]metricMetadata{}
		next := 0
		rev := uint64(0)
		meta := func() metricMetadata {
			rev++
			return metricMetadata{Revision: rev, LastUpdate: int64(rev) * 7, Version: 3}
		}
		renumbers, deadDropped := 0, 0
		for step := 0; step < 600; step++ {
			switch op := rng.IntN(10); {
			case op < 4 || len(model) == 0: // a new name
				m := rankTestMetric(next)
				next++
				md := meta()
				r.install(m.id, m, md)
				model[m.id] = md
			case op < 6: // adoption replaces the metric under an ID
				id := uint64(rng.IntN(next) + 1)
				if old := r.installed(id); old != nil {
					c := *old
					md := meta()
					r.install(id, &c, md)
					model[id] = md
				}
			case op < 7: // a seal commits new scalars for the same metric
				id := uint64(rng.IntN(next) + 1)
				if m := r.installed(id); m != nil {
					md := meta()
					r.install(id, m, md)
					model[id] = md
				}
			case op < 8: // another planner overwrites a hint
				id := uint64(rng.IntN(next) + 1)
				if m := r.installed(id); m != nil {
					m.ownerSlot.Store(uint32(rng.IntN(len(r.ids) + 2)))
				}
			default:
				if hasDeadSlot(r) {
					deadDropped++
				}
				r.renumber()
				renumbers++
				checkDense(t, r)
				for s, m := range r.dense {
					if m.ownerSlot.Load() != uint32(s)+1 {
						t.Fatalf("seed %d step %d: renumber left dense[%d] without its hint", seed, step, s)
					}
				}
			}
			checkRankArray(t, r, step)
			for id, want := range model {
				if got := r.meta[r.slotOf(id, r.installed(id))]; got != want {
					t.Fatalf("seed %d step %d: id %d scalars %+v, want %+v", seed, step, id, got, want)
				}
			}
		}
		if renumbers < 20 || deadDropped == 0 {
			t.Fatalf("seed %d: coverage renumbers=%d deadDropped=%d", seed, renumbers, deadDropped)
		}
	}
}
