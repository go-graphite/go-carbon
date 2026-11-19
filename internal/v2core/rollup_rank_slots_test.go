package v2core

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// TestRollupRenumberMovesSlotColumns is the differential across renumbers for
// the rank-ordered slot arrays. Random installs, replacements of ranked and
// unranked metrics (adoption), same-metric reinstalls and hints overwritten
// by another planner run between renumbers; after every step each installed
// ID's committed scalars, read through slotOf, must equal a map model, and
// after every renumber each slot must be its metric's rank minus one.
func TestRollupRenumberMovesSlotColumns(t *testing.T) {
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
				if old := r.metrics[id]; old != nil {
					c := *old
					md := meta()
					r.install(id, &c, md)
					model[id] = md
				}
			case op < 7: // a seal commits new scalars for the same metric
				id := uint64(rng.IntN(next) + 1)
				if m := r.metrics[id]; m != nil {
					md := meta()
					r.install(id, m, md)
					model[id] = md
				}
			case op < 8: // another planner overwrites a hint
				id := uint64(rng.IntN(next) + 1)
				if m := r.metrics[id]; m != nil {
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
				if got := r.meta[r.slotOf(id, r.metrics[id])]; got != want {
					t.Fatalf("seed %d step %d: id %d scalars %+v, want %+v", seed, step, id, got, want)
				}
			}
		}
		if renumbers < 20 || deadDropped == 0 {
			t.Fatalf("seed %d: coverage renumbers=%d deadDropped=%d", seed, renumbers, deadDropped)
		}
	}
}

// TestClosureCalendarFollowsRenumber checks that the closure calendar keeps
// naming the right metric while renumber moves slots: every live entry names
// an ID filed under that end, every ID's latest open window ends are filed
// under its current slot, and dead (adopted) slots leave at renumber.
func TestClosureCalendarFollowsRenumber(t *testing.T) {
	coarse, err := sealrollup.Prepare(sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 10, Count: 100}, {Step: 60, Count: 100}, {Step: 600, Count: 100}}})
	if err != nil {
		t.Fatal(err)
	}
	for seed := uint64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewPCG(seed, 11))
		r := newCoreRollups()
		filed := map[[2]int64]bool{} // (end, id) ever filed
		latest := map[uint64][]int64{}
		next := 0
		install := func(id uint64, m *rollupMetric) {
			md := metricMetadata{Version: 3, LastUpdate: 1}
			md.Frontier.Order, md.Frontier.MaxTime = 1, int64(rng.IntN(5000))
			r.install(id, m, md)
			latest[id] = latest[id][:0]
			for _, a := range m.schema().Archives[1:] {
				if end := rollupOpenEnd(md, a); end != math.MaxInt64 {
					filed[[2]int64{end, int64(id)}] = true
					latest[id] = append(latest[id], end)
				}
			}
		}
		renumbers, deadDropped := 0, 0
		for step := 0; step < 600; step++ {
			switch op := rng.IntN(10); {
			case op < 4 || len(r.metrics) == 0:
				m := rankTestMetric(next)
				m.policy.policyDefinition = &policyDefinition{prepared: coarse, Group: m.policy.Group}
				next++
				install(m.id, m)
			case op < 6: // adoption replaces the metric under an ID
				if old := r.metrics[uint64(rng.IntN(next)+1)]; old != nil {
					c := *old
					install(old.id, &c)
				}
			case op < 7: // a seal reinstalls the same metric
				if m := r.metrics[uint64(rng.IntN(next)+1)]; m != nil {
					install(m.id, m)
				}
			default:
				if hasDeadSlot(r) {
					deadDropped++
				}
				r.renumber()
				renumbers++
				for _, slots := range r.calendar.slots {
					for _, s := range slots {
						if r.ids[s] == deadSlot {
							t.Fatalf("seed %d step %d: renumber kept dead slot %d filed", seed, step, s)
						}
					}
				}
			}
			at := map[[2]int64]bool{}
			for end, slots := range r.calendar.slots {
				for _, s := range slots {
					if id := r.ids[s]; id != deadSlot {
						if !filed[[2]int64{end, int64(id)}] || r.slotMetric(s) != r.metrics[id] {
							t.Fatalf("seed %d step %d: end %d slot %d names id %d, never filed there", seed, step, end, s, id)
						}
						at[[2]int64{end, int64(id)}] = true
					}
				}
			}
			for id, ends := range latest {
				for _, end := range ends {
					if !at[[2]int64{end, int64(id)}] {
						t.Fatalf("seed %d step %d: id %d open end %d not filed under its slot", seed, step, id, end)
					}
				}
			}
		}
		if renumbers < 20 || deadDropped == 0 {
			t.Fatalf("seed %d: coverage renumbers=%d deadDropped=%d", seed, renumbers, deadDropped)
		}
	}
}
