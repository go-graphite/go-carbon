package v2core

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-whisper"
)

// checkRankArray asserts the owner slot arrays and dense agree both ways:
// every dense metric's slot holds its ID and ranks to its position, and every
// non-zero rank names the metric installed under that slot's ID. A stale
// entry would route the seal sweep to the wrong metric.
func checkRankArray(t *testing.T, r *coreRollups, seal int) {
	t.Helper()
	if len(r.ids) != len(r.rank) || len(r.denseSlots) != len(r.dense) || len(r.unrankedSlots) != len(r.unranked) {
		t.Fatalf("seal %d: slot arrays out of step: ids %d rank %d dense %d/%d unranked %d/%d", seal, len(r.ids), len(r.rank), len(r.dense), len(r.denseSlots), len(r.unranked), len(r.unrankedSlots))
	}
	for i, m := range r.dense {
		s := r.denseSlots[i]
		if r.ids[s] != m.id {
			t.Fatalf("seal %d: dense[%d] id %d in slot %d of id %d", seal, i, m.id, s, r.ids[s])
		}
		if r.metrics[m.id] != m {
			// A replaced metric lingers in dense until the next renumber,
			// but its slot must no longer rank to it.
			if !r.replaced || r.rank[s] == uint32(i+1) {
				t.Fatalf("seal %d: replaced dense[%d] id %d still ranked", seal, i, m.id)
			}
			continue
		}
		if r.rank[s] != uint32(i+1) {
			t.Fatalf("seal %d: dense[%d] id %d stale rank %d", seal, i, m.id, r.rank[s])
		}
	}
	for k, m := range r.unranked {
		if r.ids[r.unrankedSlots[k]] != m.id {
			t.Fatalf("seal %d: unranked[%d] id %d in slot of id %d", seal, k, m.id, r.ids[r.unrankedSlots[k]])
		}
	}
	for s, rk := range r.rank {
		if rk == 0 {
			continue
		}
		if int(rk) > len(r.dense) || r.dense[rk-1].id != r.ids[s] || r.metrics[r.ids[s]] != r.dense[rk-1] {
			t.Fatalf("seal %d: rank[%d]=%d names another metric", seal, s, rk)
		}
	}
}

// TestRollupRankRenumberBoundary drives names added, expired, revived and
// replaced across renumbers in consecutive seals, and compares every plan with
// a reference planner that never ranks. The rank array must stay exact after
// each commit.
func TestRollupRankRenumberBoundary(t *testing.T) {
	for _, workers := range []int{1, 3} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) { rankBoundaryRun(t, workers, false) })
	}
}

// TestRollupSlotAdoptionInterleaved is the boundary script with registered
// batches (the slot hint path) and an aborted adoption before each
// replacement: a planner sharing every identity installs them in a shuffled
// order, plans a seal and is discarded, which overwrites the donor's hints.
// The next seal replaces metrics in place and the one after renumbers, so
// abort, replace and renumber run in consecutive seals. Plans must still
// match the reference, the slot arrays must stay exact, and the renumber
// must restore every dense hint.
func TestRollupSlotAdoptionInterleaved(t *testing.T) {
	for _, workers := range []int{1, 3} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) { rankBoundaryRun(t, workers, true) })
	}
}

func rankBoundaryRun(t *testing.T, workers int, adopt bool) {
	{
		{
			rng := rand.New(rand.NewPCG(uint64(workers), 11))
			const ids = 640
			policies := make([][]byte, ids+1)
			for id := 1; id <= ids; id++ {
				def := "10s:3d"
				if id%3 == 0 {
					def = "10s:2d,1m:14d,30m:2y"
				}
				p := RollupPolicy{Name: partsformat.ID(rankTestName(id)), Group: partsformat.Group([]string{"test", "other"}[id%2]), Config: sealrollup.Config{Method: sealrollup.Average}}
				for _, rd := range whisper.MustParseRetentionDefs(def) {
					p.Config.Archives = append(p.Config.Archives, sealrollup.Archive{Step: int64(rd.SecondsPerPoint()), Count: int64(rd.NumberOfPoints())})
				}
				var err error
				if policies[id], err = EncodeRollupPolicy(p); err != nil {
					t.Fatal(err)
				}
			}
			// Waves: ids 1-200 at seal 0, 201-400 at seal 3 (renumber at the
			// start of seal 4), 401-640 at seal 4 (fresh in the renumber seal).
			born := func(id int) int {
				switch {
				case id <= 200:
					return 0
				case id <= 400:
					return 3
				}
				return 4
			}
			// id%5==0 go quiet after seal 1 and expire around the renumber
			// seals; they come back at seal 9.
			active := func(id, seal int) bool {
				if seal < born(id) {
					return false
				}
				if id%5 == 0 && seal >= 2 && seal < 9 {
					return false
				}
				return seal == born(id) || rng.IntN(4) != 0
			}
			clock := int64(1801958400)
			newPlanner := func() *coreRollups {
				r := newCoreRollups()
				r.parallelism = workers
				r.now = func() time.Time { return time.Unix(clock, 0) }
				r.expire = func(_ RollupPolicy, m RollupMetadata, now time.Time) bool {
					return m.LastUpdate > 0 && now.Unix()-m.LastUpdate/1e9 > 25
				}
				return r
			}
			ranked, ref := newPlanner(), newPlanner()
			order := uint64(0)
			var renumberSeals, replacedRenumbers, expiredStages, lingeringReplaced int
			var lostRanked, restoredRenumbers, hintHits int
			for seal := 0; seal < 14; seal++ {
				clock += 10
				if adopt && (seal == 3 || seal == 7) {
					// The previous seal aborted an adoption; count ranked
					// names whose hint no longer leads to their slot.
					for i, m := range ranked.dense {
						if s, ok := ranked.hintSlot(m.PolicyIdentity, m.id); !ok || s != ranked.denseSlots[i] {
							lostRanked++
						}
					}
				}
				switch seal {
				case 4, 7:
					// A move replaces metrics in place with fresh state. At
					// seal 4 the renumber runs first; at seal 7 the few
					// replacements linger below the threshold, so the sweep
					// itself must not reach the old metric through rank.
					mod := uint64(7)
					if seal == 7 {
						mod = 97
					}
					for _, p := range []*coreRollups{ranked, ref} {
						for id, m := range p.metrics {
							if id%mod == 1 {
								p.install(id, newRollupMetric(m.PolicyIdentity))
							}
						}
					}
				}
				var batches []coreBatch
				for id := 1; id <= ids; id++ {
					if !active(id, seal) {
						continue
					}
					order++
					v := float64(rng.IntN(1000)) / 4
					b := coreBatch{ID: uint64(id), Policy: policies[id], Order: order, Clock: clock, Points: []row{{ID: uint64(id), Time: clock - 5, Bits: math.Float64bits(v), Seq: order}}}
					if adopt || id%2 == 1 {
						// Registered batches take the slot hint path. The
						// reference planner has its own catalog, so it plans
						// these from the policy bytes.
						h, err := ranked.identities.intern(uint64(id), "", policies[id])
						if err != nil {
							t.Fatal(err)
						}
						b.Registered = h
					}
					batches = append(batches, b)
				}
				rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
				if adopt && (seal == 2 || seal == 6) {
					// A failed move: an adopter shares every metric and
					// identity, installs them in map-like order, plans this
					// seal (its renumber re-stores the hints) and is dropped.
					next := newPlanner()
					next.identities = ranked.identities
					adopted := make([]uint64, 0, len(ranked.metrics))
					for id := range ranked.metrics {
						adopted = append(adopted, id)
					}
					slices.Sort(adopted)
					rng.Shuffle(len(adopted), func(i, j int) { adopted[i], adopted[j] = adopted[j], adopted[i] })
					for _, id := range adopted {
						m := ranked.metrics[id]
						next.install(id, m)
						next.names[m.policy.Name] = id
					}
					plan, err := next.plan(coreSealInput{Batches: batches})
					if err != nil {
						t.Fatal(err)
					}
					plan.Abort()
				}
				renumbering := len(ranked.unranked) > 0 && len(ranked.unranked)*rankSlack >= len(ranked.dense)
				if !renumbering {
					for i := range batches {
						if ranked.hintRank(batches[i].Registered, batches[i].ID) != 0 {
							hintHits++
						}
					}
				}
				if renumbering {
					renumberSeals++
					if ranked.replaced {
						replacedRenumbers++
					}
				} else if ranked.replaced {
					lingeringReplaced++
				}
				clear(ref.rank)
				ref.dense, ref.unranked, ref.replaced = ref.dense[:0], ref.unranked[:0], false
				ref.denseSlots, ref.unrankedSlots = ref.denseSlots[:0], ref.unrankedSlots[:0]
				planA, err := ranked.plan(coreSealInput{Batches: batches})
				if err != nil {
					t.Fatal(err)
				}
				planB, err := ref.plan(coreSealInput{Batches: batches})
				if err != nil {
					t.Fatal(err)
				}
				for _, s := range ranked.stages {
					if len(s.batches) == 0 && s.next.Expired {
						expiredStages++
					}
				}
				gotA, gotB := rankBoundaryDecode(t, planA), rankBoundaryDecode(t, planB)
				planA.Commit()
				planB.Commit()
				if len(gotA) == 0 || !reflect.DeepEqual(gotA, gotB) {
					t.Fatalf("seal %d: ranked and reference plans differ", seal)
				}
				checkRankArray(t, ranked, seal)
				if renumbering {
					for i, m := range ranked.dense {
						if s, ok := ranked.hintSlot(m.PolicyIdentity, m.id); !ok || s != ranked.denseSlots[i] {
							t.Fatalf("seal %d: renumber left dense[%d] id %d without its hint", seal, i, m.id)
						}
					}
					if adopt {
						restoredRenumbers++
					}
				}
			}
			if renumberSeals < 3 || replacedRenumbers == 0 || expiredStages == 0 || lingeringReplaced == 0 || hintHits == 0 {
				t.Fatalf("coverage: renumbers=%d replaced=%d expired=%d lingering=%d hint hits=%d", renumberSeals, replacedRenumbers, expiredStages, lingeringReplaced, hintHits)
			}
			if adopt && (lostRanked == 0 || restoredRenumbers == 0) {
				t.Fatalf("adoption coverage: lost ranked hints=%d restoring renumbers=%d", lostRanked, restoredRenumbers)
			}
		}
	}
}

// TestPolicyIdentitySlotFitsPadding pins the slot hint at 0 B per name: it
// lives in PolicyIdentity's padding, and rollupMetric keeps #1587's size.
func TestPolicyIdentitySlotFitsPadding(t *testing.T) {
	if got := reflect.TypeFor[PolicyIdentity]().Size(); got != 56 {
		t.Fatalf("PolicyIdentity is %d B, want 56", got)
	}
	if got := reflect.TypeFor[rollupMetric]().Size(); got != 160 {
		t.Fatalf("rollupMetric is %d B, want 160", got)
	}
}

// TestRollupSlotArraysScaleWithNames installs names whose NumericIDs are
// spread as on a 96-producer host (one owner sees about 1/96 of the ID
// space) and checks the owner arrays are sized by names, not by max ID, and
// that in-place replacement reuses the slot.
func TestRollupSlotArraysScaleWithNames(t *testing.T) {
	const names, producers = 3000, 96
	r := newCoreRollups()
	maxID := uint64(0)
	for i := range names {
		m := rankTestMetric(i)
		m.id = uint64(i)*producers + 37
		maxID = max(maxID, m.id)
		r.install(m.id, m)
	}
	r.renumber()
	for id, m := range r.metrics {
		if id%5 == 0 {
			c := *m
			r.install(id, &c)
		}
	}
	r.renumber()
	checkDense(t, r)
	checkRankArray(t, r, 0)
	if len(r.ids) != names || len(r.rank) != names || len(r.denseSlots) != names {
		t.Fatalf("slots: ids %d rank %d denseSlots %d, want %d each", len(r.ids), len(r.rank), len(r.denseSlots), names)
	}
	bytes := 8*cap(r.ids) + 4*(cap(r.rank)+cap(r.denseSlots)+cap(r.denseSlotsSpare)+cap(r.unrankedSlots))
	if limit := 2 * 24 * names; bytes > limit || uint64(bytes) >= maxID {
		t.Fatalf("owner slot arrays hold %d B for %d names (max ID %d), limit %d", bytes, names, maxID, limit)
	}
}

func rankBoundaryDecode(t *testing.T, plan coreSealPlan) [][][3]string {
	t.Helper()
	var files [][][3]string
	for _, file := range plan.Files {
		var got [][3]string
		for i, kv := range file {
			if i > 0 && string(file[i-1].Key) >= string(kv.Key) {
				t.Fatalf("plan keys out of order at %d", i)
			}
			got = append(got, [3]string{string(kv.Name), string(kv.Key[len(kv.Key)-12:]), string(kv.encodedValue())})
		}
		files = append(files, got)
	}
	return files
}
