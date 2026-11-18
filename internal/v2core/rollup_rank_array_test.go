package v2core

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-whisper"
)

// checkRankArray asserts the owner rank array and dense agree both ways: every
// dense metric's ID ranks to its slot, and every non-zero rank names the metric
// installed under that ID. A stale entry would route the seal sweep to the
// wrong metric.
func checkRankArray(t *testing.T, r *coreRollups, seal int) {
	t.Helper()
	for i, m := range r.dense {
		if r.metrics[m.id] != m {
			// A replaced metric lingers in dense until the next renumber,
			// but its ID must no longer rank to it.
			if !r.replaced || r.rankOf(m.id) == uint32(i+1) {
				t.Fatalf("seal %d: replaced dense[%d] id %d still ranked", seal, i, m.id)
			}
			continue
		}
		if r.rankOf(m.id) != uint32(i+1) {
			t.Fatalf("seal %d: dense[%d] id %d stale rank %d", seal, i, m.id, r.rankOf(m.id))
		}
	}
	for id, rk := range r.rank {
		if rk == 0 {
			continue
		}
		if int(rk) > len(r.dense) || r.dense[rk-1].id != uint64(id) || r.metrics[uint64(id)] != r.dense[rk-1] {
			t.Fatalf("seal %d: rank[%d]=%d names another metric", seal, id, rk)
		}
	}
}

// TestRollupRankRenumberBoundary drives names added, expired, revived and
// replaced across renumbers in consecutive seals, and compares every plan with
// a reference planner that never ranks. The rank array must stay exact after
// each commit.
func TestRollupRankRenumberBoundary(t *testing.T) {
	for _, workers := range []int{1, 3} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
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
			for seal := 0; seal < 14; seal++ {
				clock += 10
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
					batches = append(batches, coreBatch{ID: uint64(id), Policy: policies[id], Order: order, Clock: clock, Points: []row{{ID: uint64(id), Time: clock - 5, Bits: math.Float64bits(v), Seq: order}}})
				}
				rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
				if len(ranked.unranked) > 0 && len(ranked.unranked)*rankSlack >= len(ranked.dense) {
					renumberSeals++
					if ranked.replaced {
						replacedRenumbers++
					}
				} else if ranked.replaced {
					lingeringReplaced++
				}
				clear(ref.rank)
				ref.dense, ref.unranked, ref.replaced = ref.dense[:0], ref.unranked[:0], false
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
			}
			if renumberSeals < 3 || replacedRenumbers == 0 || expiredStages == 0 || lingeringReplaced == 0 {
				t.Fatalf("coverage: renumbers=%d replaced=%d expired=%d lingering=%d", renumberSeals, replacedRenumbers, expiredStages, lingeringReplaced)
			}
		})
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
