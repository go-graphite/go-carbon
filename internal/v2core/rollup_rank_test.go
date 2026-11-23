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

// rankTestName mixes long shared prefixes (equal lengths, tie-breaks deep in
// the name), short names and multi-byte runes.
func rankTestName(i int) string {
	switch i % 5 {
	case 0:
		return fmt.Sprintf("a.%d", i)
	case 1:
		return fmt.Sprintf("é.%d", i)
	default:
		return fmt.Sprintf("containers.k8s.prod.cluster-%02d.ns-%03d.pod-%06x.cpu", i%3, (i/3)%5, i)
	}
}

var rankTestPrepared = func() *sealrollup.Prepared {
	p, err := sealrollup.Prepare(sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 10, Count: 100}}})
	if err != nil {
		panic(err)
	}
	return p
}()

func rankTestMetric(i int) *rollupMetric {
	def := &policyDefinition{prepared: rankTestPrepared, Group: partsformat.Group([]string{"z", "ab", "aa", "a"}[i%4])}
	return &rollupMetric{PolicyIdentity: &PolicyIdentity{id: uint64(i + 1), policy: policyName{Name: partsformat.ID(rankTestName(i)), policyDefinition: def}}, vshard: uint16((i * 2654435761) % 4096)}
}

// checkDense asserts every live metric is ranked exactly once, in canonical
// order, and that no dead metric keeps a rank.
func checkDense(t *testing.T, r *coreRollups) {
	t.Helper()
	if len(r.dense) != len(r.metrics) {
		t.Fatalf("dense %d != metrics %d", len(r.dense), len(r.metrics))
	}
	for i, m := range r.dense {
		if r.ids[i] != m.id || r.metrics[m.id] != m {
			t.Fatalf("dense[%d] stale", i)
		}
		if i > 0 && compareRollupMetrics(r.dense[i-1], m) >= 0 {
			t.Fatalf("dense out of order at %d", i)
		}
	}
}

// TestRollupRankSortMatchesMetricOrder checks rank keys plus the radix sort
// against compareRollupMetrics with ranked, unranked, replaced (copied) and
// reinstalled metrics, on both the small (comparison) and radix paths.
func TestRollupRankSortMatchesMetricOrder(t *testing.T) {
	for _, n := range []int{100, 3000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(n), 3))
			r := newCoreRollups()
			all := make([]*rollupMetric, n)
			for i := range all {
				all[i] = rankTestMetric(i)
			}
			for _, i := range rng.Perm(n)[:n/2] {
				r.install(all[i].id, all[i], metricMetadata{})
			}
			r.renumber()
			checkDense(t, r)
			// Replace a few installed metrics with struct copies (stale rank),
			// then reinstall some originals over their copies.
			origs := map[uint64]*rollupMetric{}
			for id, m := range r.metrics {
				if id%7 == 0 {
					c := *m
					origs[id] = m
					r.install(id, &c, r.metaOf(id))
					all[id-1] = &c
				}
			}
			for id, m := range origs {
				if id%2 == 0 {
					r.install(id, m, r.metaOf(id))
					all[id-1] = m
				}
			}
			// Unranked installs stay below the renumber threshold.
			for i := range all {
				if r.metrics[all[i].id] == nil && i%13 == 0 {
					r.install(all[i].id, all[i], metricMetadata{})
				}
			}
			for round := 0; round < 2; round++ {
				var refs []rollupRankRef
				stages := make([]rollupStage, 0, n)
				var metrics []*rollupMetric
				for _, m := range all {
					if r.metrics[m.id] == m || rng.IntN(4) == 0 {
						stages, metrics = append(stages, rollupStage{}), append(metrics, m)
						r.stageKey(&stages[len(stages)-1], m, nil)
					}
				}
				for i := range stages {
					refs = append(refs, rollupRankRef{key: stages[i].sortKey, stage: uint32(i)})
				}
				got, _ := sortRollupRanks(refs, nil, new([]int), metrics)
				want := make([]uint32, len(stages))
				for i := range stages {
					want[i] = uint32(i)
				}
				slices.SortFunc(want, func(a, b uint32) int { return compareRollupMetrics(metrics[a], metrics[b]) })
				for i := range got {
					if got[i].stage != want[i] {
						t.Fatalf("round %d: canonical order differs at %d of %d", round, i, len(got))
					}
				}
				r.renumber()
				checkDense(t, r)
			}
		})
	}
}

// TestRollupPlanRankDifferential compares the dense-rank planner with a
// reference planner that never ranks (every stage sorts by the canonical
// compareRollupMetrics order), over random register, seal, idle, expiry,
// adoption and move (metric replacement) sequences, for 1 and 3 lanes.
// Names are born staggered, so unranked installed names linger below the
// renumber threshold across seals.
func TestRollupPlanRankDifferential(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{1, 3} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(workers), 7))
			const ids = 1500
			policies := make([][]byte, ids+1)
			births := make([]int, ids+1)
			for id := 1; id <= ids; id++ {
				def := "10s:3d"
				if id%3 == 0 {
					def = "10s:2d,1m:14d,30m:2y"
				}
				if id > ids/2 {
					births[id] = 5 + (id*7919)%80
				}
				p := RollupPolicy{Name: partsformat.ID(rankTestName(id)), Group: partsformat.Group([]string{"test", "other", "test2"}[id%3]), Config: sealrollup.Config{Method: sealrollup.Average}}
				for _, r := range whisper.MustParseRetentionDefs(def) {
					p.Config.Archives = append(p.Config.Archives, sealrollup.Archive{Step: int64(r.SecondsPerPoint()), Count: int64(r.NumberOfPoints())})
				}
				var err error
				if policies[id], err = EncodeRollupPolicy(p); err != nil {
					t.Fatal(err)
				}
			}
			type out struct {
				Name  string
				Tail  string
				Value string
			}
			decode := func(plan coreSealPlan) [][]out {
				var files [][]out
				for _, file := range plan.Files {
					var got []out
					for i, kv := range file {
						if i > 0 && string(file[i-1].Key) >= string(kv.Key) {
							t.Fatalf("plan keys out of order at %d", i)
						}
						got = append(got, out{string(kv.Name), string(kv.Key[len(kv.Key)-12:]), string(kv.encodedValue())})
					}
					files = append(files, got)
				}
				return files
			}
			clock := int64(1801958400)
			newPlanner := func() *coreRollups {
				r := newCoreRollups()
				r.parallelism = workers
				r.now = func() time.Time { return time.Unix(clock, 0) }
				// Expire names idle for more than 5 seals: idle stages join plans.
				r.expire = func(_ RollupPolicy, m RollupMetadata, now time.Time) bool {
					return m.LastUpdate > 0 && now.Unix()-m.LastUpdate/1e9 > 50
				}
				r.setExpiryTTL(expiryWindow(51 * time.Second))
				return r
			}
			unrank := unrankAll
			ranked, ref := newPlanner(), newPlanner()
			order := uint64(0)
			lingered, renumbered := 0, 0
			for seal := 0; seal < 100; seal++ {
				clock += 10
				switch seal {
				case 30, 60:
					// Adoption hands the installed metrics to a new planner.
					for _, p := range []**coreRollups{&ranked, &ref} {
						next := newPlanner()
						next.identities = (*p).identities
						for id, m := range (*p).metrics {
							next.install(id, m, (*p).metaOf(id))
						}
						*p = next
					}
				case 45, 75:
					// A move replaces every 7th installed metric in place.
					for _, p := range []*coreRollups{ranked, ref} {
						for id, m := range p.metrics {
							if id%7 == uint64(seal%7) {
								c := *m
								p.install(id, &c, p.metaOf(id))
							}
						}
					}
				}
				var batches []coreBatch
				for id := 1; id <= ids; id++ {
					if seal < births[id] || rng.IntN(3) == 0 {
						continue
					}
					order++
					v := float64(rng.IntN(1000)) / 4
					batches = append(batches, coreBatch{ID: uint64(id), Policy: policies[id], Order: order, Clock: clock, Points: []row{{ID: uint64(id), Time: clock - 5, Bits: math.Float64bits(v), Seq: order}}})
				}
				rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
				if len(ranked.dense) > 0 && len(ranked.unranked) > 0 {
					if len(ranked.unranked)*rankSlack < len(ranked.dense) {
						lingered++
					} else {
						renumbered++
					}
				}
				unrank(ref)
				planA, err := ranked.plan(coreSealInput{Batches: batches})
				if err != nil {
					t.Fatal(err)
				}
				planB, err := ref.plan(coreSealInput{Batches: batches})
				if err != nil {
					t.Fatal(err)
				}
				gotA, gotB := decode(planA), decode(planB)
				planA.Commit()
				planB.Commit()
				if len(gotA) == 0 || !reflect.DeepEqual(gotA, gotB) {
					t.Fatalf("seal %d: ranked and reference plans differ", seal)
				}
			}
			if lingered == 0 || renumbered == 0 {
				t.Fatalf("coverage: lingered=%d renumbered=%d", lingered, renumbered)
			}
			ranked.renumber()
			checkDense(t, ranked)
		})
	}
}
