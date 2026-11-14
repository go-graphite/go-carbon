package whisperio

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// The oracle reconstructs the whole target-wins union through RestoreArchives.
// Compare every archive and every float bit, including sparse circular aliases.
func TestMergeIntoClassicTargetMatchesRestore(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1700000000, 0) }
	for _, count := range []int{1, 2, 127, 128, 129, 341, 1000} {
		for _, step := range []int{1, 10, 60} {
			for seed := int64(0); seed < 8; seed++ {
				t.Run(fmt.Sprintf("count%d/step%d/seed%d", count, step, seed), func(t *testing.T) {
					rng := rand.New(rand.NewSource(seed))
					r := store.Retention{Step: step, Count: count}
					m := store.Metadata{MetricConfig: store.MetricConfig{Name: "target.merge", Retentions: []store.Retention{r, {Step: step * max(count, 2), Count: 5}}, AggregationMethod: store.Sum, XFilesFactor: 0.5}}
					if count == 1 {
						m.Retentions = m.Retentions[:1]
					}
					source := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(m.Retentions))}
					for ai, r := range m.Retentions {
						source.Archives[ai].Retention = r
					}
					target := source
					target.Archives = slices.Clone(source.Archives)
					values := []float64{math.Copysign(0, -1), 0, math.Inf(1), math.Inf(-1), math.SmallestNonzeroFloat64, 1.25}
					for ai, ret := range m.Retentions {
						base := int64(1000000 / ret.Step * ret.Step)
						shift := int(seed%4-1) * ret.Count / 2
						for j := 0; j < ret.Count; j++ {
							if rng.Intn(4) != 0 {
								source.Archives[ai].Points = append(source.Archives[ai].Points, store.Point{Timestamp: base + int64((j+shift)*ret.Step), Value: values[rng.Intn(len(values))]})
							}
							if rng.Intn(3) != 0 {
								target.Archives[ai].Points = append(target.Archives[ai].Points, store.Point{Timestamp: base + int64(j*ret.Step), Value: values[rng.Intn(len(values))]})
							}
						}
					}
					want := source
					want.Archives = make([]store.Archive, len(source.Archives))
					for ai, a := range source.Archives {
						byTime := map[int64]store.Point{}
						for _, p := range a.Points {
							byTime[p.Timestamp] = p
						}
						for _, p := range target.Archives[ai].Points {
							byTime[p.Timestamp] = p
						}
						bySlot := map[int64]store.Point{}
						for _, p := range byTime {
							slot := p.Timestamp / int64(a.Retention.Step) % int64(a.Retention.Count)
							if q, ok := bySlot[slot]; !ok || p.Timestamp > q.Timestamp {
								bySlot[slot] = p
							}
						}
						points := []store.Point{}
						for _, p := range bySlot {
							points = append(points, p)
						}
						slices.SortFunc(points, func(a, b store.Point) int {
							if a.Timestamp < b.Timestamp {
								return -1
							}
							if a.Timestamp > b.Timestamp {
								return 1
							}
							return 0
						})
						want.Archives[ai] = store.Archive{Retention: a.Retention, Points: points}
					}
					codec := &ContinuationCodec{}
					targetKV, oracle := mapContinuationIO{}, mapContinuationIO{}
					if err := codec.RestoreArchives(ctx, targetKV, target, clock); err != nil {
						t.Fatal(err)
					}
					if err := codec.RestoreArchives(ctx, oracle, want, clock); err != nil {
						t.Fatal(err)
					}
					// Unrelated incoming keys must disappear when the target image replaces it.
					incoming := mapContinuationIO{"incoming.only": {1, 2, 3}}
					dst := mapContinuationIO{"incoming.only": {1, 2, 3}}
					reason, classic, err := mergeIntoClassicTarget(ctx, incoming, targetKV, dst, want, source, target, clock)
					if err != nil {
						t.Fatal(err)
					}
					if !classic {
						t.Fatal("classic merge not counted")
					}
					if reason != "" {
						t.Fatalf("unexpected fallback %s", reason)
					}
					if _, ok := dst["incoming.only"]; ok {
						t.Fatal("retained incoming image")
					}
					got, err := codec.Snapshot(ctx, dst, m, clock)
					if err != nil {
						t.Fatal(err)
					}
					expected, err := codec.Snapshot(ctx, oracle, m, clock)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.EqualSnapshots(expected, got); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
