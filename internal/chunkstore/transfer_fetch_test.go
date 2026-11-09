package chunkstore_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Snapshot verification scans physical slots. Real reads also require the
// classic archive's first slot to hold the timestamp anchoring its ring.
func TestTransferMergedSparseArchivesRemainFetchable(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("buckets=%t/cached=%t", buckets, cached), func(t *testing.T) {
				ctx := context.Background()
				now := time.Unix(1000080, 0)
				source := transferStoreCachedAt(t, true, buckets, 0, now)
				var budget int64
				if cached {
					budget = 8 << 20
				}
				target := transferStoreCachedAt(t, true, buckets, budget, now)
				config := store.MetricConfig{Name: "transfer.sparse.fetch", Retentions: []store.Retention{{Step: 10, Count: 60}, {Step: 60, Count: 60}}, AggregationMethod: store.Sum}
				for _, db := range []*store.Store{source, target} {
					if _, err := db.Create(ctx, config); err != nil {
						t.Fatal(err)
					}
				}
				for _, input := range []struct {
					points []store.Point
					age    int
				}{
					{[]store.Point{{Timestamp: 999780, Value: 3}, {Timestamp: 999990, Value: 0}}, 600},
					{[]store.Point{{Timestamp: 999000, Value: 41}, {Timestamp: 999600, Value: 0}, {Timestamp: 999960, Value: 29}}, 3600},
				} {
					if err := source.UpdateManyForArchive(ctx, config.Name, input.points, input.age); err != nil {
						t.Fatal(err)
					}
				}
				before, err := source.Snapshot(ctx, config.Name)
				if err != nil {
					t.Fatal(err)
				}
				if cached {
					if _, err := target.Fetch(ctx, config.Name, 999500, 1000079); err != nil {
						t.Fatal(err)
					}
				}
				records, err := source.ExportTransfer(ctx, []string{config.Name})
				if err != nil {
					t.Fatal(err)
				}
				proofs, err := target.ImportTransfer(ctx, records)
				if err != nil {
					t.Fatal(err)
				}
				if len(proofs) != 1 || !proofs[0].Merged {
					t.Fatal("fixture must exercise overlapping transfer", proofs)
				}
				if err := target.VerifyTransfer(ctx, proofs); err != nil {
					t.Fatal(err)
				}
				after, err := target.Snapshot(ctx, config.Name)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(before, after); err != nil {
					t.Fatal(err)
				}
				check := func() {
					t.Helper()
					for _, archive := range before.Archives {
						retention := archive.Retention
						from := int(now.Unix()) - retention.Step*retention.Count + 2*retention.Step
						got, err := target.Fetch(ctx, config.Name, from, int(now.Unix())-1)
						if err != nil {
							t.Fatal(err)
						}
						if got == nil || got.Step != retention.Step {
							t.Fatalf("wrong archive: %+v", got)
						}
						for _, point := range archive.Points {
							i := (int(point.Timestamp) - got.FromTime) / got.Step
							if i < 0 || i >= len(got.Values) || math.IsNaN(got.Values[i]) || got.Values[i] != point.Value {
								t.Fatalf("step=%d retained point %+v missing from Fetch", retention.Step, point)
							}
						}
					}
				}
				check()
				// The next foreground append must retain the restored ring's alignment.
				if err := target.UpdateManyForArchive(ctx, config.Name, []store.Point{{Timestamp: 1000050, Value: 71}}, 600); err != nil {
					t.Fatal(err)
				}
				check()
			})
		}
	}
}
