package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Native copying must preserve independently authoritative archive values for
// every aggregation policy, rather than recomputing coarse values from fine.
func TestNativeWindowHistoricalRetentionDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC) }
	const day = 86400
	schemas := [][]store.Retention{
		{{Step: 60, Count: 30 * 1440}, {Step: 3600, Count: 365 * 24}, {Step: day, Count: 3650}},
		{{Step: 60, Count: 30 * 1440}, {Step: 3600, Count: 5 * 365 * 24}}, // shipped storage-schemas.conf
		{{Step: 60, Count: 14 * 1440}, {Step: 3600, Count: 2 * 365 * 24}},
		{{Step: 1, Count: 2 * day}, {Step: 60, Count: 28 * 1440}, {Step: 3600, Count: 2 * 365 * 24}},
		{{Step: 60, Count: 14 * 1440}, {Step: 300, Count: 60 * 288}, {Step: 3600, Count: 2 * 365 * 24}},
		{{Step: day, Count: 3650}},
	}
	windows := []store.NativeWindow{
		{From: time.Date(2024, 6, 23, 0, 0, 0, 0, time.UTC).Unix(), Until: time.Date(2024, 6, 25, 0, 0, 0, 0, time.UTC).Unix()},
		{From: clock().Unix() - 400*day, Until: clock().Unix()},
		{From: clock().Unix() - 10*day, Until: clock().Unix()},
	}
	for _, continuation := range []bool{false, true} {
		for si, schema := range schemas {
			for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.First, store.Last, store.Min, store.Max} {
				for _, xff := range []float32{0, .5, 1} {
					t.Run(fmt.Sprintf("continuation=%v/schema=%d/method=%d/xff=%g", continuation, si, method, xff), func(t *testing.T) {
						source := windowStore(t, continuation, clock)
						cfg := store.MetricConfig{Name: "historical", Retentions: schema, AggregationMethod: method, XFilesFactor: xff}
						snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}}
						for ai, r := range schema {
							a := store.Archive{Retention: r}
							// Build sparse, non-aliased cells throughout this archive's coverage.
							seen := map[int64]bool{}
							for _, ts := range []int64{clock().Unix() - int64(r.Step*r.Count) + int64(r.Step), windows[0].From, windows[0].From + day, windows[1].From, windows[2].From, clock().Unix() - day, clock().Unix()} {
								ts = ts / int64(r.Step) * int64(r.Step)
								if ts <= clock().Unix()-int64(r.Step*r.Count) || seen[ts] {
									continue
								}
								seen[ts] = true
								v := float64(ai*100 + len(a.Points) + 1)
								if len(a.Points)%2 == 0 {
									v = math.Copysign(0, -1)
								}
								a.Points = append(a.Points, store.Point{Timestamp: ts, Value: v})
							}
							snap.Archives = append(snap.Archives, a)
						}
						seedWindowSnapshot(t, source, snap, continuation, clock)
						raw, err := source.Snapshot(ctx, cfg.Name)
						if err != nil {
							t.Fatal(err)
						}
						pin, err := source.PinNativeSource()
						if err != nil {
							t.Fatal(err)
						}
						defer pin.Close()
						for wi, w := range windows {
							t.Run(fmt.Sprint(wi), func(t *testing.T) {
								target := windowStore(t, continuation, clock)
								right := store.Snapshot{Metadata: raw.Metadata}
								for _, a := range raw.Archives {
									r := store.Archive{Retention: a.Retention}
									for i, p := range a.Points {
										if i%2 == 0 {
											p.Value = 42
											r.Points = append(r.Points, p)
										}
									}
									right.Archives = append(right.Archives, r)
								}
								seedWindowSnapshot(t, target, right, continuation, clock)
								before, err := target.Snapshot(ctx, cfg.Name)
								if err != nil {
									t.Fatal(err)
								}
								var wire bytes.Buffer
								if err := pin.WriteWindow(ctx, []string{cfg.Name}, &wire, &w); err != nil {
									t.Fatal(err)
								}
								receipt, err := target.NativeImport(ctx, windowRange([]string{cfg.Name}, &w), &wire)
								if err != nil || receipt.CopiedCount != 1 {
									t.Fatal(receipt, err)
								}
								got, err := target.Snapshot(ctx, cfg.Name)
								if err != nil {
									t.Fatal(err)
								}
								for ai, a := range raw.Archives {
									want := map[int64]uint64{}
									for _, p := range a.Points {
										if w.Contains(p.Timestamp) {
											want[p.Timestamp] = math.Float64bits(p.Value)
										}
									}
									for _, p := range before.Archives[ai].Points {
										want[p.Timestamp] = math.Float64bits(p.Value)
									}
									assertPointBits(t, want, pointBits(got.Archives[ai].Points, nil, true))
								}
							})
						}
					})
				}
			}
		}
	}
}
