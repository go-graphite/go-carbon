package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/receiver/parse"
)

func TestNonFiniteFillImportPaths(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000000, 0) }
	for _, layout := range []string{"exact", "classic", "compressed"} {
		for _, path := range []string{"transfer", "native", "window"} {
			t.Run(layout+"/"+path, func(t *testing.T) {
				continuation := layout != "exact"
				source, target := windowStore(t, continuation, clock), windowStore(t, continuation, clock)
				config := store.MetricConfig{Name: "fill.nonfinite", Retentions: []store.Retention{{Step: 10, Count: 600}, {Step: 60, Count: 1000}}, AggregationMethod: store.Average}
				donor := []float64{286.73400000000004, 989.205, 32, 42, math.Inf(1), math.Inf(1), math.NaN(), 7, 99}
				survivor := []float64{math.Inf(1), math.Inf(-1), 12, math.Copysign(0, -1), math.Inf(-1), 32, math.Inf(1), math.NaN(), math.Inf(1)}
				want := []float64{donor[0], donor[1], 12, survivor[3], survivor[4], 32, math.Inf(1), 7, 99}
				for side, db := range []*store.Store{source, target} {
					snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: config}, Archives: make([]store.Archive, len(config.Retentions))}
					for ai, r := range config.Retentions {
						snap.Archives[ai].Retention = r
						for i, v := range [][]float64{donor, survivor}[side] {
							snap.Archives[ai].Points = append(snap.Archives[ai].Points, store.Point{Timestamp: 999000 + int64(i*60), Value: v})
						}
					}
					if layout == "compressed" {
						if _, err := db.Create(ctx, config); err != nil {
							t.Fatal(err)
						}
						for _, a := range snap.Archives {
							if err := db.UpdateManyForArchive(ctx, config.Name, a.Points, a.Retention.MaxRetention()); err != nil {
								t.Fatal(err)
							}
						}
					} else {
						seedWindowSnapshot(t, db, snap, continuation, clock)
					}
				}
				if path == "transfer" {
					records, err := source.ExportTransfer(ctx, []string{config.Name})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := target.ImportTransfer(ctx, records); err != nil {
						t.Fatal(err)
					}
				} else {
					pin, err := source.PinNativeSource()
					if err != nil {
						t.Fatal(err)
					}
					defer pin.Close()
					request := windowRange([]string{config.Name}, nil)
					var wire bytes.Buffer
					if path == "window" {
						request.Window = &store.NativeWindow{From: 999000, Until: 999480}
						want[8] = math.Inf(1) // outside the requested repair window
					}
					if request.Window == nil {
						err = pin.Write(ctx, request.Names, &wire)
					} else {
						err = pin.WriteWindow(ctx, request.Names, &wire, request.Window)
					}
					if err != nil {
						t.Fatal(err)
					}
					if _, err := target.NativeImport(ctx, request, &wire); err != nil {
						t.Fatal(err)
					}
				}
				got, err := target.Snapshot(ctx, config.Name)
				if err != nil {
					t.Fatal(err)
				}
				for ai, a := range got.Archives {
					byTime := map[int64]uint64{}
					for _, p := range a.Points {
						byTime[p.Timestamp] = math.Float64bits(p.Value)
					}
					for i, v := range want {
						ts := int64(999000 + i*60)
						if bits, ok := byTime[ts]; !ok || bits != math.Float64bits(v) {
							t.Fatalf("archive%d ts%d: %016x (present %v), want %016x", ai, ts, bits, ok, math.Float64bits(v))
						}
					}
				}
			})
		}
	}
}

// Intake accepts Inf. A single Inf contaminates an average rollup; ordinary
// bounded finite input does not create Inf. This exercises the
// continuation writer's Close/flush as well as the ordinary chunk writer.
func TestClientInfinityRollup(t *testing.T) {
	ctx := context.Background()
	for _, continuation := range []bool{false, true} {
		for _, value := range []string{"32", "+Inf", "-Inf"} {
			t.Run(fmt.Sprintf("continuation=%v/value=%s", continuation, value), func(t *testing.T) {
				clock := func() time.Time { return time.Unix(1000000, 0) }
				db := windowStore(t, continuation, clock)
				config := store.MetricConfig{Name: "client.infinity", Retentions: []store.Retention{{Step: 10, Count: 600}, {Step: 60, Count: 1000}}, AggregationMethod: store.Average}
				if _, err := db.Create(ctx, config); err != nil {
					t.Fatal(err)
				}
				var input []store.Point
				for i := 0; i < 18; i++ {
					v := "32"
					if i == 0 {
						v = value
					}
					name, parsed, ts, err := parse.PlainLine([]byte(fmt.Sprintf("%s %s %d\n", config.Name, v, 999000+i*10)))
					if err != nil || string(name) != config.Name {
						t.Fatal(err)
					}
					input = append(input, store.Point{Timestamp: ts, Value: parsed})
				}
				if err := db.UpdateMany(ctx, config.Name, input); err != nil {
					t.Fatal(err)
				}
				snap, err := db.Snapshot(ctx, config.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := input[0].Value
				for ai, a := range snap.Archives {
					found := false
					for _, p := range a.Points {
						if p.Timestamp == 999000 {
							found = true
							if math.Float64bits(p.Value) != math.Float64bits(want) {
								t.Fatalf("archive%d: got %v want %v", ai, p.Value, want)
							}
						}
					}
					if !found {
						t.Fatalf("archive%d missing rollup", ai)
					}
				}
				// A later live write remains authoritative, including a genuine Inf.
				if err := db.UpdateMany(ctx, config.Name, []store.Point{{Timestamp: 999170, Value: math.Inf(1)}}); err != nil {
					t.Fatal(err)
				}
				if err := db.Flush(); err != nil {
					t.Fatal(err)
				}
				if err := db.Compact(); err != nil {
					t.Fatal(err)
				}
				series, err := db.Fetch(ctx, config.Name, 999169, 999171)
				if err != nil {
					t.Fatal(err)
				}
				if len(series.Values) != 1 || !math.IsInf(series.Values[0], 1) {
					t.Fatal("live Inf lost", series)
				}
			})
		}
	}
}
