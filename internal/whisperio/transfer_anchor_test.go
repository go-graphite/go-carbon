package whisperio

import (
	"context"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestRestoreArchiveAnchorExactImage(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000080, 0) }
	r := []store.Retention{{Step: 10, Count: 6}, {Step: 60, Count: 10}, {Step: 600, Count: 10}}
	// More than one ring of timestamp span, no physical aliases, unordered input,
	// distinct archive grids, an empty archive, and every exceptional float bit.
	fine := []store.Point{{Timestamp: 1000070, Value: math.Inf(1)}, {Timestamp: 999900, Value: math.Copysign(0, -1)}, {Timestamp: 1000060, Value: math.Float64frombits(0x7ff8000000000042)}}
	coarse := []store.Point{{Timestamp: 999960, Value: math.Inf(-1)}, {Timestamp: 999480, Value: 0}}
	want := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "anchor", Retentions: r, AggregationMethod: store.Sum}}, Archives: []store.Archive{{Retention: r[0], Points: fine}, {Retention: r[1], Points: coarse}, {Retention: r[2]}}}
	kv := mapContinuationIO{}
	codec := &ContinuationCodec{}
	if err := codec.RestoreArchives(ctx, kv, want, clock); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Snapshot(ctx, kv, want.Metadata, clock)
	if err != nil {
		t.Fatal(err)
	}
	for i := range want.Archives {
		slices.SortFunc(want.Archives[i].Points, func(a, b store.Point) int {
			if a.Timestamp < b.Timestamp {
				return -1
			}
			if a.Timestamp > b.Timestamp {
				return 1
			}
			return 0
		})
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	series, err := codec.Fetch(ctx, kv, want.Metadata, 1000020, 1000079, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range fine {
		if p.Timestamp < int64(series.FromTime) {
			continue
		}
		i := (int(p.Timestamp) - series.FromTime) / series.Step
		if i >= len(series.Values) || math.Float64bits(series.Values[i]) != math.Float64bits(p.Value) {
			t.Fatalf("fetch changed %+v: %+v", p, series)
		}
	}
}

func TestRestoreArchiveRejectsUnrepresentableImages(t *testing.T) {
	r := store.Retention{Step: 10, Count: 6}
	for _, points := range [][]store.Point{
		{{Timestamp: 999901, Value: 1}},
		{{Timestamp: 999900, Value: 1}, {Timestamp: 999960, Value: 2}},
		{{Timestamp: 0, Value: 1}},
		{{Timestamp: 1 << 32, Value: 1}},
	} {
		kv := mapContinuationIO{}
		snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "anchor", Retentions: []store.Retention{r}, AggregationMethod: store.Sum}}, Archives: []store.Archive{{Retention: r, Points: points}}}
		if err := (&ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, time.Now); err == nil {
			t.Fatalf("accepted %+v", points)
		}
		if len(kv) != 0 {
			t.Fatal("invalid restore wrote keys")
		}
	}
}

func TestRestoreAnchorFromColdRepresentations(t *testing.T) {
	for _, floating := range []bool{false, true} {
		t.Run(map[bool]string{false: "v9", true: "v11"}[floating], func(t *testing.T) {
			ctx := context.Background()
			options := coldOptions(&ContinuationCodec{})
			source, err := store.Open(t.TempDir(), options)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			source.StopBackground()
			var want store.Snapshot
			if !floating {
				want = coldFixture(t, source)
			} else {
				cfg := store.MetricConfig{Name: "float.metric", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
				if _, err := source.Create(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				random := rand.New(rand.NewPCG(42, 17))
				points := make([]store.Point, 1800)
				for i := range points {
					points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: float64(random.IntN(16)) / 3}
				}
				if err := source.UpdateMany(ctx, cfg.Name, points); err != nil {
					t.Fatal(err)
				}
				want, err = source.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
			}
			floatSweep(t, source, "exact", false)
			if floating {
				floatSweep(t, source, "float-repack", true)
			}
			expectedFormat := 9
			if floating {
				expectedFormat = 11
			}
			if source.TransferFormat() != expectedFormat {
				t.Fatalf("source format %d", source.TransferFormat())
			}
			records, err := source.ExportTransfer(ctx, []string{want.Metadata.Name})
			if err != nil {
				t.Fatal(err)
			}
			for _, cached := range []bool{false, true} {
				targetOptions := options
				if !cached {
					targetOptions.MetadataCacheSize = 0
				}
				target, err := store.Open(t.TempDir(), targetOptions)
				if err != nil {
					t.Fatal(err)
				}
				defer target.Close()
				target.StopBackground()
				if err := target.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", FloatCodec: floating}); err != nil {
					t.Fatal(err)
				}
				if _, err := target.Create(ctx, want.Metadata.MetricConfig); err != nil {
					t.Fatal(err)
				}
				proofs, err := target.ImportTransfer(ctx, records)
				if err != nil {
					t.Fatal(err)
				}
				if len(proofs) != 1 || !proofs[0].Merged {
					t.Fatal("did not restore overlap")
				}
				got, err := target.Snapshot(ctx, want.Metadata.Name)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(want, got); err != nil {
					t.Fatal(err)
				}
				now := int(options.Now().Unix())
				for _, a := range got.Archives {
					series, err := target.Fetch(ctx, want.Metadata.Name, now-a.Retention.Step*a.Retention.Count+2*a.Retention.Step, now-1)
					if err != nil {
						t.Fatal(err)
					}
					if series.Step != a.Retention.Step {
						t.Fatalf("selected wrong archive: %d", series.Step)
					}
					for _, p := range a.Points {
						if int(p.Timestamp) < series.FromTime || int(p.Timestamp) >= series.UntilTime {
							continue
						}
						index := (int(p.Timestamp) - series.FromTime) / series.Step
						if math.Float64bits(series.Values[index]) != math.Float64bits(p.Value) {
							t.Fatalf("cold source point missing %+v", p)
						}
					}
				}
			}
		})
	}
}
