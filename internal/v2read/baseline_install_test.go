package v2read

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2convert"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

type baselineFixtures map[string]chunkstore.Snapshot

func (f baselineFixtures) Snapshot(_ context.Context, name string) (chunkstore.Snapshot, error) {
	return f[name], nil
}

type baselineClassic struct {
	name, path  string
	policy      []byte
	config      sealrollup.Config
	corrections []sealrollup.Sample
}

// Every production inventory definition runs through real classic bytes ->
// NativeRecords/CoreWriter conversion -> atomic daemon install -> original
// journal applications -> pending/sealed reader -> retained-journal replay.
func TestInstalledBaselineClassicInventory(t *testing.T) {
	const now = int64(1794700837)
	ctx := context.Background()
	directory := t.TempDir()
	source := baselineFixtures{}
	fixtures := []baselineClassic{}
	for si, def := range boundarySchemas {
		for method := sealrollup.Average; method <= sealrollup.First; method++ {
			for xi, xff := range []float32{0, .5, math.Nextafter32(.5, 1), 1} {
				name := fmt.Sprintf("fixture.%03d.%d.%d", si, method, xi)
				path := filepath.Join(directory, name+".wsp")
				clock := now
				retentions := whisper.MustParseRetentionDefs(def)
				c := sealrollup.Config{Method: method, XFilesFactor: xff}
				m := chunkstore.Metadata{ID: uint64(len(fixtures) + 100), Revision: 1, LastUpdate: time.Unix(now, 0), MetricConfig: chunkstore.MetricConfig{Name: name, AggregationMethod: chunkstore.AggregationMethod(method), XFilesFactor: xff}}
				for _, r := range retentions {
					c.Archives = append(c.Archives, sealrollup.Archive{Step: int64(r.SecondsPerPoint()), Count: int64(r.NumberOfPoints())})
					m.Retentions = append(m.Retentions, chunkstore.Retention{Step: r.SecondsPerPoint(), Count: r.NumberOfPoints()})
				}
				w, err := whisper.CreateWithOptions(path, retentions, whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
				if err != nil {
					t.Fatal(err)
				}
				candidates := map[boundarySlot]bool{}
				fixture := baselineClassic{name: name, path: path, config: c}
				for a, r := range c.Archives {
					start := boundaryAlign(now-20*r.Step, r.Step)
					count := int64(8)
					clock = now
					if a+1 < len(c.Archives) {
						coarse := c.Archives[a+1]
						start = boundaryAlign(now-r.Step*r.Count, coarse.Step)
						count = coarse.Step / r.Step / 2
						clock = now - coarse.Step
					}
					seeds := make([]*whisper.TimeSeriesPoint, count)
					for i := int64(0); i < count; i++ {
						stamp := start + i*r.Step
						seeds[i] = &whisper.TimeSeriesPoint{Time: int(stamp), Value: float64(i%7) - 3}
						for ai := range c.Archives {
							candidates[boundaryKey(ai, stamp, c)] = true
						}
					}
					if err := w.UpdateMany(seeds); err != nil {
						t.Fatal(err)
					}
					for i, bits := range []uint64{1 << 63, 0x7ff80000000000ab, 0, math.Float64bits(17)} {
						fixture.corrections = append(fixture.corrections, sealrollup.Sample{Time: start + int64(i)*r.Step, Bits: bits})
					}
					for _, delta := range []int64{-1, 0, 1} {
						fixture.corrections = append(fixture.corrections, sealrollup.Sample{Time: now - r.Step*r.Count + delta, Bits: math.Float64bits(float64(delta + 9))})
					}
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				snapshot := chunkstore.Snapshot{Metadata: m, Archives: make([]chunkstore.Archive, len(c.Archives))}
				for a := range snapshot.Archives {
					snapshot.Archives[a].Retention = m.Retentions[a]
				}
				for k, p := range boundaryClassicSlots(t, path, c, candidates) {
					snapshot.Archives[k.archive].Points = append(snapshot.Archives[k.archive].Points, chunkstore.Point{Timestamp: p.Time, Value: math.Float64frombits(p.Bits)})
				}
				source[name] = snapshot
				fixture.policy, err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: c})
				if err != nil {
					t.Fatal(err)
				}
				fixtures = append(fixtures, fixture)
			}
		}
	}
	plan := v2convert.NativeCheckpointPlan{Version: 1, Now: now, Baseline: v2convert.NativeBaseline{Source: v2part.Provenance{Incarnation: v2part.PartID{7}, Stream: "fenced-classic-inventory"}, Group: "local", Part: v2part.PartID{8}, Order: 100}}
	sum := sha256.Sum256([]byte("immutable-classic-inventory-fixture"))
	plan.Files = []v2convert.File{{Name: "CHUNKSTORE", SHA256: hex.EncodeToString(sum[:]), Bytes: 1}, {Name: "CURRENT", SHA256: hex.EncodeToString(sum[:]), Bytes: 1}}
	for _, fixture := range fixtures {
		plan.Names = append(plan.Names, fixture.name)
	}
	var err error
	plan.Baseline.Snapshot, err = plan.InventoryDigest()
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(directory, "converted")
	if _, err := v2convert.ConvertNativeCheckpoint(ctx, source, plan, output, 256<<20, 4); err != nil {
		t.Fatal(err)
	}
	receipts, err := filepath.Glob(filepath.Join(output, "*", "receipt.json"))
	if err != nil || len(receipts) != 1 {
		t.Fatal(receipts, err)
	}
	root := filepath.Join(directory, "runtime")
	cfg := v2daemon.Config{Layer: 3, Producers: 3, Shards: 2, GroupInterval: time.Hour, BaselinePath: filepath.Dir(receipts[0])}
	var store *v2daemon.Store
	var reader *Reader
	open := func() {
		t.Helper()
		reader, err = New(2, 512<<20, 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		reader.Now = func() time.Time { return time.Unix(now, 0) }
		cfg.Hooks = v2core.Hooks{PublishBatches: reader.Publish}
		store, err = v2daemon.Open(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	closeStore := func() {
		t.Helper()
		reader.Close()
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store = nil
	}
	open()
	defer func() {
		if store != nil {
			reader.Close()
			_ = store.Close()
		}
	}()
	// Conversion inputs are not a recovery dependency after atomic installation.
	cfg.BaselinePath = filepath.Join(directory, "source-no-longer-available")
	compare := func(stage string) {
		t.Helper()
		for _, fixture := range fixtures {
			baseline, err := reader.BaselineMetadata(ctx, fixture.name)
			if err != nil || baseline.BaselineOrder != 100 {
				t.Fatalf("%s baseline %s: %+v %v", stage, fixture.name, baseline, err)
			}
			w, err := whisper.OpenWithOptions(fixture.path, &whisper.Options{Now: func() time.Time { return time.Unix(now, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range fixture.config.Archives {
				edge := now - a.Step*a.Count
				for _, delta := range []int64{-1, 0, 1} {
					from := edge + delta
					until := from + 2*a.Step + 1
					want, err := w.Fetch(int(from), int(until))
					if err != nil {
						t.Fatal(err)
					}
					got, masked, err := reader.FetchWithMask(ctx, fixture.name, int(from), int(until))
					if err != nil {
						t.Fatalf("%s %s: %v", stage, fixture.name, err)
					}
					if !masked || (got == nil) != (want == nil) {
						t.Fatalf("%s %s baseline mask or nil mismatch", stage, fixture.name)
					}
					if got != nil {
						if got.FromTime() != want.FromTime() || got.UntilTime() != want.UntilTime() || got.Step() != want.Step() || len(got.Values()) != len(want.Values()) {
							t.Fatalf("%s %s archive shape mismatch", stage, fixture.name)
						}
						for i, v := range want.Values() {
							if math.Float64bits(got.Values()[i]) != math.Float64bits(v) {
								t.Fatalf("%s %s archive%d range%d slot%d got%016x want%016x", stage, fixture.name, a.Step, from, i, math.Float64bits(got.Values()[i]), math.Float64bits(v))
							}
						}
					}
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	compare("installed")
	for round := 0; round < 2; round++ {
		for fi, fixture := range fixtures {
			samples := fixture.corrections
			if round == 1 {
				samples = []sealrollup.Sample{{Time: samples[0].Time, Bits: math.Float64bits(23)}}
			}
			p := store.Producers[v2recv.OwnerFor(fixture.name, 3)]
			app := v2daemon.Application{Name: fixture.name, Policy: fixture.policy, Clock: now, Points: make([]points.Point, len(samples)), Sequence: make([]uint64, len(samples))}
			// Orders are producer-local; each fixture has exactly one application per round.
			app.Order = 100 + uint64(round*len(fixtures)) + uint64(fi) + 1
			classicPoints := make([]*whisper.TimeSeriesPoint, len(samples))
			for i, s := range samples {
				app.Points[i] = points.Point{Timestamp: s.Time, Value: math.Float64frombits(s.Bits)}
				app.Sequence[i] = uint64(round*10000 + i + 1)
				classicPoints[i] = &whisper.TimeSeriesPoint{Time: int(s.Time), Value: app.Points[i].Value}
			}
			if err := p.AppendApplications([]v2daemon.Application{app}); err != nil {
				t.Fatal(err)
			}
			w, err := whisper.OpenWithOptions(fixture.path, &whisper.Options{Now: func() time.Time { return time.Unix(now, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.UpdateMany(classicPoints); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for _, p := range store.Producers {
			if err := p.Drain(); err != nil {
				t.Fatal(err)
			}
		}
		// Observe every submitted application through ordinary pending publications.
		await(t, func() bool {
			n := 0
			for i := range reader.owners {
				p := reader.owners[i].Load()
				if p != nil {
					n += len(p.pending)
				}
			}
			return n == len(fixtures)
		})
		compare(fmt.Sprintf("pending%d", round))
		closeStore()
		open()
		compare(fmt.Sprintf("replayed%d", round))
	}
	closeStore()
	t.Logf("converted/installed/admitted/replayed %d classic policies", len(fixtures))
}
