package v2read

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
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
	t.Parallel()
	// Keep the complete schema × method × xFilesFactor matrix, but install
	// bounded inventories. Each fetch opens and validates the installed part;
	// one descriptor containing the whole matrix makes this oracle quadratic.
	const schemasPerInventory = 4
	inventories := (len(boundarySchemas) + schemasPerInventory - 1) / schemasPerInventory
	for first := 0; first < inventories; first++ {
		t.Run(fmt.Sprintf("inventory=%d", first), func(t *testing.T) {
			t.Parallel()
			// Advance publication/seal timers when the owners are idle; keep
			// all real file, conversion, install and replay operations. Each
			// bubble runs in its own process: Pebble's pooled sstable write
			// tasks must not cross into parallel inventories or other tests.
			pebbleSynctest(t, func(t *testing.T) {
				checkInstalledBaselineClassicInventory(t, first, inventories)
			})
		})
	}
}

func checkInstalledBaselineClassicInventory(t *testing.T, first, stride int) {
	const now = int64(1794700837)
	ctx := context.Background()
	directory := t.TempDir()
	source := baselineFixtures{}
	fixtures := []baselineClassic{}
	// Stripe definitions so neighboring wide-fan-in schemas do not all
	// inflate the same descriptor and pending head.
	for si := first; si < len(boundarySchemas); si += stride {
		def := boundarySchemas[si]
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
	cfg := v2daemon.Config{Layer: 3, Producers: 3, Shards: 2, MaxCoreRows: 8192, GroupInterval: time.Hour, BaselinePath: filepath.Dir(receipts[0])}
	var store *v2daemon.Store
	var reader *Reader
	var pendingObserved atomic.Bool
	open := func() {
		t.Helper()
		reader, err = New(2, 512<<20, 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		reader.Now = func() time.Time { return time.Unix(now, 0) }
		cfg.Hooks = v2core.Hooks{PublishBatches: func(owner int, snapshot *pebble.Snapshot, pending []v2core.Batch, number uint64) error {
			if len(pending) > 0 {
				pendingObserved.Store(true)
			}
			return reader.Publish(owner, snapshot, pending, number)
		}}
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
			if err != nil || baseline.BaselineOrder != 1 {
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
		applications := make([][]v2daemon.Application, len(store.Producers))
		for fi, fixture := range fixtures {
			samples := fixture.corrections
			if round == 1 {
				samples = []sealrollup.Sample{{Time: samples[0].Time, Bits: math.Float64bits(23)}}
			}
			producer := v2recv.OwnerFor(fixture.name, len(store.Producers))
			app := v2daemon.Application{Name: fixture.name, Policy: fixture.policy, Clock: now, Points: make([]points.Point, len(samples)), Sequence: make([]uint64, len(samples))}
			// Orders are producer-local; each fixture has exactly one application per round.
			app.Order = 100 + uint64(round*len(fixtures)) + uint64(fi) + 1
			classicPoints := make([]*whisper.TimeSeriesPoint, len(samples))
			for i, s := range samples {
				app.Points[i] = points.Point{Timestamp: s.Time, Value: math.Float64frombits(s.Bits)}
				app.Sequence[i] = uint64(round*10000 + i + 1)
				classicPoints[i] = &whisper.TimeSeriesPoint{Time: int(s.Time), Value: app.Points[i].Value}
			}
			applications[producer] = append(applications[producer], app)
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
		// Group admission retains each original application/order, without
		// publishing a growing pending tail once per fixture.
		for producer, p := range store.Producers {
			if len(applications[producer]) == 0 {
				continue
			}
			if err := p.AppendApplications(applications[producer]); err != nil {
				t.Fatal(err)
			}
			if err := p.Drain(); err != nil {
				t.Fatal(err)
			}
		}
		// The one-second seal can run while this large inventory is admitted.
		// Observe every order in the published pending tail or sealed frontier;
		// requiring all applications to remain pending at once races that seal.
		await(t, func() bool {
			visible := map[uint64]bool{}
			first := 101 + uint64(round*len(fixtures))
			last := first + uint64(len(fixtures))
			add := func(order uint64) {
				if order >= first && order < last {
					visible[order] = true
				}
			}
			for i := range reader.owners {
				g := reader.owners[i].Load()
				if g != nil && g.retain() {
					g.load()
					if g.err != nil {
						g.release()
						return false
					}
					g.sealed.Metadata(func(_ string, _ v2core.RollupPolicy, m v2core.RollupMetadata) { add(m.Frontier.Order) })
					g.view.Each(func(a *v2core.PendingApplication) bool {
						add(a.Order)
						return true
					})
					g.release()
				}
			}
			return len(visible) == len(fixtures)
		})
		compare(fmt.Sprintf("pending%d", round))
		closeStore()
		open()
		compare(fmt.Sprintf("replayed%d", round))
	}
	closeStore()
	if !pendingObserved.Load() {
		t.Fatal("inventory never exercised a pending publication")
	}
	t.Logf("converted/installed/admitted/replayed %d classic policies", len(fixtures))
}
