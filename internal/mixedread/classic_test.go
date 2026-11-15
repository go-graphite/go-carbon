package mixedread

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestClassicCutoverDifferential(t *testing.T) {
	queries, slots := 0, 0
	for si, def := range schemas {
		for method := sr.Average; method <= sr.First; method++ {
			t.Run(fmt.Sprintf("schema%d-method%d", si, method), func(t *testing.T) {
				ctx := context.Background()
				const clock int64 = 1794700800
				c := sr.Config{Method: method, XFilesFactor: .5}
				cfg := chunkstore.MetricConfig{Name: "cutover.metric", AggregationMethod: chunkstore.AggregationMethod(method), XFilesFactor: .5}
				metric := partsread.Metric{ID: 42, AggregationMethod: cfg.AggregationMethod.String(), XFilesFactor: .5}
				rets := whisper.MustParseRetentionDefs(def)
				for _, r := range rets {
					a := sr.Archive{Step: int64(r.SecondsPerPoint()), Count: int64(r.NumberOfPoints())}
					c.Archives = append(c.Archives, a)
					cfg.Retentions = append(cfg.Retentions, chunkstore.Retention{Step: int(a.Step), Count: int(a.Count)})
					metric.Archives = append(metric.Archives, partsread.Archive{Step: a.Step, Points: a.Count})
				}
				// The legacy source is actual continuation storage, frozen exactly at the
				// batch frontier. A second legacy store receives the complete input.
				legacyFS := vfs.NewMem()
				opts := chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, Now: func() time.Time { return time.Unix(clock, 0) }}
				legacy, err := chunkstore.Open("legacy", opts.WithFS(legacyFS))
				if err != nil {
					t.Fatal(err)
				}
				full, err := chunkstore.Open("full", opts.WithFS(vfs.NewMem()))
				if err != nil {
					t.Fatal(err)
				}
				defer full.Close()
				for _, s := range []*chunkstore.Store{legacy, full} {
					if _, err = s.Create(ctx, cfg); err != nil {
						t.Fatal(err)
					}
				}
				w, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "classic.wsp"), rets, whisper.AggregationMethod(method), .5, &whisper.Options{Sparse: true, Now: opts.Now})
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				step := c.Archives[0].Step
				start := (clock - 240*step) / step * step
				var before, after, late, edges []sr.Sample
				for i := int64(0); i < 180; i++ {
					p := sr.Sample{Time: start + i*step, Bits: math.Float64bits(float64(i%19) - 9), Sequence: uint64(i + 1)}
					if i < 119 {
						before = append(before, p)
					} else {
						after = append(after, p)
					}
				}
				for i, b := range []uint64{0x8000000000000000, 0x7ff8000000000042, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000000} {
					late = append(late, sr.Sample{Time: start + int64(i+1)*step, Bits: b, Sequence: uint64(1000 + i)})
				}
				late = append(late, sr.Sample{Time: start + 3*step, Bits: math.Float64bits(-13), Sequence: 2000})
				for _, a := range c.Archives {
					for _, d := range []int64{-1, 0, 1} {
						edges = append(edges, sr.Sample{Time: clock - a.Step*a.Count + d, Bits: math.Float64bits(float64(d + 4)), Sequence: uint64(3000 + len(edges))})
					}
				}
				state := map[[2]int64]sr.Sample{}
				var post []partsread.Record
				for bi, input := range [][]sr.Sample{before, after, late, edges} {
					ds, e := sr.Reduce(c, func(a int, slot int64) (sr.Sample, bool) { p, ok := state[[2]int64{int64(a), slot}]; return p, ok }, []sr.Batch{{Order: uint64(bi + 1), Now: clock, Points: input}})
					if e != nil {
						t.Fatal(e)
					}
					for _, d := range ds {
						state[[2]int64{int64(d.Archive), d.Slot}] = d.Sample
						if bi > 0 {
							post = append(post, partsread.Record{ID: 42, Step: c.Archives[d.Archive].Step, Sample: partsread.Sample{Time: d.Sample.Time, Bits: d.Sample.Bits, Sequence: d.Order}})
						}
					}
					var ps []chunkstore.Point
					var ptrs []*whisper.TimeSeriesPoint
					for _, p := range input {
						v := math.Float64frombits(p.Bits)
						ps = append(ps, chunkstore.Point{Timestamp: p.Time, Value: v})
						ptrs = append(ptrs, &whisper.TimeSeriesPoint{Time: int(p.Time), Value: v})
					}
					if e = w.UpdateMany(ptrs); e != nil {
						t.Fatal(e)
					}
					if e = full.UpdateMany(ctx, cfg.Name, ps); e != nil {
						t.Fatal(e)
					}
					if bi == 0 {
						if e = legacy.UpdateMany(ctx, cfg.Name, ps); e != nil {
							t.Fatal(e)
						}
					}
				}
				if err = legacy.Close(); err != nil {
					t.Fatal(err)
				}
				opts.ReadOnly = true
				legacy, err = chunkstore.Open("legacy", opts.WithFS(legacyFS))
				if err != nil {
					t.Fatal(err)
				}
				defer legacy.Close()
				cat := partsread.CatalogFuncs{LookupFunc: func(name string) (partsread.Metric, bool) { return metric, name == cfg.Name }, FindFunc: func(ctx context.Context, _ string, y func(string) error) error { return y(cfg.Name) }}
				var complete []partsread.Record
				for k, p := range state {
					complete = append(complete, partsread.Record{ID: 42, Step: c.Archives[k[0]].Step, Sample: partsread.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}})
				}
				var pub, whole partsread.Publication
				pub.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{partsread.Freeze(post)}})
				defer pub.Close()
				whole.Publish(partsread.View{Catalog: cat, Blocks: []partsread.Source{partsread.Freeze(complete)}})
				defer whole.Close()
				adapter := partsread.Adapter{Select: func(string) *partsread.Publication { return &pub }, Catalog: &pub, Budget: partsread.NewBudget(256 << 20), WorkBytes: 128 << 20, Now: opts.Now}
				mixed, e := New(Options{Legacy: legacy, LegacyCatalog: cat, Parts: adapter})
				if e != nil {
					t.Fatal(e)
				}
				frozen, e := legacy.ResolveFrozen(ctx, cfg.Name)
				if e != nil {
					t.Fatal(e)
				}
				binding, e := BindLegacy(frozen, metric)
				if e != nil {
					t.Fatal(e)
				}
				var boundPub partsread.Publication
				boundPub.Publish(partsread.View{Catalog: entryCatalog{Catalog: cat, entries: map[string]catalogEntry{cfg.Name: {metric: metric, legacy: binding}}}, Parts: []partsread.Source{partsread.Freeze(post)}})
				boundSnap, e := boundPub.Snapshot()
				if e != nil {
					t.Fatal(e)
				}
				defer boundSnap.Close()
				boundPub.Close()
				bound, e := New(Options{Legacy: legacy, LegacyCatalog: cat, Parts: adapter, UnifiedCatalog: true, Snapshot: boundSnap})
				if e != nil {
					t.Fatal(e)
				}
				for _, width := range []int64{3600, 86400, 7 * 86400, 30 * 86400} {
					from := clock - width
					want, e := w.Fetch(int(from), int(clock))
					if e != nil {
						t.Fatal(e)
					}
					var shape *partsread.Series
					check := func(label string, got *partsread.Series) {
						t.Helper()
						if (got == nil) != (want == nil) {
							t.Fatalf("%s nil mismatch", label)
						}
						if got == nil {
							return
						}
						if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
							t.Fatalf("%s width%d shape", label, width)
						}
						for i, v := range want.Values() {
							if math.Float64bits(got.Values[i]) != math.Float64bits(v) {
								t.Fatalf("%s width%d slot%d got%x want%x", label, width, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
							}
						}
						shape = got
					}
					if e = mixed.Fetch(ctx, cfg.Name, from, clock, func(got *partsread.Series) error { check("mixed", got); return nil }); e != nil {
						t.Fatal(e)
					}
					if e = bound.Fetch(ctx, cfg.Name, from, clock, func(got *partsread.Series) error { check("bound-mixed", got); return nil }); e != nil {
						t.Fatal(e)
					}
					h, e := whole.Pin(partsread.NewBudget(128<<20), 128<<20)
					if e != nil {
						t.Fatal(e)
					}
					s, e := h.Render(ctx, cfg.Name, from, clock, clock)
					if e != nil {
						t.Fatal(e)
					}
					check("v2-only", s)
					h.Close()
					old, e := full.Fetch(ctx, cfg.Name, int(from), int(clock))
					if e != nil {
						t.Fatal(e)
					}
					if old == nil {
						check("legacy-only", nil)
					} else {
						check("legacy-only", &partsread.Series{From: int64(old.FromTime), Until: int64(old.UntilTime), Step: int64(old.Step), Values: old.Values})
					}
					queries++
					if shape != nil {
						slots += len(shape.Values)
					}
				}
			})
		}
	}
	t.Logf("schemas=%d methods=6 ranges=4 queries=%d slots=%d per oracle; classic vs legacy-only/v2-only/mixed; actual frozen continuation legacy", len(schemas), queries, slots)
}

var schemas = []string{
	"1s:1d", "10s:3d", "1s:2d", "1m:35d", "5m:90d", "15m:180d", "30m:1y", "1h:2y", "1d:10y",
	"1s:1d,1m:30d,1h:1y,1d:10y", "1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y", "1s:3d,1m:14d,30m:2y",
	"10s:2d,1m:14d,30m:2y", "10m:1y,1d:10y", "1s:2d,1m:28d,1h:2y", "1s:2d,1m:62d,10m:5y",
	"10m:2d,1h:9d,1d:3y", "10s:4d,1m:31d,30m:4y", "1m:7d,5m:60d,1h:2y", "1m:14d,5m:60d,1h:2y",
	"1m:62d,30m:2y", "1h:90d,1d:2y", "1s:7d,1m:14d,30m:2y", "1s:1d,1m:30d,1h:2y",
	"1s:3d,1m:14d,30m:2y", "5m:60d,30m:2y", "10m:7d,1h:5y,1d:10y", "1m:14d,1h:5y",
	"10s:15d,1m:100d,15m:2y", "1m:200d,1h:5y", "1m:400d,1d:10y", "1m:400d,1h:5y",
	"1m:100d,30m:2y", "5m:400d,1h:2y", "1m:30d,15m:400d,1h:5y", "5m:400d,1h:5y",
	"5s:15d,1m:100d,15m:5y", "2s:7d,1m:100d,15m:2y", "1s:40d,1m:100d,15m:2y",
	"5m:14d,1d:10y", "1h:90d,1d:10y",
	"10m:30d",
	"12h:10y",
	"15m:1y",
	"15m:840d",
	"1d:2y",
	"1d:3y",
	"1d:5y",
	"1d:90d",
	"1h:10y",
	"1h:15y",
	"1h:1y",
	"1h:30d",
	"1h:5y",
	"1m:14d",
	"1m:14d,5m:60d,60m:2y",
	"1m:25y",
	"1m:2d,1h:1y,1d:10y",
	"1m:30d",
	"1m:7d,5m:60d,60m:2y",
	"1m:90d",
	"1s:7d",
	"1w:10y",
	"2h:10y",
	"30m:30d",
	"30m:3y",
	"5m:1y",
	"5m:30d",
	"5m:420d",
}
