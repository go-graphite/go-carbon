package mixedread

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// This opt-in diagnostic measures the mixed adapter, with v2 parts in memory.
// It is deliberately not evidence for a production SST reader or ingest soak.
func TestLatencyComparison(t *testing.T) {
	root := os.Getenv("GO_CARBON_MIXED_LATENCY_DIR")
	profile := os.Getenv("GO_CARBON_MIXED_PROFILE") != ""
	path := os.Getenv("GO_CARBON_MIXED_COMPARE")
	local := os.Getenv("GO_CARBON_MIXED_LATENCY_LOCAL") != ""
	if profile || local {
		root = t.TempDir()
	}
	if root == "" {
		t.Skip("explicit isolated benchmark directory required")
	}
	if !profile && !local && filepath.Clean(root) != "/var/lib/carbon/v2-legacy2" {
		t.Fatal("benchmark requires assigned private host directory")
	}
	const clock int64 = 1794700800
	dir, e := os.MkdirTemp(root, "latency-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	ctx := context.Background()
	opts := chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, CacheSize: 32 << 20, Now: func() time.Time { return time.Unix(clock, 0) }}
	cfg := chunkstore.MetricConfig{Name: "bench.metric", Retentions: []chunkstore.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}, AggregationMethod: chunkstore.Average, XFilesFactor: .5}
	m := partsread.Metric{ID: 42, AggregationMethod: "average", XFilesFactor: .5}
	c := sr.Config{Method: sr.Average, XFilesFactor: .5}
	for _, a := range cfg.Retentions {
		m.Archives = append(m.Archives, partsread.Archive{Step: int64(a.Step), Points: int64(a.Count)})
		c.Archives = append(c.Archives, sr.Archive{Step: int64(a.Step), Count: int64(a.Count)})
	}
	paths := []string{filepath.Join(dir, "legacy"), filepath.Join(dir, "full")}
	stores := make([]*chunkstore.Store, 2)
	for i, p := range paths {
		s, e := chunkstore.Open(p, opts)
		if e != nil {
			t.Fatal(e)
		}
		stores[i] = s
		if _, e = s.Create(ctx, cfg); e != nil {
			t.Fatal(e)
		}
	}
	state := map[[2]int64]sr.Sample{}
	var records []partsread.Record
	for bi := 0; bi < 2; bi++ {
		var samples []sr.Sample
		var ps []chunkstore.Point
		from, to := clock-7200, clock-60
		if bi == 1 {
			from, to = to, clock
		}
		for ts := from; ts < to; ts++ {
			v := float64(ts%19) - 9
			samples = append(samples, sr.Sample{Time: ts, Bits: math.Float64bits(v), Sequence: uint64(ts)})
			ps = append(ps, chunkstore.Point{Timestamp: ts, Value: v})
		}
		ds, e := sr.Reduce(c, func(a int, slot int64) (sr.Sample, bool) { p, ok := state[[2]int64{int64(a), slot}]; return p, ok }, []sr.Batch{{Order: uint64(bi + 1), Now: clock, Points: samples}})
		if e != nil {
			t.Fatal(e)
		}
		for _, d := range ds {
			state[[2]int64{int64(d.Archive), d.Slot}] = d.Sample
			if bi == 1 {
				records = append(records, partsread.Record{ID: 42, Step: c.Archives[d.Archive].Step, Sample: partsread.Sample{Time: d.Sample.Time, Bits: d.Sample.Bits, Sequence: d.Order}})
			}
		}
		if e = stores[1].UpdateMany(ctx, cfg.Name, ps); e != nil {
			t.Fatal(e)
		}
		if bi == 0 {
			if e = stores[0].UpdateMany(ctx, cfg.Name, ps); e != nil {
				t.Fatal(e)
			}
		}
	}
	for _, s := range stores {
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
	}
	opts.ReadOnly = true
	names := map[string]partsread.Metric{cfg.Name: m}
	for i := 0; i < 4095; i++ {
		names[fmt.Sprintf("find.metric.%04d", i)] = m
	}
	cat := catalog(names)
	var pub partsread.Publication
	pub.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{partsread.Freeze(records)}})
	defer pub.Close()
	open := func() (*chunkstore.Store, *chunkstore.Store, *Reader) {
		t.Helper()
		old, e := chunkstore.Open(paths[0], opts)
		if e != nil {
			t.Fatal(e)
		}
		full, e := chunkstore.Open(paths[1], opts)
		if e != nil {
			t.Fatal(e)
		}
		r, e := New(Options{Legacy: old, LegacyCatalog: cat, UnifiedCatalog: true, LegacyHorizon: clock - 61, Parts: partsread.Adapter{Select: func(string) *partsread.Publication { return &pub }, Catalog: &pub, Budget: partsread.NewBudget(64 << 20), WorkBytes: 32 << 20, Now: opts.Now}})
		if e != nil {
			t.Fatal(e)
		}
		return old, full, r
	}
	when := time.Now().In(time.FixedZone("CET", 3600)).Format("2006-01-02 15:04:05")
	fmt.Println("| When (CET) | Class | Legacy p50 us | Mixed p50 us | Legacy p99 us | Mixed p99 us | Gate |")
	fmt.Println("|---|---|---:|---:|---:|---:|---|")
	for _, warm := range []bool{true, false} {
		for _, width := range []int64{3600, 86400, 7 * 86400, 30 * 86400} {
			var base, mix []int64
			var baseBytes, mixedBytes uint64
			old, full, r := open()
			// Validate within the callback lifetime, outside measured samples.
			want, err := full.Fetch(ctx, cfg.Name, int(clock-width), int(clock))
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Fetch(ctx, cfg.Name, clock-width, clock, func(got *partsread.Series) error {
				if got.From != int64(want.FromTime) || got.Until != int64(want.UntilTime) || got.Step != int64(want.Step) || len(got.Values) != len(want.Values) {
					t.Fatal("latency fixture shape mismatch")
				}
				for j, v := range want.Values {
					if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
						t.Fatal("latency fixture bits mismatch")
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			_ = old.Close()
			_ = full.Close()
			old, full, r = open()
			count := 200
			if profile {
				count = 2000
			}
			if !warm {
				count = 30
			}
			measure := func(mixed bool) {
				var before, after runtime.MemStats
				if profile {
					kind := "native"
					if mixed {
						kind = "mixed"
					}
					pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("class", fmt.Sprintf("%t-%dh-%s", warm, width/3600, kind))))
					runtime.ReadMemStats(&before)
				}
				a := time.Now()
				var err error
				if mixed {
					if path == "legacy" {
						_, err = old.Fetch(ctx, cfg.Name, int(clock-width), int(clock))
					} else {
						err = r.Fetch(ctx, cfg.Name, clock-width, clock, func(*partsread.Series) error { return nil })
					}
				} else {
					_, err = full.Fetch(ctx, cfg.Name, int(clock-width), int(clock))
				}
				elapsed := time.Since(a).Nanoseconds()
				if mixed {
					mix = append(mix, elapsed)
				} else {
					base = append(base, elapsed)
				}
				if profile {
					runtime.ReadMemStats(&after)
					if mixed {
						mixedBytes += after.TotalAlloc - before.TotalAlloc
					} else {
						baseBytes += after.TotalAlloc - before.TotalAlloc
					}
					pprof.SetGoroutineLabels(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < count; i++ {
				if !warm && i > 0 {
					old, full, r = open()
				}
				if i%2 == 0 {
					measure(false)
					measure(true)
				} else {
					measure(true)
					measure(false)
				}
				if !warm {
					_ = old.Close()
					_ = full.Close()
				}
			}
			if warm {
				_ = old.Close()
				_ = full.Close()
			}
			if profile {
				fmt.Printf("ALLOC warm=%t hours=%d native=%d mixed=%d bytes/op\n", warm, width/3600, baseBytes/uint64(count), mixedBytes/uint64(count))
			}

			bp, bq := quantiles(base)
			mp, mq := quantiles(mix)
			gate := "FAIL"
			if mp <= bp && mq <= bq {
				gate = "PASS"
			}
			kind := "warm"
			if !warm {
				kind = "cold-blockcache"
			}
			fmt.Printf("| %s | %s %dh | %.1f | %.1f | %.1f | %.1f | %s |\n", when, kind, width/3600, float64(bp)/1000, float64(mp)/1000, float64(bq)/1000, float64(mq)/1000, gate)
		}
	}
	old, full, r := open()
	defer old.Close()
	defer full.Close()
	var base, mix []int64
	var before, after runtime.MemStats
	var baseBytes, mixedBytes uint64
	for range 1000 {
		if profile {
			pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("class", "find-native")))
			runtime.ReadMemStats(&before)
		}
		a := time.Now()
		e = cat.Find(ctx, "*", func(string) error { return nil })
		base = append(base, time.Since(a).Nanoseconds())
		if profile {
			runtime.ReadMemStats(&after)
			baseBytes += after.TotalAlloc - before.TotalAlloc
		}
		if e != nil {
			t.Fatal(e)
		}
		if profile {
			pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("class", "find-mixed")))
			runtime.ReadMemStats(&before)
		}
		a = time.Now()
		e = r.Find(ctx, "*", func(string) error { return nil })
		mix = append(mix, time.Since(a).Nanoseconds())
		if profile {
			runtime.ReadMemStats(&after)
			mixedBytes += after.TotalAlloc - before.TotalAlloc
			pprof.SetGoroutineLabels(ctx)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	if profile {
		fmt.Printf("ALLOC find native=%d mixed=%d bytes/op\n", baseBytes/1000, mixedBytes/1000)
	}
	bp, bq := quantiles(base)
	mp, mq := quantiles(mix)
	gate := "FAIL"
	if mp <= bp && mq <= bq {
		gate = "PASS"
	}
	fmt.Printf("| %s | warm find4096 | %.1f | %.1f | %.1f | %.1f | %s |\n", when, float64(bp)/1000, float64(mp)/1000, float64(bq)/1000, float64(mq)/1000, gate)
}
func quantiles(ns []int64) (int64, int64) {
	slices.Sort(ns)
	return ns[len(ns)/2], ns[(len(ns)*99-1)/100]
}
