package mixedread

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
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
	allocs := os.Getenv("GO_CARBON_MIXED_ALLOCS") != ""
	ingest := os.Getenv("GO_CARBON_MIXED_INGEST") != ""
	raw := os.Getenv("GO_CARBON_MIXED_RAW") != ""
	persisted := os.Getenv("GO_CARBON_MIXED_PERSISTED") != ""
	profile := os.Getenv("GO_CARBON_MIXED_PROFILE") != ""
	traced := os.Getenv("GO_CARBON_MIXED_TRACE") != ""
	class := os.Getenv("GO_CARBON_MIXED_CLASS")
	path := os.Getenv("GO_CARBON_MIXED_COMPARE")
	local := os.Getenv("GO_CARBON_MIXED_LATENCY_LOCAL") != ""
	if profile || local {
		root = t.TempDir()
	}
	if root == "" {
		t.Skip("explicit isolated benchmark directory required")
	}
	if !profile && !local && filepath.Clean(root) != "/var/lib/carbon/v2-legacy4" && filepath.Clean(root) != "/var/lib/carbon/v2-legacy5" && filepath.Clean(root) != "/var/lib/carbon/v2-legacy6" && filepath.Clean(root) != "/var/lib/carbon/v2-legacy7" && filepath.Clean(root) != "/var/lib/carbon/v2-legacy8" {
		t.Fatal("benchmark requires assigned private host directory")
	}
	if class != "" && !profile && !traced {
		t.Fatal("class selection is for diagnostics only; the latency gate must run every class")
	}
	if class != "" && !slices.Contains([]string{"warm-1h", "warm-24h", "warm-168h", "warm-720h", "cold-blockcache-1h", "cold-blockcache-24h", "cold-blockcache-168h", "cold-blockcache-720h", "warm-find4096"}, class) {
		t.Fatal("unknown diagnostic class")
	}
	seed := int64(1)
	if value := os.Getenv("GO_CARBON_MIXED_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(seed))
	fmt.Printf("MATCHED seed=%d comparison=%q; OS page cache retained\n", seed, path)
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
	paths := []string{filepath.Join(dir, "legacy"), filepath.Join(dir, "full"), filepath.Join(dir, "control")}
	stores := make([]*chunkstore.Store, len(paths))
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
		if e = stores[2].UpdateMany(ctx, cfg.Name, ps); e != nil {
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
	var disk *persistedLatency
	if persisted {
		disk = newPersistedLatency(t, filepath.Join(dir, "persisted"), cat, m, records, ingest)
		defer disk.close(t)
	}
	borrowedSnapshots := make(map[*chunkstore.Store]*partsread.Snapshot)
	defer func() {
		for _, s := range borrowedSnapshots {
			s.Close()
		}
	}()
	var pub partsread.Publication
	pub.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{partsread.Freeze(records)}})
	defer pub.Close()
	var writers sync.Map
	open := func() (*chunkstore.Store, *chunkstore.Store, *Reader) {
		t.Helper()
		legacyPath := paths[0]
		if path == "native" {
			legacyPath = paths[2]
		}
		old, e := chunkstore.Open(legacyPath, opts)
		if e != nil {
			t.Fatal(e)
		}
		full, e := chunkstore.Open(paths[1], opts)
		if e != nil {
			t.Fatal(e)
		}
		frozen, e := old.ResolveFrozen(ctx, cfg.Name)
		if e != nil {
			t.Fatal(e)
		}
		binding, e := BindLegacy(frozen, m)
		if e != nil {
			t.Fatal(e)
		}
		entries := make(map[string]catalogEntry, len(names))
		for name, metric := range names {
			entries[name] = catalogEntry{metric: metric}
		}
		entries[cfg.Name] = catalogEntry{metric: m, legacy: binding}
		var boundPub partsread.Publication
		view := partsread.View{Parts: []partsread.Source{partsread.Freeze(records)}}
		if disk != nil {
			view, e = disk.p.AcquireView()
			if e != nil {
				t.Fatal(e)
			}
		}
		view.Catalog = entryCatalog{Catalog: cat, entries: entries}
		boundPub.Publish(view)
		snapshot, e := boundPub.Snapshot()
		if e != nil {
			t.Fatal(e)
		}
		if !ingest || disk != nil {
			boundPub.Close()
		}
		borrowedSnapshots[old] = snapshot
		r, e := New(Options{Snapshot: snapshot, Legacy: old, LegacyCatalog: cat, UnifiedCatalog: true, LegacyHorizon: clock - 61, Parts: partsread.Adapter{Select: func(string) *partsread.Publication { return &pub }, Catalog: &pub, Budget: partsread.NewBudget(128 << 20), WorkBytes: 32 << 20, Now: opts.Now}})
		if e != nil {
			t.Fatal(e)
		}
		if ingest && disk == nil {
			stop := make(chan struct{})
			done := make(chan struct{})
			ready := make(chan struct{})
			var published atomic.Uint64
			writers.Store(old, func() { close(stop); <-done; fmt.Printf("INGEST publications=%d\n", published.Load()) })
			// Readers retain an immutable generation while the same part publication
			// accepts newly sealed parts. The legacy store remains read-only by design.
			go func() {
				defer close(done)
				defer boundPub.Close()
				for {
					select {
					case <-stop:
						return
					default:
						n := published.Add(1)
						next := slices.Clone(records)
						for i := range 256 {
							next = append(next, partsread.Record{ID: 43, Step: 1, Sample: partsread.Sample{Time: clock + int64(n)*256 + int64(i), Bits: math.Float64bits(float64(i)), Sequence: n}})
						}
						// Include an exact replacement of a queried metric slot as well as
						// new sibling samples; readers retain their coherent pinned generation.
						next = append(next, partsread.Record{ID: 42, Step: 1, Sample: partsread.Sample{Time: clock - 1, Bits: math.Float64bits(float64((clock-1)%19) - 9), Sequence: n + uint64(clock)}})
						boundPub.Publish(partsread.View{Catalog: entryCatalog{Catalog: cat, entries: entries}, Parts: []partsread.Source{partsread.Freeze(next)}})
						if n == 1 {
							close(ready)
						}
						runtime.Gosched()
					}
				}
			}()
			<-ready
		}
		return old, full, r
	}
	closePair := func(old, full *chunkstore.Store) {
		if stop, ok := writers.LoadAndDelete(old); ok {
			stop.(func())()
		}
		// Each generation drains before its legacy store closes. Do not retain
		// every cold sample's complete catalog until the end of the benchmark.
		borrowedSnapshots[old].Close()
		delete(borrowedSnapshots, old)
		_ = old.Close()
		_ = full.Close()
	}
	when := time.Now().In(time.FixedZone("CET", 3600)).Format("2006-01-02 15:04:05")
	comparison := "Mixed"
	if path == "frozen" {
		comparison = "Frozen"
	}
	if path == "native" {
		comparison = "Native"
	}
	fmt.Printf("| When (CET) | Class | Native p50 us | %s p50 us | Native p99 us | %s p99 us | Gate |\n", comparison, comparison)
	fmt.Println("|---|---|---:|---:|---:|---:|---|")
	warmOrder := []bool{true, false}
	rng.Shuffle(len(warmOrder), func(i, j int) { warmOrder[i], warmOrder[j] = warmOrder[j], warmOrder[i] })
	widths := []int64{3600, 86400, 7 * 86400, 30 * 86400}
	rng.Shuffle(len(widths), func(i, j int) { widths[i], widths[j] = widths[j], widths[i] })
	for _, warm := range warmOrder {
		if path == "frozen" && !warm {
			continue
		}
		for _, width := range widths {
			kind := "warm"
			if !warm {
				kind = "cold-blockcache"
			}
			className := fmt.Sprintf("%s-%dh", kind, width/3600)
			if class != "" && class != className {
				continue
			}
			var publishedBefore uint64
			if disk != nil {
				publishedBefore = disk.publications.Load()
			}
			var base, mix []int64
			var baseBytes, mixedBytes, baseAllocs, mixedAllocs uint64
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
			closePair(old, full)
			old, full, r = open()
			count := 10000
			if !warm {
				count = 1000
			}
			// Footprint/profile runs are separate diagnostics, never gate samples.
			if allocs || profile {
				count = 1000
				if !warm {
					count = 100
				}
			}
			// Both warm paths complete their native lazy initialization first.
			// Cold samples still use fresh block caches; no OS cache flush.
			if warm {
				for range 10 {
					if _, err := full.Fetch(ctx, cfg.Name, int(clock-width), int(clock)); err != nil {
						t.Fatal(err)
					}
					if err := r.Fetch(ctx, cfg.Name, clock-width, clock, func(*partsread.Series) error { return nil }); err != nil {
						t.Fatal(err)
					}
				}
			}
			frozen, err := old.ResolveFrozen(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			// Dense profiling is separate from the predeclared paired gate. Keep
			// CPU sampling dense without per-call MemStats or allocation tracing.
			if duration := os.Getenv("GO_CARBON_MIXED_DENSE"); duration != "" {
				d, err := time.ParseDuration(duration)
				if err != nil || d < 30*time.Second || class == "" || !profile {
					t.Fatal("dense diagnostics require a class, profile mode and >=30s per arm")
				}
				for _, arm := range []string{"native", "mixed"} {
					pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("arm", arm, "class", className)))
					started, calls, nanos := time.Now(), 0, int64(0)
					for time.Since(started) < d {
						if !warm && calls > 0 {
							old, full, r = open()
						}
						region := trace.StartRegion(ctx, arm+"-fetch")
						a := time.Now()
						if arm == "native" {
							_, err = full.Fetch(ctx, cfg.Name, int(clock-width), int(clock))
						} else {
							err = r.Fetch(ctx, cfg.Name, clock-width, clock, func(*partsread.Series) error { return nil })
						}
						nanos += time.Since(a).Nanoseconds()
						region.End()
						if err != nil {
							t.Fatal(err)
						}
						calls++
						if !warm {
							closePair(old, full)
						}
					}
					fmt.Printf("DENSE class=%s arm=%s calls=%d fetch_ns=%d elapsed_ns=%d\n", className, arm, calls, nanos, time.Since(started).Nanoseconds())
					if !warm {
						old, full, r = open()
					}
				}
				pprof.SetGoroutineLabels(ctx)
				closePair(old, full)
				continue
			}
			base = make([]int64, 0, count)
			mix = make([]int64, 0, count)
			measure := func(mixed bool) {
				var before, after runtime.MemStats
				if profile || allocs {
					kind := "native"
					if mixed {
						kind = "mixed"
					}
					pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("class", fmt.Sprintf("%t-%dh-%s", warm, width/3600, kind))))
					runtime.ReadMemStats(&before)
				}
				var region *trace.Region
				if traced {
					region = trace.StartRegion(ctx, fmt.Sprintf("warm=%t,hours=%d,mixed=%t,sample=%d", warm, width/3600, mixed, len(base)+len(mix)))
				}
				a := time.Now()
				var err error
				if mixed {
					if path == "frozen" {
						_, err = old.FetchFrozen(ctx, frozen, int(clock-width), int(clock))
					} else if path == "legacy" || path == "native" {
						_, err = old.Fetch(ctx, cfg.Name, int(clock-width), int(clock))
					} else {
						err = r.Fetch(ctx, cfg.Name, clock-width, clock, func(*partsread.Series) error { return nil })
					}
				} else if path == "frozen" {
					_, err = old.Fetch(ctx, cfg.Name, int(clock-width), int(clock))
				} else {
					_, err = full.Fetch(ctx, cfg.Name, int(clock-width), int(clock))
				}
				elapsed := time.Since(a).Nanoseconds()
				if region != nil {
					region.End()
				}
				if mixed {
					mix = append(mix, elapsed)
				} else {
					base = append(base, elapsed)
				}
				if profile || allocs {
					runtime.ReadMemStats(&after)
					if mixed {
						mixedBytes += after.TotalAlloc - before.TotalAlloc
						mixedAllocs += after.Mallocs - before.Mallocs
					} else {
						baseBytes += after.TotalAlloc - before.TotalAlloc
						baseAllocs += after.Mallocs - before.Mallocs
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
					frozen, err = old.ResolveFrozen(ctx, cfg.Name)
					if err != nil {
						t.Fatal(err)
					}
				}
				if rng.Intn(2) == 0 {
					measure(false)
					measure(true)
				} else {
					measure(true)
					measure(false)
				}
				if !warm {
					closePair(old, full)
				}
			}
			if warm {
				closePair(old, full)
			}
			if disk != nil && ingest {
				delta := disk.publications.Load() - publishedBefore
				fmt.Printf("PERSISTED_CLASS class=%s publications=%d\n", className, delta)
				if delta == 0 {
					t.Fatal("persisted publisher made no progress during class")
				}
			}
			if profile || allocs {
				fmt.Printf("ALLOC warm=%t hours=%d native=%d mixed=%d bytes/op\n", warm, width/3600, baseBytes/uint64(count), mixedBytes/uint64(count))
			}

			if allocs {
				fmt.Printf("ALLOCS class=%s native_allocs=%.3f mixed_allocs=%.3f native_bytes=%.3f mixed_bytes=%.3f\n", className, float64(baseAllocs)/float64(count), float64(mixedAllocs)/float64(count), float64(baseBytes)/float64(count), float64(mixedBytes)/float64(count))
			}
			if raw {
				emitLatencyPairs(seed, className, comparison, ingest, base, mix)
			}
			bp, bq := quantiles(base)
			mp, mq := quantiles(mix)
			gate := "FAIL"
			if mp <= bp && mq <= bq {
				gate = "PASS"
			}
			fmt.Printf("| %s | %s %dh | %.1f | %.1f | %.1f | %.1f | %s |\n", when, kind, width/3600, float64(bp)/1000, float64(mp)/1000, float64(bq)/1000, float64(mq)/1000, gate)
			fmt.Printf("LATENCY seed=%d class=%s comparison=%s samples=%d native_p50_ns=%d native_p99_ns=%d mixed_p50_ns=%d mixed_p99_ns=%d gate=%s\n", seed, className, comparison, count, bp, bq, mp, mq, gate)
		}
	}
	if path == "frozen" || (class != "" && class != "warm-find4096") {
		return
	}
	var findPublishedBefore uint64
	if disk != nil {
		findPublishedBefore = disk.publications.Load()
	}
	old, full, r := open()
	defer closePair(old, full)
	var nativeNames, mixedNames []string
	if err := cat.Find(ctx, "*", func(name string) error { nativeNames = append(nativeNames, name); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.Find(ctx, "*", func(name string) error { mixedNames = append(mixedNames, name); return nil }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(nativeNames, mixedNames) {
		t.Fatal("latency find fixture names mismatch")
	}
	findCount := 10000
	if allocs || profile {
		findCount = 1000
	}
	base, mix := make([]int64, 0, findCount), make([]int64, 0, findCount)
	var before, after runtime.MemStats
	var baseBytes, mixedBytes, baseAllocs, mixedAllocs uint64
	measureFind := func(mixed bool, sample int) {
		var region *trace.Region
		kind := "native"
		if mixed {
			kind = "mixed"
		}
		if traced {
			region = trace.StartRegion(ctx, fmt.Sprintf("find-%s-%d", kind, sample))
		}
		if profile || allocs {
			pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("class", "find-"+kind)))
			runtime.ReadMemStats(&before)
		}
		started := time.Now()
		var err error
		if mixed && path != "native" {
			err = r.Find(ctx, "*", func(string) error { return nil })
		} else {
			err = cat.Find(ctx, "*", func(string) error { return nil })
		}
		elapsed := time.Since(started).Nanoseconds()
		if region != nil {
			region.End()
		}
		if mixed {
			mix = append(mix, elapsed)
		} else {
			base = append(base, elapsed)
		}
		if profile || allocs {
			runtime.ReadMemStats(&after)
			if mixed {
				mixedBytes += after.TotalAlloc - before.TotalAlloc
				mixedAllocs += after.Mallocs - before.Mallocs
			} else {
				baseBytes += after.TotalAlloc - before.TotalAlloc
				baseAllocs += after.Mallocs - before.Mallocs
			}
			pprof.SetGoroutineLabels(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for sample := range findCount {
		if rng.Intn(2) == 0 {
			measureFind(false, sample)
			measureFind(true, sample)
		} else {
			measureFind(true, sample)
			measureFind(false, sample)
		}
	}

	if disk != nil && ingest {
		delta := disk.publications.Load() - findPublishedBefore
		fmt.Printf("PERSISTED_CLASS class=warm-find4096 publications=%d\n", delta)
		if delta == 0 {
			t.Fatal("persisted publisher made no progress during Find")
		}
	}
	if profile {
		fmt.Printf("ALLOC find native=%d mixed=%d bytes/op\n", baseBytes/uint64(findCount), mixedBytes/uint64(findCount))
	}
	if allocs {
		fmt.Printf("ALLOCS class=warm-find4096 native_allocs=%.3f mixed_allocs=%.3f native_bytes=%.3f mixed_bytes=%.3f\n", float64(baseAllocs)/float64(findCount), float64(mixedAllocs)/float64(findCount), float64(baseBytes)/float64(findCount), float64(mixedBytes)/float64(findCount))
	}
	if raw {
		emitLatencyPairs(seed, "warm-find4096", comparison, ingest, base, mix)
	}
	bp, bq := quantiles(base)
	mp, mq := quantiles(mix)
	gate := "FAIL"
	if mp <= bp && mq <= bq {
		gate = "PASS"
	}
	fmt.Printf("| %s | warm find4096 | %.1f | %.1f | %.1f | %.1f | %s |\n", when, float64(bp)/1000, float64(mp)/1000, float64(bq)/1000, float64(mq)/1000, gate)
	fmt.Printf("LATENCY seed=%d class=warm-find4096 comparison=%s samples=%d native_p50_ns=%d native_p99_ns=%d mixed_p50_ns=%d mixed_p99_ns=%d gate=%s\n", seed, comparison, len(base), bp, bq, mp, mq, gate)
}
func quantiles(ns []int64) (int64, int64) {
	slices.Sort(ns)
	return ns[len(ns)/2], ns[(len(ns)*99-1)/100]
}

// Emit before quantiles sorts the arrays, preserving paired request order.
func emitLatencyPairs(seed int64, class, comparison string, ingest bool, native, mixed []int64) {
	row := struct {
		Seed              int64
		Class, Comparison string
		Ingest            bool
		Native, Mixed     []int64
	}{seed, class, comparison, ingest, native, mixed}
	b, err := json.Marshal(row)
	if err != nil {
		panic(err)
	}
	fmt.Printf("PAIRS %s\n", b)
}
