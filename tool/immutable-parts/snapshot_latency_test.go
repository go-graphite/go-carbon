//go:build sealrollup_integration

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

// latencyArchiveSource is a disk-backed fixture source, not the core's raw-only
// codec. It lets the same read algorithm and authoritative coarse slots be
// compared with the existing continuation engine before daemon wiring exists.
type latencyStages struct {
	Seek, Decode, Emit, Close int64
	Records                   int64
}
type latencyArchiveSource struct {
	snap   *pebble.Snapshot
	stages *latencyStages
}

func (s latencyArchiveSource) scan(ctx context.Context, step, first, end int64, yield func(pr.Sample) error) error {
	var lo, hi [16]byte
	binary.BigEndian.PutUint64(lo[:8], uint64(step))
	binary.BigEndian.PutUint64(hi[:8], uint64(step))
	binary.BigEndian.PutUint64(lo[8:], uint64(first))
	binary.BigEndian.PutUint64(hi[8:], uint64(end))
	var seekStart time.Time
	if s.stages != nil {
		seekStart = time.Now()
	}
	it, e := s.snap.NewIter(&pebble.IterOptions{LowerBound: lo[:], UpperBound: hi[:]})
	if e != nil {
		return e
	}
	defer func() {
		var start time.Time
		if s.stages != nil {
			start = time.Now()
		}
		_ = it.Close()
		if s.stages != nil {
			s.stages.Close += time.Since(start).Nanoseconds()
		}
	}()
	firstOK := it.First()
	if s.stages != nil {
		s.stages.Seek += time.Since(seekStart).Nanoseconds()
	}
	for ok := firstOK; ok; ok = it.Next() {
		if e = ctx.Err(); e != nil {
			return e
		}
		var decodeStart time.Time
		if s.stages != nil {
			decodeStart = time.Now()
		}
		v := it.Value()
		if len(v) != 24 {
			return fmt.Errorf("invalid fixture record")
		}
		sample := pr.Sample{Time: int64(binary.LittleEndian.Uint64(v)), Bits: binary.LittleEndian.Uint64(v[8:]), Sequence: binary.LittleEndian.Uint64(v[16:])}
		if s.stages != nil {
			s.stages.Decode += time.Since(decodeStart).Nanoseconds()
			s.stages.Records++
		}
		var emitStart time.Time
		if s.stages != nil {
			emitStart = time.Now()
		}
		e = yield(sample)
		if s.stages != nil {
			s.stages.Emit += time.Since(emitStart).Nanoseconds()
		}
		if e != nil {
			return e
		}
	}
	return it.Error()
}
func (s latencyArchiveSource) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	if e := ctx.Err(); e != nil {
		return false, e
	}
	var lo, hi [8]byte
	binary.BigEndian.PutUint64(lo[:], uint64(step))
	binary.BigEndian.PutUint64(hi[:], uint64(step+1))
	it, e := s.snap.NewIter(&pebble.IterOptions{LowerBound: lo[:], UpperBound: hi[:]})
	if e != nil {
		return false, e
	}
	defer it.Close()
	ok := it.First()
	return ok, it.Error()
}
func (s latencyArchiveSource) Scan(ctx context.Context, id uint64, a pr.Archive, from, until int64, y func(pr.Sample) error) error {
	ranges, n := pr.SlotRanges(a, from, until)
	for _, r := range ranges[:n] {
		if e := s.scan(ctx, a.Step, r.From, r.Until, y); e != nil {
			return e
		}
	}
	return nil
}
func latencyPercentile(ds []int64, q float64) int64 {
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[min(len(ds)-1, int(math.Ceil(float64(len(ds))*q))-1)]
}

// Opt-in same-host, same-data, same-query comparison. Fresh engine caches retain
// the OS page cache. No global drop_caches, daemon access or live history writes.
// This fixture does not qualify the unfinished production index or live ingest.
func TestSnapshotLatencyComparison(t *testing.T) {
	root := os.Getenv("PARTS_READ_COMPARE_DIR")
	if root == "" {
		t.Skip("set PARTS_READ_COMPARE_DIR to a new private directory")
	}
	if e := os.Mkdir(root, 0750); e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	const clock int64 = 1794700800
	for si, def := range []string{"1s:1d", "1s:1d,1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y"} {
		dir := filepath.Join(root, fmt.Sprint(si))
		if e := os.Mkdir(dir, 0750); e != nil {
			t.Fatal(e)
		}
		opts := chunkstore.Options{Now: func() time.Time { return time.Unix(clock, 0) }, ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, TimeBuckets: true}
		native, e := chunkstore.Open(filepath.Join(dir, "native"), opts)
		if e != nil {
			t.Fatal(e)
		}
		pdb, e := pebble.Open(filepath.Join(dir, "parts"), &pebble.Options{})
		if e != nil {
			t.Fatal(e)
		}
		cfg := chunkstore.MetricConfig{Name: "sample", AggregationMethod: chunkstore.Average, XFilesFactor: .5}
		rc := sr.Config{Method: sr.Average, XFilesFactor: .5}
		var archives []pr.Archive
		for _, r := range whisper.MustParseRetentionDefs(def) {
			a := pr.Archive{Step: int64(r.SecondsPerPoint()), Points: int64(r.NumberOfPoints())}
			archives = append(archives, a)
			cfg.Retentions = append(cfg.Retentions, chunkstore.Retention{Step: int(a.Step), Count: int(a.Points)})
			rc.Archives = append(rc.Archives, sr.Archive{Step: a.Step, Count: a.Points})
		}
		if _, e = native.Create(ctx, cfg); e != nil {
			t.Fatal(e)
		}
		var raw []sr.Sample
		var ps []chunkstore.Point
		for _, a := range archives {
			for i := int64(1); i <= min(a.Points/2, 300); i++ {
				ts := (clock/a.Step - i) * a.Step
				bits := math.Float64bits(float64(i % 19))
				raw = append(raw, sr.Sample{Time: ts, Bits: bits, Sequence: uint64(len(raw) + 1)})
				ps = append(ps, chunkstore.Point{Timestamp: ts, Value: math.Float64frombits(bits)})
			}
		}
		if e = native.UpdateMany(ctx, "sample", ps); e != nil {
			t.Fatal(e)
		}
		deltas, e := sr.Reduce(rc, func(int, int64) (sr.Sample, bool) { return sr.Sample{}, false }, []sr.Batch{{Order: 1, Now: clock, Points: raw}})
		if e != nil {
			t.Fatal(e)
		}
		batch := pdb.NewBatch()
		for i, d := range deltas {
			var k [24]byte
			var v [24]byte
			binary.BigEndian.PutUint64(k[:8], uint64(archives[d.Archive].Step))
			a := archives[d.Archive]
			slot := (d.Sample.Time / a.Step) % a.Points
			if slot < 0 {
				slot += a.Points
			}
			binary.BigEndian.PutUint64(k[8:16], uint64(slot))
			binary.BigEndian.PutUint64(k[16:], uint64(i))
			binary.LittleEndian.PutUint64(v[:8], uint64(d.Sample.Time))
			binary.LittleEndian.PutUint64(v[8:16], d.Sample.Bits)
			binary.LittleEndian.PutUint64(v[16:], d.Order)
			if e = batch.Set(k[:], v[:], nil); e != nil {
				t.Fatal(e)
			}
		}
		if e = batch.Commit(pebble.Sync); e != nil {
			t.Fatal(e)
		}
		_ = batch.Close()
		if e = pdb.Flush(); e != nil {
			t.Fatal(e)
		}
		for _, cold := range []bool{false, true} {
			for _, width := range []int64{3600, 86400, 7 * 86400, 30 * 86400} {
				repeats := 200
				if cold {
					repeats = 30
				}
				if v, e := strconv.Atoi(os.Getenv("PARTS_READ_COMPARE_REPEATS")); e == nil && v > repeats && !cold {
					repeats = v
				}
				profile := os.Getenv("PARTS_READ_PROFILE") != ""
				var stages latencyStages
				var allocV, allocN, mallocV, mallocN, gcV, gcN, pinNS uint64
				label := fmt.Sprintf("%d/%d/%t", si, width, cold)
				var vd, nd []int64
				for iteration := 0; iteration < repeats; iteration++ {
					if cold {
						if e = native.Close(); e != nil {
							t.Fatal(e)
						}
						if e = pdb.Close(); e != nil {
							t.Fatal(e)
						}
						native, e = chunkstore.Open(filepath.Join(dir, "native"), opts)
						if e != nil {
							t.Fatal(e)
						}
						pdb, e = pebble.Open(filepath.Join(dir, "parts"), &pebble.Options{})
						if e != nil {
							t.Fatal(e)
						}
					}
					snap := pdb.NewSnapshot()
					var pub pr.Publication
					pub.Publish(pr.View{Catalog: pr.CatalogFuncs{LookupFunc: func(name string) (pr.Metric, bool) { return pr.Metric{ID: 7, Archives: archives}, name == "sample" }}, Parts: []pr.Source{latencyArchiveSource{snap: snap, stages: func() *latencyStages {
						if profile {
							return &stages
						}
						return nil
					}()}}, Close: func() { _ = snap.Close() }})
					pinStart := time.Now()
					h, e := pub.Pin(pr.NewBudget(128<<20), 128<<20)
					if e != nil {
						t.Fatal(e)
					}
					pinNS += uint64(time.Since(pinStart).Nanoseconds())
					var got *pr.Series
					var want *chunkstore.Series
					var vt, nt int64
					readV2 := func() {
						var before, after runtime.MemStats
						if profile {
							runtime.ReadMemStats(&before)
							pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("class", label, "engine", "v2")))
						}
						start := time.Now()
						got, e = h.Render(ctx, "sample", clock-width, clock, clock)
						vt = time.Since(start).Nanoseconds()
						if profile {
							runtime.ReadMemStats(&after)
							allocV += after.TotalAlloc - before.TotalAlloc
							mallocV += after.Mallocs - before.Mallocs
							gcV += uint64(after.NumGC - before.NumGC)
							pprof.SetGoroutineLabels(ctx)
						}
						if e != nil {
							t.Fatal(e)
						}
					}
					readOld := func() {
						var before, after runtime.MemStats
						if profile {
							runtime.ReadMemStats(&before)
							pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("class", label, "engine", "native")))
						}
						start := time.Now()
						want, e = native.Fetch(ctx, "sample", int(clock-width), int(clock))
						nt = time.Since(start).Nanoseconds()
						if profile {
							runtime.ReadMemStats(&after)
							allocN += after.TotalAlloc - before.TotalAlloc
							mallocN += after.Mallocs - before.Mallocs
							gcN += uint64(after.NumGC - before.NumGC)
							pprof.SetGoroutineLabels(ctx)
						}
						if e != nil {
							t.Fatal(e)
						}
					}
					if iteration%2 == 0 {
						readV2()
						readOld()
					} else {
						readOld()
						readV2()
					}
					if got == nil || want == nil || got.From != int64(want.FromTime) || got.Until != int64(want.UntilTime) || got.Step != int64(want.Step) || len(got.Values) != len(want.Values) {
						t.Fatalf("schema%d width%d shape mismatch", si, width)
					}
					for i, v := range want.Values {
						if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
							t.Fatalf("schema%d width%d slot%d bits%x != %x", si, width, i, math.Float64bits(v), math.Float64bits(got.Values[i]))
						}
					}
					h.Close()
					pub.Close()
					vd = append(vd, vt)
					nd = append(nd, nt)
				}
				if profile {
					_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "latency_profile", "class": label, "samples": repeats, "v2_alloc_bytes": allocV, "native_alloc_bytes": allocN, "v2_mallocs": mallocV, "native_mallocs": mallocN, "v2_gc": gcV, "native_gc": gcN, "pin_ns": pinNS, "source": stages})
				}
				v50, v99, n50, n99 := latencyPercentile(vd, .5), latencyPercentile(vd, .99), latencyPercentile(nd, .5), latencyPercentile(nd, .99)
				_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "snapshot_latency_comparison", "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "schema": def, "range_seconds": width, "fresh_engine_cache": cold, "os_page_cache": "uncontrolled", "samples": repeats, "v2_p50_ns": v50, "v2_p99_ns": v99, "native_p50_ns": n50, "native_p99_ns": n99, "latency_gate": v50 <= n50 && v99 <= n99, "exact_bits": true, "source": "disk_fixture_archive_codec", "sustained_ingest": false})
			}
		}
		if e = native.Close(); e != nil {
			t.Fatal(e)
		}
		if e = pdb.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
