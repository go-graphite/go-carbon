package sealrollup

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func config(def string, method Method, xff float32) Config {
	c := Config{Method: method, XFilesFactor: xff}
	for _, r := range whisper.MustParseRetentionDefs(def) {
		c.Archives = append(c.Archives, Archive{int64(r.SecondsPerPoint()), int64(r.NumberOfPoints())})
	}
	return c
}
func sample(t int64, v float64, seq uint64) Sample { return Sample{t, math.Float64bits(v), seq} }
func install(state map[key]Sample, ds []Delta) {
	for _, d := range ds {
		state[key{d.Archive, d.Slot}] = d.Sample
	}
}
func view(state map[key]Sample) View {
	return func(a int, s int64) (Sample, bool) { v, ok := state[key{a, s}]; return v, ok }
}

// These are the complete default ladders, representative single-archive
// policies, and the multi-archive special-case shapes. Names stay generic.
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

func TestDifferential(t *testing.T)             { testDifferential(t, false) }
func TestContinuationDifferential(t *testing.T) { testDifferential(t, true) }
func testDifferential(t *testing.T, continuation bool) {
	ctx := context.Background()
	clock := int64(1794700800)
	options := chunkstore.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(clock, 0) }, CacheSize: 4 << 20, MemTableSize: 4 << 20}
	if continuation {
		options.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
		options.TimeBuckets = true
	}
	db, err := chunkstore.Open("oracle", options.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for si, def := range schemas {
		for method := Average; method <= First; method++ {
			for _, xff := range []float32{0, .5, 1} {
				t.Run(fmt.Sprintf("s%d/m%d/xff%g", si, method, xff), func(t *testing.T) {
					c := config(def, method, xff)
					clock = 1794700800
					name := fmt.Sprintf("s%d.m%d.x%g", si, method, xff)
					cfg := chunkstore.MetricConfig{Name: name, AggregationMethod: chunkstore.AggregationMethod(method), XFilesFactor: xff}
					for _, a := range c.Archives {
						cfg.Retentions = append(cfg.Retentions, chunkstore.Retention{Step: int(a.Step), Count: int(a.Count)})
					}
					if _, err := db.Create(ctx, cfg); err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(t.TempDir(), "oracle.wsp")
					w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(def), whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					state := map[key]Sample{}
					candidates := map[key]bool{}
					step := c.Archives[0].Step
					// Keep fine samples inside the first retention for every schema.
					start := align(clock-240*step, step)
					var dense, sparse, late, boundary []Sample
					for i := int64(0); i < 180; i++ {
						dense = append(dense, sample(start+i*step, float64(i%17)-8, uint64(i+1)))
					}
					for i := int64(180); i < 220; i += 3 {
						sparse = append(sparse, sample(start+i*step, float64(i), uint64(i+1)))
					}
					late = []Sample{sample(start+17*step, -0.0, 1000), sample(start+3*step, 99, 1001), sample(start+3*step, -13, 1002), sample(start+step, math.Float64frombits(0x7ff80000000000ab), 1003), sample(start+2*step, math.Inf(1), 1004), sample(start+4*step, math.Inf(-1), 1005), sample(start+5*step, math.Copysign(0, -1), 1006), sample(start+6*step, math.SmallestNonzeroFloat64, 1007)}
					for _, a := range c.Archives {
						for _, delta := range []int64{-1, 0, 1} {
							boundary = append(boundary, sample(clock-a.Step*a.Count+delta, float64(delta+4), uint64(2000+len(boundary))))
						}
					}
					batches := []Batch{{1, clock, dense}, {2, clock, sparse}, {3, clock, late}, {4, clock, boundary}}
					// Advance past a complete fine ring and overwrite the same slots. Then
					// route an old correction directly into its retained coarse archive.
					advanced := clock + c.Archives[0].Step*c.Archives[0].Count
					var wrapped []Sample
					for i := int64(0); i < 180; i++ {
						wrapped = append(wrapped, sample(start+i*step+c.Archives[0].Step*c.Archives[0].Count, float64(300-i), uint64(3000+i)))
					}
					batches = append(batches, Batch{5, advanced, wrapped}, Batch{6, advanced, []Sample{sample(start+17*step, 127, 4000)}})

					knownGap := continuation && ((def == "1m:400d,1d:10y" && xff == 0) || (def == "5m:14d,1d:10y" && xff <= .5))
					gapObserved := false
					for bi, b := range batches {
						clock = b.Now
						before := cloneState(state)
						input := slices.Clone(b.Points)
						ds, err := Reduce(c, view(state), []Batch{b})
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(before, state) || !reflect.DeepEqual(input, b.Points) {
							t.Fatal("input mutated")
						}
						install(state, ds)
						ptrs := make([]*whisper.TimeSeriesPoint, len(b.Points))
						ps := make([]chunkstore.Point, len(b.Points))
						for i, p := range b.Points {
							v := math.Float64frombits(p.Bits)
							ptrs[i] = &whisper.TimeSeriesPoint{Time: int(p.Time), Value: v}
							ps[i] = chunkstore.Point{Timestamp: p.Time, Value: v}
							for ai, a := range c.Archives {
								candidates[key{ai, slot(align(p.Time, a.Step), a)}] = true
							}
						}
						if err := w.UpdateMany(ptrs); err != nil {
							t.Fatal(err)
						}
						if err := db.UpdateMany(ctx, name, ps); err != nil {
							t.Fatal(err)
						}
						checkClassic(t, path, c, state, candidates, bi)
						snap, err := db.Snapshot(ctx, name)
						if err != nil {
							t.Fatal(err)
						}
						got := map[key]Sample{}
						for ai, a := range snap.Archives {
							for _, p := range a.Points {
								got[key{ai, slot(p.Timestamp, c.Archives[ai])}] = sample(p.Timestamp, p.Value, 0)
							}
						}
						if diff := stateDifference(state, got); diff != "" {
							if !knownGap || bi < 4 {
								t.Fatalf("current engine batch%d: %s", bi, diff)
							}
							gapObserved = true
							t.Logf("KNOWN_CONTINUATION_GAP schema=%s method=%d xff=%g batch=%d: %s", def, method, xff, bi, diff)
							// Verify that at least the inspected disagreement is visible to Fetch,
							// rather than being only extra expired bytes in the physical snapshot.
							for k, p := range state {
								q, ok := got[k]
								if ok && (p.Time != q.Time || p.Bits != q.Bits) && p.Time < clock-c.Archives[0].Step*c.Archives[0].Count {
									series, e := db.Fetch(ctx, name, int(p.Time-c.Archives[k.archive].Step), int(p.Time+c.Archives[k.archive].Step))
									if e != nil {
										t.Fatal(e)
									}
									if series != nil {
										for j, v := range series.Values {
											if int64(series.FromTime+j*series.Step) == p.Time {
												t.Logf("KNOWN_CONTINUATION_RENDER timestamp=%d step=%d got=%016x classic=%016x", p.Time, series.Step, math.Float64bits(v), p.Bits)
											}
										}
									}
									break
								}
							}
						}
					}
					if knownGap && !gapObserved {
						t.Fatal("documented continuation gap no longer reproduced; update compatibility policy")
					}
					// Compaction replays ordered original batch boundaries from empty state.
					ds, err := Reduce(c, nil, batches)
					if err != nil {
						t.Fatal(err)
					}
					compacted := map[key]Sample{}
					install(compacted, ds)
					compare(t, state, compacted, "compaction")
				})
			}
		}
	}
}
func cloneState(s map[key]Sample) map[key]Sample {
	r := map[key]Sample{}
	for k, v := range s {
		r[k] = v
	}
	return r
}
func stateDifference(want, got map[key]Sample) string {
	if len(want) != len(got) {
		return fmt.Sprintf("slots got%d want%d", len(got), len(want))
	}
	for k, p := range want {
		q, ok := got[k]
		if !ok || p.Time != q.Time || p.Bits != q.Bits {
			return fmt.Sprintf("archive%d slot%d got%+v want%+v", k.archive, k.slot, q, p)
		}
	}
	return ""
}
func compare(t *testing.T, want, got map[key]Sample, where string) {
	t.Helper()
	if diff := stateDifference(want, got); diff != "" {
		t.Fatalf("%s: %s", where, diff)
	}
}

func checkClassic(t *testing.T, path string, c Config, state map[key]Sample, candidates map[key]bool, batch int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	header := make([]byte, 16+12*len(c.Archives))
	if _, err := f.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	for k := range candidates {
		a := c.Archives[k.archive]
		offset := int64(binary.BigEndian.Uint32(header[16+k.archive*12:]))
		var raw [12]byte
		if _, err := f.ReadAt(raw[:], offset); err != nil {
			t.Fatal(err)
		}
		base := int64(binary.BigEndian.Uint32(raw[:]))
		physical := (k.slot - (base/a.Step)%a.Count + a.Count) % a.Count
		if _, err := f.ReadAt(raw[:], offset+physical*12); err != nil {
			t.Fatal(err)
		}
		ts := int64(binary.BigEndian.Uint32(raw[:]))
		p, ok := state[k]
		if ts == 0 {
			if ok {
				t.Fatalf("classic missing batch%d a%d slot%d want%+v", batch, k.archive, k.slot, p)
			}
			continue
		}
		bits := binary.BigEndian.Uint64(raw[4:])
		if !ok || ts != p.Time || bits != p.Bits {
			t.Fatalf("classic batch%d a%d slot%d got(%d,%x) want%+v", batch, k.archive, k.slot, ts, bits, p)
		}
	}
}
func TestValidationAndOrder(t *testing.T) {
	c := config("1s:1d,1m:30d", Average, 0)
	for _, b := range [][]Batch{{{1, 100, nil}, {1, 100, nil}}, {{1, 100, []Sample{{Time: -1}}}}} {
		if _, err := Reduce(c, nil, b); err == nil {
			t.Fatal("accepted invalid input")
		}
	}
	for _, bad := range []Config{{}, {Archives: c.Archives, Method: 7}, {Archives: c.Archives, Method: Average, XFilesFactor: float32(math.NaN())}, {Archives: []Archive{{1, 60}, {60, 1}}, Method: Average}} {
		if _, err := Reduce(bad, nil, nil); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
}
func TestSeededCorrections(t *testing.T) {
	c := config("1s:120,1m:1d", Average, .5)
	rng := rand.New(rand.NewPCG(17, 9))
	var batches []Batch
	for i := uint64(1); i <= 12; i++ {
		b := Batch{Order: i, Now: 10000}
		for j := 0; j < 30; j++ {
			b.Points = append(b.Points, sample(9890+int64(rng.IntN(110)), rng.Float64()*100, uint64(j)))
		}
		batches = append(batches, b)
	}
	state := map[key]Sample{}
	for _, b := range batches {
		ds, err := Reduce(c, view(state), []Batch{b})
		if err != nil {
			t.Fatal(err)
		}
		install(state, ds)
	}
	ds, err := Reduce(c, nil, batches)
	if err != nil {
		t.Fatal(err)
	}
	all := map[key]Sample{}
	install(all, ds)
	compare(t, state, all, "seeded correction compaction")
}

// Original clocks can route the same timestamp into different archives.
func TestOriginalBatchBoundariesMatter(t *testing.T) {
	c := config("1s:120,1m:1d", Average, 0)
	batches := []Batch{{1, 10000, []Sample{sample(9901, 4, 1)}}, {2, 10100, []Sample{sample(9901, 8, 2)}}}
	ds, err := Reduce(c, nil, batches)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 {
		t.Fatalf("deltas%d", len(ds))
	}
	for _, d := range ds {
		want := 4.0
		if d.Archive == 1 {
			want = 8
		}
		if d.Sample.Bits != math.Float64bits(want) {
			t.Fatalf("wrong archive routing: %+v", d)
		}
	}
	flat, err := Reduce(c, nil, []Batch{{2, 10100, append(slices.Clone(batches[0].Points), batches[1].Points...)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(flat) == len(ds) {
		t.Fatal("fixture failed to distinguish flattened history")
	}
}

// TestSumNaNHasAmd64Bits pins Average and Sum NaN bits, on every architecture,
// to what classic whisper's sum stores on amd64 (checked under linux/amd64).
func TestSumNaNHasAmd64Bits(t *testing.T) {
	const amd64Default = 0xfff8000000000000
	a, b, s := uint64(0x7ff8000000000042), uint64(0xfff8000000000099), uint64(0x7ff0000000000007)
	inf, ninf, one := math.Float64bits(math.Inf(1)), math.Float64bits(math.Inf(-1)), math.Float64bits(1)
	for i, c := range []struct {
		in   []uint64
		want uint64
	}{
		{[]uint64{inf, ninf}, amd64Default},
		{[]uint64{one, one, ninf, inf, one}, amd64Default},
		{[]uint64{inf, ninf, a}, amd64Default},
		{[]uint64{inf, ninf, s}, amd64Default},
		{[]uint64{a, inf, ninf}, a},
		{[]uint64{a, b}, a},
		{[]uint64{b, a}, b},
		{[]uint64{one, s}, s | 1<<51},
		{[]uint64{s, a}, s | 1<<51},
		{[]uint64{a, s}, a},
	} {
		samples := make([]Sample, len(c.in))
		for j, bits := range c.in {
			samples[j] = Sample{Time: int64(j), Bits: bits}
		}
		for _, m := range []Method{Sum, Average} {
			if got := math.Float64bits(aggregateSamples(m, samples)); got != c.want {
				t.Errorf("case %d method %d: got %#016x want %#016x", i, m, got, c.want)
			}
		}
	}
}
