package v2read

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	whisper "github.com/go-graphite/go-whisper"
)

// Full retention definitions, including generated single-resolution policies.
// Keep names generic: only archive geometry participates in the oracle.
var boundarySchemas = []string{
	"10m:1y,1d:10y",
	"10m:2d,1h:9d,1d:3y",
	"10m:30d",
	"10m:700d",
	"10m:7d,1h:5y,1d:10y",
	"10s:15d,1m:100d,15m:2y",
	"10s:2d,1m:14d,30m:2y",
	"10s:3d",
	"10s:4d,1m:31d,30m:4y",
	"120m:700d",
	"12h:10y",
	"12m:700d",
	"144m:700d",
	"15m:180d",
	"15m:1y",
	"15m:700d",
	"15m:840d",
	"160m:700d",
	"16m:700d",
	"180m:700d",
	"18m:700d",
	"1d:10y",
	"1d:2y",
	"1d:3y",
	"1d:5y",
	"1d:90d",
	"1h:10y",
	"1h:15y",
	"1h:1y",
	"1h:2y",
	"1h:30d",
	"1h:5y",
	"1h:90d,1d:10y",
	"1h:90d,1d:2y",
	"1m:100d,30m:2y",
	"1m:14d",
	"1m:14d,1h:5y",
	"1m:14d,30m:2y",
	"1m:14d,5m:60d,60m:2y",
	"1m:200d,1h:5y",
	"1m:25y",
	"1m:2d,1h:1y,1d:10y",
	"1m:30d",
	"1m:30d,15m:400d,1h:5y",
	"1m:30d,1h:1y,1d:10y",
	"1m:35d",
	"1m:400d,1d:10y",
	"1m:400d,1h:5y",
	"1m:62d,30m:2y",
	"1m:700d",
	"1m:7d,5m:60d,60m:2y",
	"1m:90d",
	"1s:1d",
	"1s:1d,1m:30d,1h:1y,1d:10y",
	"1s:1d,1m:30d,1h:2y",
	"1s:2d",
	"1s:2d,1m:28d,1h:2y",
	"1s:2d,1m:62d,10m:5y",
	"1s:3d,1m:14d,30m:2y",
	"1s:40d,1m:100d,15m:2y",
	"1s:7d",
	"1s:7d,1m:14d,30m:2y",
	"1w:10y",
	"20m:700d",
	"240m:700d",
	"24m:700d",
	"2h:10y",
	"2m:700d",
	"2s:7d,1m:100d,15m:2y",
	"30m:1y",
	"30m:30d",
	"30m:3y",
	"30m:700d",
	"32m:700d",
	"36m:700d",
	"3m:700d",
	"40m:700d",
	"45m:700d",
	"48m:700d",
	"4m:700d",
	"5m:14d,1d:10y",
	"5m:1y",
	"5m:30d",
	"5m:400d,1h:2y",
	"5m:400d,1h:5y",
	"5m:420d",
	"5m:60d,30m:2y",
	"5m:700d",
	"5m:90d",
	"5s:15d,1m:100d,15m:5y",
	"60m:700d",
	"6m:700d",
	"72m:700d",
	"80m:700d",
	"8m:700d",
	"90m:700d",
	"96m:700d",
	"9m:700d",
}

type boundarySlot struct {
	archive int
	slot    int64
}

func boundaryAlign(t, step int64) int64 { return t - t%step }
func boundaryKey(a int, t int64, c sealrollup.Config) boundarySlot {
	r := c.Archives[a]
	return boundarySlot{a, (t / r.Step) % r.Count}
}

// Seed from actual classic physical bytes, rather than from the reducer under
// test. Reading candidates includes empty slots and stale ring epochs. Fetch
// alone cannot distinguish an occupied NaN from a missing slot.
func boundaryClassicSlots(t *testing.T, path string, c sealrollup.Config, candidates map[boundarySlot]bool) map[boundarySlot]sealrollup.Sample {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	header := make([]byte, 16+12*len(c.Archives))
	if _, err = f.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	state := map[boundarySlot]sealrollup.Sample{}
	for k := range candidates {
		a := c.Archives[k.archive]
		offset := int64(binary.BigEndian.Uint32(header[16+12*k.archive:]))
		var raw [12]byte
		if _, err = f.ReadAt(raw[:], offset); err != nil {
			t.Fatal(err)
		}
		base := int64(binary.BigEndian.Uint32(raw[:]))
		physical := (k.slot - (base/a.Step)%a.Count + a.Count) % a.Count
		if _, err = f.ReadAt(raw[:], offset+12*physical); err != nil {
			t.Fatal(err)
		}
		stamp := int64(binary.BigEndian.Uint32(raw[:]))
		if stamp != 0 {
			state[k] = sealrollup.Sample{Time: stamp, Bits: binary.BigEndian.Uint64(raw[4:]), Sequence: 1}
		}
	}
	return state
}

func TestClassicArchiveBoundaries(t *testing.T) {
	dir := t.TempDir()
	for _, def := range boundarySchemas {
		for method := sealrollup.Average; method <= sealrollup.First; method++ {
			for _, xff := range []float32{0, .5, math.Nextafter32(.5, 1), 1} {
				t.Run(fmt.Sprintf("%s/m%d/xff%08x", def, method, math.Float32bits(xff)), func(t *testing.T) {
					retentions := whisper.MustParseRetentionDefs(def)
					c := sealrollup.Config{Method: method, XFilesFactor: xff}
					var archives []Archive
					for _, r := range retentions {
						c.Archives = append(c.Archives, sealrollup.Archive{Step: int64(r.SecondsPerPoint()), Count: int64(r.NumberOfPoints())})
						archives = append(archives, Archive{Step: int64(r.SecondsPerPoint()), Points: int64(r.NumberOfPoints())})
					}
					clock := int64(1794700837)
					path := filepath.Join(dir, "classic.wsp")
					w, err := whisper.CreateWithOptions(path, retentions, whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := w.Close(); err != nil {
							t.Error(err)
						}
						if err := os.Remove(path); err != nil {
							t.Error(err)
						}
					}()
					candidates := map[boundarySlot]bool{}
					applyClassic := func(b sealrollup.Batch) {
						t.Helper()
						clock = b.Now
						points := make([]*whisper.TimeSeriesPoint, len(b.Points))
						for i, p := range b.Points {
							points[i] = &whisper.TimeSeriesPoint{Time: int(p.Time), Value: math.Float64frombits(p.Bits)}
							for a := range c.Archives {
								candidates[boundaryKey(a, p.Time, c)] = true
							}
						}
						if err := w.UpdateMany(points); err != nil {
							t.Fatal(err)
						}
					}
					now := clock
					var seeds, corrections []sealrollup.Sample
					// Each neighboring pair gets a bucket crossing the finer retention edge.
					// Seed half a bucket while it still belongs to that finer archive, then
					// correct both sides of the edge at the later admission clock.
					for a, r := range c.Archives {
						step := r.Step
						start := boundaryAlign(now-20*step, step)
						count := int64(8)
						seedClock := now
						if a+1 < len(c.Archives) {
							coarse := c.Archives[a+1]
							start = boundaryAlign(now-r.Step*r.Count, coarse.Step)
							count = coarse.Step / step / 2
							seedClock = now - coarse.Step
						}
						seeds = nil
						for i := int64(0); i < count; i++ {
							seeds = append(seeds, sealrollup.Sample{Time: start + i*step, Bits: math.Float64bits(float64(i%7) - 3), Sequence: uint64(i + 1)})
						}
						applyClassic(sealrollup.Batch{Now: seedClock, Points: seeds})
						for i, bits := range []uint64{0x8000000000000000, 0x7ff80000000000ab, 0, math.Float64bits(17)} {
							corrections = append(corrections, sealrollup.Sample{Time: start + int64(i)*step, Bits: bits, Sequence: uint64(len(corrections) + 1)})
						}
						// Exact admission boundary and immediately older/newer timestamps.
						for _, delta := range []int64{-1, 0, 1} {
							corrections = append(corrections, sealrollup.Sample{Time: now - step*r.Count + delta, Bits: math.Float64bits(float64(delta + 9)), Sequence: uint64(len(corrections) + 1)})
						}
					}
					if err = w.Close(); err != nil {
						t.Fatal(err)
					}
					w, err = whisper.OpenWithOptions(path, &whisper.Options{Now: func() time.Time { return time.Unix(clock, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					state := boundaryClassicSlots(t, path, c, candidates)
					// Two seals exercise original application boundaries, late replacements,
					// XFF equality and failure, occupied NaNs and signed-zero replacements.
					prepared, err := sealrollup.Prepare(c)
					if err != nil {
						t.Fatal(err)
					}
					frontier := sealrollup.Frontier{}
					var workspace sealrollup.Workspace
					for bi, points := range [][]sealrollup.Sample{corrections, {{Time: corrections[0].Time, Bits: math.Float64bits(23), Sequence: 10000}}} {
						b := sealrollup.Batch{Order: uint64(bi + 2), Now: now, Points: points}
						view := func(a int, s int64) (sealrollup.Sample, bool) { p, ok := state[boundarySlot{a, s}]; return p, ok }
						plan, err := prepared.Plan(frontier, view, nil, []sealrollup.Batch{b})
						if err != nil {
							t.Fatal(err)
						}
						ranges := func(a int, start, end int64, dst []sealrollup.Sample) []sealrollup.Sample {
							for k, p := range state {
								if k.archive == a && p.Time >= start && p.Time < end {
									dst = append(dst, p)
								}
							}
							slices.SortFunc(dst, func(a, b sealrollup.Sample) int {
								if a.Time < b.Time {
									return -1
								}
								if a.Time > b.Time {
									return 1
								}
								return 0
							})
							return dst
						}
						sparse, err := prepared.PlanWithWorkspace(frontier, view, ranges, []sealrollup.Batch{b}, &workspace)
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(plan, sparse) {
							t.Fatalf("seal%d sparse and point-view plans differ", bi)
						}
						for _, r := range sparse.Records {
							// Exercise the durable singleton codec across the seal boundary.
							decoded, err := sealrollup.DecodeRecord(r.Archive, r.Slot, sealrollup.EncodeRecord(r))
							if err != nil {
								t.Fatal(err)
							}
							state[boundarySlot{r.Archive, r.Slot}] = decoded.Sample
						}
						frontier = plan.Next
						applyClassic(b)
						want := boundaryClassicSlots(t, path, c, candidates)
						if len(want) != len(state) {
							t.Fatalf("seal%d slots got%d want%d", bi, len(state), len(want))
						}
						for k, p := range want {
							q, ok := state[k]
							if !ok || p.Time != q.Time || p.Bits != q.Bits {
								t.Fatalf("seal%d archive%d slot%d got(%d,%016x) want(%d,%016x)", bi, k.archive, k.slot, q.Time, q.Bits, p.Time, p.Bits)
							}
						}
						var records []Record
						for k, p := range state {
							records = append(records, Record{ID: 7, Step: c.Archives[k.archive].Step, Sample: Sample{Time: p.Time, Bits: p.Bits, Sequence: uint64(bi + 2)}})
						}
						source := Freeze(records)
						for _, a := range archives {
							edge := now - a.Step*a.Points
							for _, delta := range []int64{-1, 0, 1} {
								from := edge + delta
								until := from + 2*a.Step + 1
								want, err := w.Fetch(int(from), int(until))
								if err != nil {
									t.Fatal(err)
								}
								got, err := testRender(t, source, archives, from, until, now, 1<<20)
								if err != nil {
									t.Fatal(err)
								}
								if (got == nil) != (want == nil) {
									t.Fatalf("range [%d,%d] nil mismatch", from, until)
								}
								if got != nil {
									if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
										t.Fatalf("range [%d,%d] shape got(%d,%d,%d,%d) want(%d,%d,%d,%d)", from, until, got.From, got.Until, got.Step, len(got.Values), want.FromTime(), want.UntilTime(), want.Step(), len(want.Values()))
									}
									for i, v := range want.Values() {
										if math.Float64bits(got.Values[i]) != math.Float64bits(v) {
											t.Fatalf("range [%d,%d] slot%d got%016x want%016x", from, until, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
										}
									}
								}
							}
						}
					}
				})
			}
		}
	}
}

// This is the smallest cutover witness: overlaying a freshly reduced coarse
// slot cannot repair the missing finer classic inputs. The runtime must retain
// its cutover guard until an immutable baseline is installed before admission.
func TestClassicMultiarchiveNeedsBaseline(t *testing.T) {
	const now = int64(1794700800)
	const start = now - 60
	c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 60, Count: 1440}}, Method: sealrollup.Average}
	path := filepath.Join(t.TempDir(), "classic.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:120,1m:1d"), whisper.Average, 0, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(start + 1), Value: 2}, {Time: int(start + 2), Value: 4}}); err != nil {
		t.Fatal(err)
	}
	candidates := map[boundarySlot]bool{}
	for _, stamp := range []int64{start + 1, start + 2, start + 3} {
		for a := range c.Archives {
			candidates[boundaryKey(a, stamp, c)] = true
		}
	}
	baseline := boundaryClassicSlots(t, path, c, candidates)
	input := []sealrollup.Batch{{Order: 2, Now: now, Points: []sealrollup.Sample{{Time: start + 3, Bits: math.Float64bits(12), Sequence: 1}}}}
	empty, err := sealrollup.Plan(c, sealrollup.Frontier{}, nil, input)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := sealrollup.Plan(c, sealrollup.Frontier{}, func(a int, s int64) (sealrollup.Sample, bool) { p, ok := baseline[boundarySlot{a, s}]; return p, ok }, input)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(start + 3), Value: 12}}); err != nil {
		t.Fatal(err)
	}
	want := boundaryClassicSlots(t, path, c, candidates)[boundaryKey(1, start, c)]
	coarseBits := func(plan sealrollup.SealPlan) uint64 {
		t.Helper()
		for _, r := range plan.Records {
			if r.Archive == 1 && r.Sample.Time == start {
				return r.Sample.Bits
			}
		}
		t.Fatal("missing coarse result")
		return 0
	}
	if want.Bits != math.Float64bits(6) || coarseBits(seeded) != want.Bits || coarseBits(empty) != math.Float64bits(12) {
		t.Fatalf("classic=%016x seeded=%016x empty=%016x", want.Bits, coarseBits(seeded), coarseBits(empty))
	}
}
