package chunkstore

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeArchiveUnionDifferential(t *testing.T) {
	random := rand.New(rand.NewPCG(42, 17))
	values := []float64{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000042), math.Float64frombits(0x7ff0000000000043), 1.5, -17}
	for trial := range 512 {
		r := Retention{Step: 1 + random.IntN(120), Count: 1 + random.IntN(1000)}
		makePoints := func() []Point {
			p := make([]Point, random.IntN(1024))
			for i := range p {
				p[i] = Point{Timestamp: 1 + int64(random.IntN(100000)), Value: values[random.IntN(len(values))]}
			}
			slices.SortStableFunc(p, func(a, b Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
			return p
		}
		a, b := makePoints(), makePoints()
		want, err := fillArchive(a, b, r.Step)
		if err != nil {
			t.Fatal(err)
		}
		want = retainTransferSlots(want, r)
		// Frozen pre-repair oracle: every finite winner must retain all 64 bits.
		legacy := map[int64]float64{}
		for _, input := range [][]Point{a, b} {
			for _, p := range input {
				if !math.IsNaN(p.Value) {
					legacy[p.Timestamp-p.Timestamp%int64(r.Step)] = p.Value
				}
			}
		}
		got, _, _, err := nativeArchiveUnion(Archive{r, a}, Archive{r, b})
		if err != nil {
			t.Fatal(err)
		}
		got = retainTransferSlots(got, r)
		if len(want) != len(got) {
			t.Fatalf("trial%d lengths %d/%d", trial, len(want), len(got))
		}
		for i := range want {
			if want[i].Timestamp != got[i].Timestamp || math.Float64bits(want[i].Value) != math.Float64bits(got[i].Value) {
				t.Fatalf("trial%d point%d changed: %+v / %+v", trial, i, want[i], got[i])
			}
			if old := legacy[got[i].Timestamp]; !math.IsNaN(old) && !math.IsInf(old, 0) && math.Float64bits(old) != math.Float64bits(got[i].Value) {
				t.Fatalf("trial%d finite oracle value changed at %d", trial, got[i].Timestamp)
			}
		}
	}
}

func TestNativeArchiveUnionNonFinite(t *testing.T) {
	for _, tc := range []struct {
		name                string
		donor, target, want float64
	}{
		{"positive", 286.73400000000004, math.Inf(1), 286.73400000000004},
		{"negative", 989.205, math.Inf(-1), 989.205},
		{"finite_target", 32, 42, 42},
		{"signed_zero", 0, math.Copysign(0, -1), math.Copysign(0, -1)},
		{"infinite_donor", math.Inf(1), 32, 32},
		{"both_infinite", math.Inf(1), math.Inf(-1), math.Inf(-1)},
		{"both_positive", math.Inf(1), math.Inf(1), math.Inf(1)},
		{"nan_donor", math.NaN(), math.Inf(1), math.Inf(1)},
		{"nan_target", 32, math.NaN(), 32},
		{"both_nan", math.NaN(), math.NaN(), math.NaN()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := Retention{Step: 60, Count: 100}
			a, b := []Point{{Timestamp: 600, Value: tc.donor}}, []Point{{Timestamp: 600, Value: tc.target}}
			got, _, changed, err := nativeArchiveUnion(Archive{r, a}, Archive{r, b})
			if err != nil {
				t.Fatal(err)
			}
			if math.IsNaN(tc.want) {
				if len(got) != 0 {
					t.Fatal("NaN normalization changed", got)
				}
				return
			}
			if len(got) != 1 || got[0].Timestamp != 600 || math.Float64bits(got[0].Value) != math.Float64bits(tc.want) {
				t.Fatalf("got %+v, want %v", got, tc.want)
			}
			if math.IsInf(tc.target, 0) && !math.IsInf(tc.donor, 0) && !math.IsNaN(tc.donor) && !changed {
				t.Fatal("repair must disable target-buffer append")
			}
		})
	}
}

func TestFillDuplicatesNormalizeBeforeRepair(t *testing.T) {
	r := Retention{Step: 60, Count: 100}
	a := []Point{{Timestamp: 600, Value: 32}}
	b := []Point{{Timestamp: 600, Value: 42}, {Timestamp: 601, Value: math.Inf(1)}}
	want, err := fillArchive(a, b, r.Step)
	if err != nil {
		t.Fatal(err)
	}
	got, _, _, err := nativeArchiveUnion(Archive{r, a}, Archive{r, b})
	if err != nil || len(got) != 1 || len(want) != 1 || got[0].Value != 32 || want[0].Value != 32 {
		t.Fatal("last target duplicate must be repaired from donor", got, want, err)
	}
}

// Pebble uses this merger for reads and compaction. The explicit fill policy
// must never let older finite history overwrite a later live Inf mutation.
func TestChunkMergeKeepsLiveInfinity(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, value := range []float64{math.Inf(1), math.Inf(-1)} {
			older := delta(5, Point{Timestamp: 600, Value: 32})
			live := delta(5, Point{Timestamp: 600, Value: value})
			for _, olderFirst := range []bool{false, true} {
				first := &live
				if olderFirst {
					first = &older
				}
				m, err := newChunkMerger(exact).Merge(nil, encodeChunk(first))
				if err != nil {
					t.Fatal(err)
				}
				if olderFirst {
					err = m.MergeNewer(encodeChunk(&live))
				} else {
					err = m.MergeOlder(encodeChunk(&older))
				}
				if err != nil {
					t.Fatal(err)
				}
				got := finish(t, m)
				if math.Float64bits(got.Points[5].Value) != math.Float64bits(value) {
					t.Fatal("live infinity was overwritten", got.Points[5])
				}
			}
		}
	}
}

func TestRollupEmptyWindowWithZeroXFF(t *testing.T) {
	s, _ := openTestStore(t, "/empty", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	m := Metadata{ID: 1, Generation: 1, MetricConfig: MetricConfig{Retentions: []Retention{{Step: 60, Count: 60}, {Step: 1800, Count: 120}}, AggregationMethod: Average, XFilesFactor: 0}}
	w := newChunkWriter(s, m)
	defer w.release()
	if wrote, err := rollup(w, m, 1, 1800); err != nil || wrote || w.changed() {
		t.Fatal("empty window must not produce a coarse value", wrote, err)
	}
}
