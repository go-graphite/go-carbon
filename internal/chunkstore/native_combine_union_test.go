package chunkstore

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
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
		}
	}
}
