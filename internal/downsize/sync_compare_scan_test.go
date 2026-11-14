package downsize

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestSyncCompareScanOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	vals := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, 42}
	for trial := 0; trial < 10000; trial++ {
		step := int64(1 + rng.IntN(30))
		from := int64(100 + rng.IntN(70))
		until := from + int64(rng.IntN(100))
		makeSeries := func() syncSeries {
			s := syncSeries{Start: int64(80 + rng.IntN(100)), Step: step, Values: make([]float64, rng.IntN(80))}
			if trial%3 == 0 {
				s.Start = from - int64(rng.IntN(8))*step
			}
			if trial%11 == 0 {
				s.Step++
			}
			for i := range s.Values {
				s.Values[i] = vals[rng.IntN(len(vals))]
			}
			return s
		}
		src, dst := makeSeries(), makeSeries()
		want := syncHole{Name: "metric", Source: "source", Step: step}
		for ts := from; ts < until; ts += step {
			if src.finite(ts) && !dst.finite(ts) {
				if want.Points == 0 {
					want.First = ts
				}
				want.Last = ts
				want.Points++
			}
		}
		got, found := syncCompare("metric", "source", src, dst, step, from, until)
		if !reflect.DeepEqual(got, want) || found != (want.Points > 0) {
			t.Fatal(fmt.Sprintf("trial%d: got%+v want%+v", trial, got, want))
		}
	}
}
