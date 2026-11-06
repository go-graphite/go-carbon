package chunkstore

import (
	"math"
	"testing"
)

func TestCanonicalSealedRunAgainstDecoder(t *testing.T) {
	for _, count := range []int{4, 128, 1024, 8192} {
		for _, delta := range []int64{0, 1, -3} {
			points := make([]Point, count)
			for i := range points {
				points[i] = Point{Timestamp: 1790000040 + int64(i)*60, Value: float64(40000 + int64(i)*delta)}
			}
			data, err := EncodeSealedPoints(points, 60)
			if err != nil {
				t.Fatal(err)
			}
			first, last, n, ok := CanonicalSealedRun(data, 60)
			if !ok || first != points[0].Timestamp || last != points[count-1].Timestamp || n != count {
				t.Fatalf("run rejected: %d %d %x", count, delta, data)
			}
			got, err := DecodeSealedPoints(data)
			if err != nil || len(got) != count {
				t.Fatal(err)
			}
			for i, p := range got {
				if p != points[i] {
					t.Fatal(i, p, points[i])
				}
			}
			bad := append([]byte(nil), data...)
			bad[len(bad)-1] ^= 1
			if _, _, _, ok := CanonicalSealedRun(bad, 60); ok {
				t.Fatal("corrupt run accepted")
			}
			if _, _, _, ok := CanonicalSealedRun(data, 61); ok {
				t.Fatal("wrong grid accepted")
			}
		}
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Copysign(0, -1), .1} {
		points := make([]Point, 128)
		for i := range points {
			points[i] = Point{Timestamp: 1790000040 + int64(i)*60, Value: value}
		}
		data, err := EncodeSealedPoints(points, 60)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, ok := CanonicalSealedRun(data, 60); ok {
			t.Fatal("unproved scalar encoding accepted", value)
		}
	}
}
