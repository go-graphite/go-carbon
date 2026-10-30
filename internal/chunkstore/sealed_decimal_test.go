package chunkstore

import (
	"math"
	"testing"
)

func TestSealedDecimalBound(t *testing.T) {
	for _, size := range []int{1, 128, 4096, 8192} {
		for _, kind := range []string{"decimal", "linear", "gaps", "large", "dynamic", "special"} {
			points := make([]Point, size)
			for i := range points {
				v := 1000 + math.Sin(float64(i))*0.0123456789012345
				switch kind {
				case "linear":
					v = 1.23456789012345 + float64(i)*0.0001
				case "large":
					v = 1.8446744073709552e19 + float64(i)*10000
				case "dynamic":
					if i%2 == 0 {
						v = 1e-200
					} else {
						v = 1e200
					}
				case "special":
					v = math.Float64frombits([]uint64{0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000123, 1}[i%5])
				}
				points[i] = Point{Timestamp: 1700000000 + int64(i*60), Value: v}
				if kind == "gaps" {
					points[i].Timestamp += int64(i / 3 * 60)
				}
			}
			exact, err := EncodeSealedPoints(points, 60)
			if err != nil {
				t.Fatal(err)
			}
			data, err := EncodeSealedPointsDecimal(points, 60)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > len(exact) {
				t.Fatal("adaptive encoding grew")
			}
			got, err := DecodeSealedPoints(data)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range points {
				q := got[i]
				if p.Timestamp != q.Timestamp {
					t.Fatal("timestamps changed")
				}
				if kind == "special" || kind == "dynamic" && size > 1 {
					if math.Float64bits(p.Value) != math.Float64bits(q.Value) {
						t.Fatal("fallback changed bits")
					}
					continue
				}
				if math.Abs((q.Value-p.Value)/p.Value) > decimalRelativeError {
					t.Fatalf("%s/%d relative error at %d", kind, size, i)
				}
			}
			// Selected mantissas must not accumulate drift when a cold block is thawed
			// and sealed again. Check the original bound after ten cycles.
			if kind != "special" && kind != "dynamic" {
				for n := 0; n < 10; n++ {
					data, err = EncodeSealedPointsDecimal(got, 60)
					if err != nil {
						t.Fatal(err)
					}
					got, err = DecodeSealedPoints(data)
					if err != nil {
						t.Fatal(err)
					}
				}
				for i, p := range points {
					if math.Abs((got[i].Value-p.Value)/p.Value) > decimalRelativeError {
						t.Fatal("repeated encoding exceeded original bound")
					}
				}
			}
			for i := 0; i < len(data); i += max(1, len(data)/17) {
				data[i] ^= 0x80
				if corrupt, err := DecodeSealedPoints(data); err == nil {
					if len(corrupt) != len(got) {
						t.Fatal("corruption changed count")
					}
					for j, p := range got {
						if p.Timestamp != corrupt[j].Timestamp || math.Float64bits(p.Value) != math.Float64bits(corrupt[j].Value) {
							t.Fatal("corruption changed points")
						}
					}
				}
				data[i] ^= 0x80
			}
		}
	}
}
