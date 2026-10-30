package chunkstore

import (
	"math"
	"testing"
)

func TestSealedPointCodec(t *testing.T) {
	for _, size := range []int{1, 127, 1024, MaxSealedPoints} {
		for _, kind := range []string{"constant", "counter", "float", "special", "gaps"} {
			points := make([]Point, size)
			for i := range points {
				points[i] = Point{Timestamp: 1700000000 + int64(i*60), Value: 7}
				switch kind {
				case "counter":
					points[i].Value = float64(i * 123)
				case "float":
					points[i].Value = math.Sin(float64(i))
				case "special":
					points[i].Value = math.Float64frombits([]uint64{0x8000000000000000, 0x7ff0000000000000, 0x7ff8000000000123, 1}[i%4])
				case "gaps":
					points[i].Timestamp += int64(i / 3 * 60)
				}
			}
			data, err := EncodeSealedPoints(points, 60)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeSealedPoints(data)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range points {
				if p.Timestamp != got[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(got[i].Value) {
					t.Fatalf("%s/%d point %d differs", kind, size, i)
				}
			}
			for i := 0; i < len(data); i += max(1, len(data)/24) {
				data[i] ^= 0x80
				if changed, err := DecodeSealedPoints(data); err == nil {
					for j, p := range points {
						if p.Timestamp != changed[j].Timestamp || math.Float64bits(p.Value) != math.Float64bits(changed[j].Value) {
							t.Fatalf("corruption changed point %d at byte %d", j, i)
						}
					}
				}
				data[i] ^= 0x80
			}
		}
	}
}

func TestSealedPointBounds(t *testing.T) {
	for _, points := range [][]Point{nil, make([]Point, MaxSealedPoints+1), {{Timestamp: 0}}, {{Timestamp: 1}, {Timestamp: 1}}, {{Timestamp: math.MaxUint32 + 1}}} {
		if _, err := EncodeSealedPoints(points, 60); err == nil {
			t.Fatal("invalid points accepted")
		}
	}
	for n := 0; n < sealedHeader; n++ {
		if _, err := DecodeSealedPoints(make([]byte, n)); err == nil {
			t.Fatal("short header accepted")
		}
	}
}

func BenchmarkSealedDecode(b *testing.B) {
	for _, kind := range []string{"constant", "float"} {
		b.Run(kind, func(b *testing.B) {
			points := make([]Point, 4096)
			for i := range points {
				v := 1.
				if kind == "float" {
					v = math.Sin(float64(i))
				}
				points[i] = Point{Timestamp: 1700000000 + int64(i*60), Value: v}
			}
			data, err := EncodeSealedPoints(points, 60)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := DecodeSealedPoints(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
