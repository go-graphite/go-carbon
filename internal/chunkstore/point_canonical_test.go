package chunkstore

import (
	"math"
	"testing"
)

func TestCanonicalPointRunAgainstDecoder(t *testing.T) {
	for _, count := range []int{1, 63, 64, 65, 128} {
		for _, value := range []float64{0, 123, .123, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, math.NaN(), math.Inf(1)} {
			var block PointBlock
			for i := 0; i < count; i++ {
				block.Present[i/64] |= 1 << uint(i%64)
				block.Points[i] = Point{Timestamp: 1790000040 + int64(i)*60, Value: value}
			}
			data := EncodePointBlock(&block)
			first, last, n, ok := CanonicalPointRun(data, 60)
			want := !math.IsNaN(value) && !math.IsInf(value, 0)
			if ok != want || ok && (first != block.Points[0].Timestamp || last != block.Points[count-1].Timestamp || n != count) {
				t.Fatal(count, value, first, last, n, ok)
			}
			if ok {
				var got PointBlock
				if err := DecodePointBlock(data, &got); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < count; i++ {
					if got.Points[i].Timestamp != block.Points[i].Timestamp || math.Float64bits(got.Points[i].Value) != math.Float64bits(value) {
						t.Fatal(i)
					}
				}
			}
			data[len(data)-1] ^= 1
			if _, _, _, ok := CanonicalPointRun(data, 60); ok {
				t.Fatal("corrupt")
			}
		}
	}
	var block PointBlock
	block.Present[0] = 5
	block.Points[0] = Point{Timestamp: 1790000040, Value: 1}
	block.Points[2] = Point{Timestamp: 1790000160, Value: 2}
	if _, _, _, ok := CanonicalPointRun(EncodePointBlock(&block), 60); ok {
		t.Fatal("sparse")
	}
	block.Present[0] = 3
	block.Points[1] = Point{Timestamp: 1790000101, Value: 2}
	if _, _, _, ok := CanonicalPointRun(EncodePointBlock(&block), 60); ok {
		t.Fatal("off grid")
	}
}
