package partspack

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

var benchBytes []byte
var benchPoints []chunkstore.Point

func BenchmarkSeal(b *testing.B) {
	for _, n := range []int{144, 1440, 4096} {
		ps := make([]chunkstore.Point, n)
		r := rand.New(rand.NewPCG(27, 39))
		for i := range ps {
			ps[i] = chunkstore.Point{Timestamp: 1700000000 + int64(i)*60, Value: math.Float64frombits(r.Uint64())}
		}
		for _, kind := range []string{"coldpack", "partspack"} {
			b.Run(fmt.Sprintf("%d/%s", n, kind), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(n) * 16)
				for i := 0; i < b.N; i++ {
					var err error
					if kind == "coldpack" {
						benchBytes, err = chunkstore.EncodeSealedPoints(ps, 60)
					} else {
						benchBytes, err = EncodeBlock(ps, 60)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
func BenchmarkDecode(b *testing.B) {
	ps := make([]chunkstore.Point, 4096)
	for i := range ps {
		ps[i] = chunkstore.Point{Timestamp: 1700000000 + int64(i)*60, Value: float64(i % 97)}
	}
	encoded, err := EncodeBlock(ps, 60)
	if err != nil {
		b.Fatal(err)
	}
	for _, kind := range []string{"coldpack", "partspack"} {
		b.Run(kind, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(4096 * 16)
			for i := 0; i < b.N; i++ {
				var err error
				if kind == "coldpack" {
					benchPoints, err = chunkstore.DecodeSealedPoints(encoded)
				} else {
					benchPoints, err = DecodeBlock(encoded)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
