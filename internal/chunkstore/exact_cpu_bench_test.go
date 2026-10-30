package chunkstore

import (
	"crypto/sha256"
	"fmt"
	"math"
	"testing"
)

func BenchmarkExactChunkCodec(b *testing.B) {
	for _, count := range []int{32, 128} {
		for _, integer := range []bool{true, false} {
			var input chunk
			for i := range count {
				value := float64(i)
				if !integer {
					value = math.Sin(float64(i))
				}
				input.set(i, Point{Timestamp: 1792400400 + int64(i)*60, Value: value})
			}
			encoded := encodeExactChunk(&input)
			b.Run(fmt.Sprintf("points=%d/integer=%v", count, integer), func(b *testing.B) {
				b.Run("encode", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if got := encodeExactChunk(&input); len(got) != len(encoded) {
							b.Fatal("encoded length changed")
						}
					}
					b.ReportMetric(float64(len(encoded)), "encoded-B/op")
				})
				b.Run("decode", func(b *testing.B) {
					var output chunk
					b.ReportAllocs()
					for b.Loop() {
						if err := decodeChunk(encoded, &output); err != nil {
							b.Fatal(err)
						}
					}
					if input != output {
						b.Fatal("decoded chunk differs")
					}
				})
			})
		}
	}
}

// A result must not borrow either serialization buffer from the pool.
func TestExactChunkEncodingOwnsScratch(t *testing.T) {
	var input, output chunk
	for i := range input.Points {
		input.set(i, Point{Timestamp: 1792400400 + int64(i)*60, Value: math.Sin(float64(i))})
	}
	encoded := encodeExactChunk(&input)
	for round := range 64 {
		var other chunk
		other.set(round, Point{Timestamp: int64(round), Value: float64(round)})
		encodeExactChunk(&other)
	}
	if err := decodeChunk(encoded, &output); err != nil {
		t.Fatal(err)
	}
	assertChunkEqual(t, &input, &output)
}

func TestExactChunkEncodingStable(t *testing.T) {
	h := sha256.New()
	values := []uint64{0, math.Float64bits(.1), 0x8000000000000000, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, math.Float64bits(1 << 53)}
	for count := 0; count <= chunkSlots; count++ {
		var input chunk
		for i := range count {
			input.set(i*73%chunkSlots, Point{Timestamp: 1792400400 + int64(i)*60, Value: math.Float64frombits(values[i%len(values)])})
		}
		h.Write(encodeExactChunk(&input))
	}
	// Recorded from de5d6c44 with the same vendored Zstd encoder.
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != "e84eafa740609efeca214683b395580bab0b5e85b90ad6e02b7501ef85dcae73" {
		t.Fatalf("persistent encoding changed: %s", got)
	}
}
