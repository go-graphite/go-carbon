package chunkstore

import "testing"

func TestDecodeChunkReplacesDestination(t *testing.T) {
	var full, sparse, empty chunk
	for i := range full.Points {
		full.set(i, Point{Timestamp: 1790000000 + int64(i), Value: float64(i) + .1})
	}
	sparse.set(127, Point{Timestamp: -1, Value: -3})
	for _, encode := range []func(*chunk) []byte{encodeChunk, encodeExactChunk} {
		output := full
		for _, input := range []*chunk{&sparse, &empty, &full} {
			data := encode(input)
			if err := decodeChunk(data, &output); err != nil {
				t.Fatal(err)
			}
			assertChunkEqual(t, input, &output)
			if err := decodeChunk(data[:len(data)-1], &output); err == nil {
				t.Fatal("truncated chunk accepted")
			}
			assertChunkEqual(t, input, &output)
		}
	}
}

// Short-lived render workers pay stack growth that a steady decode loop hides.
func BenchmarkDecodeWorker(b *testing.B) {
	var input chunk
	for i := range input.Points {
		input.set(i, Point{Timestamp: 1790000000 + int64(i), Value: float64(i) + .1})
	}
	data := encodeExactChunk(&input)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var output chunk
		done := make(chan error)
		ran := false
		for pb.Next() {
			ran = true
			go func() { done <- decodeChunk(data, &output) }()
			if err := <-done; err != nil {
				b.Error(err)
			}
		}
		if ran && input != output {
			b.Error("decoded chunk differs from input")
		}
	})
}
