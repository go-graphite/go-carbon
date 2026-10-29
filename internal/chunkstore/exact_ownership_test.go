package chunkstore

import (
	"bytes"
	"math"
	"sync"
	"testing"
)

func TestExactEncoderOwnsRetainedOutput(t *testing.T) {
	var sparse, integer, floating chunk
	sparse.set(5, Point{Timestamp: 1700000000, Value: 2})
	bits := uint64(42)
	for slot := range chunkSlots {
		integer.set(slot, Point{Timestamp: 1700000000 + int64(slot)*60, Value: float64(slot % 7)})
		bits ^= bits << 13
		bits ^= bits >> 7
		bits ^= bits << 17
		floating.set(slot, Point{Timestamp: int64(bits), Value: math.Float64frombits(bits)})
	}
	floating.Points[0].Value = math.Float64frombits(0x8000000000000000)
	floating.Points[1].Value = math.Float64frombits(0x7ff8000000000042)
	cases := []chunk{sparse, integer, floating}
	retained := make([][]byte, len(cases))
	want := make([][]byte, len(cases))
	for i := range cases {
		retained[i] = encodeExactChunk(&cases[i])
		want[i] = bytes.Clone(retained[i])
	}

	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			outputs := make([][]byte, 96)
			for i := range outputs {
				outputs[i] = encodeExactChunk(&cases[i%len(cases)])
			}
			for i, encoded := range outputs {
				index := i % len(cases)
				if !bytes.Equal(encoded, want[index]) {
					t.Error("encoded result changed after subsequent encodes")
					return
				}
				var decoded chunk
				if err := decodeChunk(encoded, &decoded); err != nil {
					t.Error(err)
					return
				}
				assertChunkEqual(t, &cases[index], &decoded)
			}
		})
	}
	workers.Wait()
	for i := range retained {
		if !bytes.Equal(retained[i], want[i]) {
			t.Fatal("retained result aliases encoder scratch")
		}
	}
}
