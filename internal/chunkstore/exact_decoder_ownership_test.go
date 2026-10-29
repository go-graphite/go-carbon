package chunkstore

import (
	"bytes"
	"hash/crc32"
	"math"
	"sync"
	"testing"
)

func TestExactChunkChecksumMatchesHeaderAndBody(t *testing.T) {
	for _, flags := range []byte{0, exactChunkInteger, exactChunkZstd, exactChunkInteger | exactChunkZstd} {
		for _, body := range [][]byte{nil, {0, 1, 255}, bytes.Repeat([]byte{0, 255, 128, 127}, 300)} {
			stream := append([]byte{exactChunkVersion, flags & exactChunkInteger}, body...)
			if got, want := exactChunkChecksum(flags, body), crc32.ChecksumIEEE(stream); got != want {
				t.Fatalf("flags=%d length=%d: got=%x want=%x", flags, len(body), got, want)
			}
		}
	}
}

func TestExactDecoderOwnsRetainedOutput(t *testing.T) {
	cases := make([]chunk, 3)
	cases[0].set(5, Point{Timestamp: 1700000000, Value: 2})
	bits := uint64(42)
	for slot := range chunkSlots {
		cases[1].set(slot, Point{Timestamp: 1700000000 + int64(slot), Value: float64(slot % 7)})
		bits ^= bits << 13
		bits ^= bits >> 7
		bits ^= bits << 17
		cases[2].set(slot, Point{Timestamp: int64(bits), Value: math.Float64frombits(bits)})
	}
	cases[2].Points[0].Value = math.Float64frombits(0x8000000000000000)
	cases[2].Points[1].Value = math.Float64frombits(0x7ff8000000000042)
	encoded := make([][]byte, len(cases))
	original := make([][]byte, len(cases))
	retained := make([]chunk, len(cases))
	var compressed, raw bool
	for i := range cases {
		encoded[i] = encodeExactChunk(&cases[i])
		original[i] = bytes.Clone(encoded[i])
		compressed = compressed || encoded[i][1]&exactChunkZstd != 0
		raw = raw || encoded[i][1]&exactChunkZstd == 0
		if err := decodeChunk(encoded[i], &retained[i]); err != nil {
			t.Fatal(err)
		}
	}
	if !compressed || !raw {
		t.Fatal("fixture must cover both compressed and raw exact blocks")
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			outputs := make([]chunk, 96)
			for i := range outputs {
				if err := decodeChunk(encoded[i%len(cases)], &outputs[i]); err != nil {
					t.Error(err)
					return
				}
			}
			for i := range outputs {
				assertChunkEqual(t, &cases[i%len(cases)], &outputs[i])
			}
		})
	}
	workers.Wait()
	for i := range cases {
		assertChunkEqual(t, &cases[i], &retained[i])
		if !bytes.Equal(encoded[i], original[i]) {
			t.Fatal("decoding modified the caller's encoded input")
		}
		clear(encoded[i])
		assertChunkEqual(t, &cases[i], &retained[i])
	}
}
