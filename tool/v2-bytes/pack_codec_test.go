package main

import (
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"math"
	"testing"
)

func TestPackedArchiveFallbackPreservesOccupiedImage(t *testing.T) {
	for _, ps := range [][]chunkstore.Point{
		{{Timestamp: 1700000000, Value: 1}, {Timestamp: 1700000000, Value: math.Copysign(0, -1)}},
		{{Timestamp: -1, Value: math.Float64frombits(0x7ff8000000001234)}},
	} {
		b, e := EncodeBlock(ps, 60)
		if e != nil {
			t.Fatal(e)
		}
		if b[0] != 0 {
			t.Fatal("expected fallback")
		}
		q, e := DecodeBlock(b)
		if e != nil || len(q) != len(ps) {
			t.Fatal(e)
		}
		for i := range ps {
			if q[i].Timestamp != ps[i].Timestamp || math.Float64bits(q[i].Value) != math.Float64bits(ps[i].Value) {
				t.Fatal("occupied image changed")
			}
		}
		b[len(b)-1] ^= 1
		if _, e := DecodeBlock(b); e == nil {
			t.Fatal("missing checksum rejection")
		}
	}
}
