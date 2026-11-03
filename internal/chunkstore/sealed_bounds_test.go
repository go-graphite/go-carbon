package chunkstore

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestSealedTimeBounds(t *testing.T) {
	for _, gap := range []bool{false, true} {
		points := []Point{{Timestamp: 1700000001, Value: 1}, {Timestamp: 1700000061, Value: 2}}
		if gap {
			points[1].Timestamp++
		}
		data, err := EncodeSealedPoints(points, 60)
		if err != nil {
			t.Fatal(err)
		}
		first, last, n, err := SealedTimeBounds(data)
		if err != nil || n != len(points) || first != points[0].Timestamp || last < points[1].Timestamp {
			t.Fatalf("bounds: %d %d %d %v", first, last, n, err)
		}
		if gap && last != math.MaxUint32 || !gap && last != points[1].Timestamp {
			t.Fatal("incorrect upper bound")
		}
		for _, change := range []string{"short", "count", "step", "first", "overflow", "flags"} {
			malformed := append([]byte(nil), data...)
			switch change {
			case "short":
				malformed = malformed[:15]
			case "count":
				binary.LittleEndian.PutUint16(malformed[2:4], 0)
			case "step":
				binary.LittleEndian.PutUint32(malformed[8:12], 0)
			case "first":
				binary.LittleEndian.PutUint32(malformed[4:8], 0)
			case "overflow":
				malformed[1] |= sealedImplicit
				binary.LittleEndian.PutUint32(malformed[4:8], math.MaxUint32)
			case "flags":
				malformed[1] |= 128
			}
			if _, _, _, err := SealedTimeBounds(malformed); err == nil {
				t.Fatalf("%s accepted", change)
			}
		}
	}
}
