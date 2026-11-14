package inputlog

import (
	"hash/crc32"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

func TestArrivalMetadataIsReadableByLegacyStreamDecoder(t *testing.T) {
	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	var s streamEncoder
	var w streamScratch
	s.begin(1)
	ref := NewStreamRef("exact.bits")
	ref.Arrival.Store(1790000000123456789)
	s.add(&w, ref.Entry(math.Float64frombits(0x7ff8000000000042), -9))
	s.add(&w, ref.Entry(math.Copysign(0, -1), 7))
	s.flush(&w, nil, func(raw []byte, n int) []byte {
		payload := appendArrivalFrame(enc.EncodeAll(raw, nil), s.frameAges)
		f := replayFrame{payload: payload, seq: 1, apply: true, h: frameHeader{codec: codecStream, rawLength: uint32(len(raw)), crc: crc32.Checksum(payload, castagnoli)}}
		plain, err := decompress(dec, f, nil)
		if err != nil {
			t.Fatal(err)
		}
		var legacy streamDecoder
		legacy.begin(1)
		var old, new []*points.Points
		if _, err := legacy.decode(plain, func(p *points.Points) { old = append(old, p) }); err != nil {
			t.Fatal("legacy reader rejected skippable metadata", err)
		}
		var next streamDecoder
		if _, _, err := decodeFrame(dec, f, nil, &next, func(p *points.Points) { new = append(new, p) }); err != nil {
			t.Fatal(err)
		}
		if len(old) != 1 || len(new) != 1 || new[0].FirstArrival != ref.Arrival.Load() {
			t.Fatal(old, new)
		}
		for i, p := range old[0].Data {
			q := new[0].Data[i]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				t.Fatal("point bits changed")
			}
		}
		return nil
	})
}
