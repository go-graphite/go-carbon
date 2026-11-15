package partlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestDictionaryBlocksExactAndCompact(t *testing.T) {
	for _, density := range []int{118, 513} {
		t.Run(fmt.Sprint(density), func(t *testing.T) {
			var enc DictionaryBlockEncoder
			defer enc.Close()
			var dec DictionaryBlockDecoder
			defer dec.Close()
			const metrics = 32
			var total, count int
			var journal [][]byte
			for start := 0; start < density; start += 8 {
				var frames []DictionaryFrame
				for offset := 0; offset < 8 && start+offset < density; offset++ {
					for metric := range metrics {
						n := start + offset
						bits := []uint64{0x8000000000000000, 0x7ff8000000000042, 1, 0x7ff0000000000000, 0xfff0000000000000, math.Float64bits(float64(n) + .125)}[n%6]
						frames = append(frames, DictionaryFrame{Key: fmt.Sprintf("namespace.long.metric.host%04d.requests.value", metric), Definition: bytes.Repeat([]byte{byte(metric)}, 128), Clock: 1794700800 + int64(n), Points: []points.Point{{Timestamp: 1794700770 + int64(n), Value: math.Float64frombits(bits)}}})
					}
				}
				b, err := enc.Encode(frames)
				if err != nil {
					t.Fatal(err)
				}
				journal = append(journal, b)
				total += len(b) + headerSize
				count += len(frames)
				got, err := dec.Decode(b)
				if err != nil || len(got) != len(frames) {
					t.Fatalf("decode count: %d %v", len(got), err)
				}
				for i, f := range frames {
					g := got[i]
					if f.Key != g.Key || !bytes.Equal(f.Definition, g.Definition) || f.Clock != g.Clock || f.Points[0].Timestamp != g.Points[0].Timestamp || math.Float64bits(f.Points[0].Value) != math.Float64bits(g.Points[0].Value) {
						t.Fatalf("frame %d bits/metadata differ", i)
					}
				}
			}
			t.Logf("JOURNAL_BLOCK density=%d points=%d bytes=%d bytes_per_point=%.6f exact=true", density, count, total, float64(total)/float64(count))
			dec.Close()
			dec = DictionaryBlockDecoder{}
			for _, b := range journal {
				if _, err := dec.Decode(b); err != nil {
					t.Fatal(err)
				}
			}
			// A new process resets the dictionary without relying on prior blocks.
			enc.Close()
			enc = DictionaryBlockEncoder{}
			b, err := enc.Encode([]DictionaryFrame{{Key: "new.metric", Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700799, Value: 1}}}})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := dec.Decode(b); err != nil || got[0].Key != "new.metric" {
				t.Fatalf("reset: %v", err)
			}
		})
	}
}

func TestDictionaryBlockRejectsCorruptionAndExpansion(t *testing.T) {
	var enc DictionaryBlockEncoder
	defer enc.Close()
	fs := make([]DictionaryFrame, 256)
	for i := range fs {
		fs[i] = DictionaryFrame{Key: "metric", Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700799, Value: 1}}}
	}
	b, err := enc.Encode(fs)
	if err != nil || !bytes.HasPrefix(b, dictionaryBlockMagic) {
		t.Fatalf("compressed fixture: %v", err)
	}
	for i := range b {
		var dec DictionaryBlockDecoder
		_, err := dec.Decode(b[:i])
		dec.Close()
		if err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	bad := bytes.Clone(b)
	bad[len(bad)-1] ^= 1
	var dec DictionaryBlockDecoder
	defer dec.Close()
	if _, err = dec.Decode(bad); err == nil {
		t.Fatal("accepted checksum corruption")
	}
	bad = binary.AppendUvarint(append([]byte(nil), dictionaryBlockMagic...), maxPayload+1)
	if _, err = dec.Decode(bad); err == nil {
		t.Fatal("accepted expansion beyond journal limit")
	}
}

// Compression targets depend on entropy. Keep an exact high-entropy receipt so
// the structured-density numbers cannot be mistaken for a universal byte bound.
func TestDictionaryBlockHighEntropyReceipt(t *testing.T) {
	var enc DictionaryBlockEncoder
	defer enc.Close()
	var dec DictionaryBlockDecoder
	defer dec.Close()
	var total int
	bits := uint64(0x0123456789abcdef)
	const count = 4096
	for start := 0; start < count; start += 256 {
		fs := make([]DictionaryFrame, 256)
		for i := range fs {
			bits ^= bits << 13
			bits ^= bits >> 7
			bits ^= bits << 17
			fs[i] = DictionaryFrame{Key: fmt.Sprintf("metric.%d", i%32), Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700770 + int64((start+i)/32), Value: math.Float64frombits(bits)}}}
		}
		b, err := enc.Encode(fs)
		if err != nil {
			t.Fatal(err)
		}
		total += len(b) + headerSize
		got, err := dec.Decode(b)
		if err != nil || len(got) != len(fs) {
			t.Fatalf("decode: %v", err)
		}
		for i := range fs {
			if math.Float64bits(got[i].Points[0].Value) != math.Float64bits(fs[i].Points[0].Value) || got[i].Points[0].Timestamp != fs[i].Points[0].Timestamp || got[i].Key != fs[i].Key {
				t.Fatal("high-entropy receipt differs")
			}
		}
	}
	t.Logf("JOURNAL_BLOCK high_entropy=true points=%d bytes=%d bytes_per_point=%.6f exact=true", count, total, float64(total)/count)
}

func FuzzDictionaryBlockDecode(f *testing.F) {
	var enc DictionaryBlockEncoder
	frames := make([]DictionaryFrame, 256)
	for i := range frames {
		frames[i] = DictionaryFrame{Key: "metric", Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700799, Value: 1}}}
	}
	b, _ := enc.Encode(frames)
	enc.Close()
	f.Add(b)
	f.Add([]byte{'P', 'D', 'C', 2, 1})
	f.Fuzz(func(t *testing.T, b []byte) {
		var dec DictionaryBlockDecoder
		defer dec.Close()
		_, _ = dec.Decode(b)
	})
}
