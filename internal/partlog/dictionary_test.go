package partlog

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestDictionaryEpochExactAndCompact(t *testing.T) {
	var enc DictionaryEncoder
	var dec DictionaryDecoder
	var journal [][]byte
	var total int
	const frames = 8192
	for start := 0; start < frames; start += 256 {
		fs := make([]DictionaryFrame, 256)
		for i := range fs {
			n := start + i
			fs[i] = DictionaryFrame{Key: fmt.Sprintf("namespace.long.metric.host%04d.requests.value", n%32), Definition: bytes.Repeat([]byte{byte(n % 32)}, 128), Clock: 1794700800 + int64(n/100), Points: []points.Point{{Timestamp: 1794700770 + int64(n%9), Value: math.Float64frombits([]uint64{0x8000000000000000, 0x7ff8000000000042, 1, 0x7ff0000000000000, 0xfff0000000000000}[n%5])}}}
		}
		b, e := enc.Encode(fs)
		if e != nil {
			t.Fatal(e)
		}
		journal = append(journal, b)
		total += len(b) + headerSize
		got, e := dec.Decode(b)
		if e != nil {
			t.Fatal(e)
		}
		for i, f := range fs {
			g := got[i]
			if g.Key != f.Key || !bytes.Equal(g.Definition, f.Definition) || g.Clock != f.Clock || len(g.Points) != 1 || g.Points[0].Timestamp != f.Points[0].Timestamp || math.Float64bits(g.Points[0].Value) != math.Float64bits(f.Points[0].Value) {
				t.Fatalf("frame %d mismatch", start+i)
			}
		}
	}
	if rate := float64(total) / frames; rate > 20 {
		t.Fatalf("%.3f B/pt exceeds20", rate)
	} else {
		t.Logf("JOURNAL_COMPACT points=%d bytes=%d bytes_per_point=%.6f exact=true", frames, total, rate)
	}
	// Rebuild only from persisted bytes, then accept a fresh process epoch.
	dec = DictionaryDecoder{}
	for _, b := range journal {
		if _, e := dec.Decode(b); e != nil {
			t.Fatal(e)
		}
	}
	fresh := DictionaryEncoder{}
	b, e := fresh.Encode([]DictionaryFrame{{Key: "other", Definition: []byte("policy"), Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700800, Value: 42}}}})
	if e != nil {
		t.Fatal(e)
	}
	if got, e := dec.Decode(b); e != nil || got[0].Key != "other" {
		t.Fatalf("epoch reset: %v", e)
	}
}

func TestDictionaryRejectsTruncationAndMissingEpoch(t *testing.T) {
	var enc DictionaryEncoder
	f := []DictionaryFrame{{Key: "metric", Definition: []byte("policy"), Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700799, Value: 1}}}}
	b, e := enc.Encode(f)
	if e != nil {
		t.Fatal(e)
	}
	for i := range b {
		var dec DictionaryDecoder
		if _, e := dec.Decode(b[:i]); e == nil {
			t.Fatalf("accepted truncated %d", i)
		}
	}
	b, e = enc.Encode(f)
	if e != nil {
		t.Fatal(e)
	}
	var dec DictionaryDecoder
	if _, e := dec.Decode(b); e == nil {
		t.Fatal("accepted reference without epoch")
	}
	f[0].Definition = []byte("changed")
	if _, e := enc.Encode(f); e == nil {
		t.Fatal("accepted policy mutation")
	}
}

func FuzzDictionaryDecode(f *testing.F) {
	var enc DictionaryEncoder
	b, _ := enc.Encode([]DictionaryFrame{{Key: "a", Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700800, Value: 1}}}})
	f.Add(b)
	f.Add([]byte("PDC\x01\x00"))
	f.Fuzz(func(t *testing.T, b []byte) { var dec DictionaryDecoder; _, _ = dec.Decode(b) })
}
