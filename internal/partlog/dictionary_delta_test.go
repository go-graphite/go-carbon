package partlog

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func assertDictionaryDeltaFrames(t *testing.T, got, want []DictionaryFrame) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("frames %d != %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.Key != w.Key || g.Clock != w.Clock || !bytes.Equal(g.Definition, w.Definition) || len(g.Points) != len(w.Points) {
			t.Fatalf("metadata %d", i)
		}
		for j, p := range g.Points {
			if p.Timestamp != w.Points[j].Timestamp || math.Float64bits(p.Value) != math.Float64bits(w.Points[j].Value) {
				t.Fatalf("point %d/%d", i, j)
			}
		}
	}
}

func TestDictionaryDeltaExactEpochAndLegacy(t *testing.T) {
	var enc DictionaryDeltaEncoder
	defer enc.Close()
	var dec DictionaryDeltaDecoder
	defer dec.Close()
	random := rand.New(rand.NewPCG(17, 23))
	var records [][]byte
	var expected [][]DictionaryFrame
	for block := range 40 {
		fs := make([]DictionaryFrame, 97)
		for i := range fs {
			id := (block*97 + i) % 127
			fs[i] = DictionaryFrame{Key: fmt.Sprintf("name.%d", id), Definition: []byte{byte(id)}, Clock: 1794700800 + int64(block), Points: make([]points.Point, 3)}
			for j := range fs[i].Points {
				bits := []uint64{0, 1, 0x8000000000000000, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, random.Uint64()}[(block+i+j)%7]
				fs[i].Points[j] = points.Point{Timestamp: 1794700799 + int64(block-j), Value: math.Float64frombits(bits)}
			}
		}
		// Encoding must not mutate the caller's points, including NaN payloads.
		want := make([]DictionaryFrame, len(fs))
		copy(want, fs)
		for i := range want {
			want[i].Points = append([]points.Point(nil), fs[i].Points...)
		}
		b, err := enc.Encode(fs)
		if err != nil {
			t.Fatal(err)
		}
		assertDictionaryDeltaFrames(t, fs, want)
		got, err := dec.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		assertDictionaryDeltaFrames(t, got, want)
		records = append(records, b)
		expected = append(expected, want)
	}
	var restarted DictionaryDeltaDecoder
	defer restarted.Close()
	for i, b := range records {
		got, err := restarted.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		assertDictionaryDeltaFrames(t, got, expected[i])
	}
	fs := expected[0]
	var legacy DictionaryBlockEncoder
	defer legacy.Close()
	b, err := legacy.Encode(fs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dec.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	assertDictionaryDeltaFrames(t, got, fs)
	enc.Close()
	enc = DictionaryDeltaEncoder{}
	b, err = enc.Encode(fs)
	if err != nil {
		t.Fatal(err)
	}
	got, err = dec.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	assertDictionaryDeltaFrames(t, got, fs)
	for i := range b {
		bad := bytes.Clone(b)
		bad[i] ^= 1
		var d DictionaryDeltaDecoder
		_, err = d.Decode(bad)
		d.Close()
		if err == nil {
			t.Fatalf("accepted corruption at%d", i)
		}
	}
}

func TestDictionaryDeltaInterleavedReceipt(t *testing.T) {
	var enc DictionaryDeltaEncoder
	defer enc.Close()
	var dec DictionaryDeltaDecoder
	defer dec.Close()
	const names = 4096
	var total, count int
	for block := range 512 {
		fs := make([]DictionaryFrame, 512)
		for i := range fs {
			id := (block*512 + i) % names
			fs[i] = DictionaryFrame{Key: fmt.Sprintf("bench.namespace%d.cluster1.host%07d.component.requests.value", id%4, id), Definition: []byte("stable policy and portable placement"), Clock: 1794700800 + int64(block/8), Points: []points.Point{{Timestamp: 1794700799 + int64(block/8), Value: float64(id) + .125}}}
		}
		b, err := enc.Encode(fs)
		if err != nil {
			t.Fatal(err)
		}
		total += len(b) + headerSize
		count += len(fs)
		got, err := dec.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		assertDictionaryDeltaFrames(t, got, fs)
	}
	t.Logf("JOURNAL_DELTA interleaved_names=%d points=%d bytes=%d bytes_per_point=%.6f exact=true", names, count, total, float64(total)/float64(count))
}

func TestDictionaryDeltaLegacyPlainAndMissingEpoch(t *testing.T) {
	fs := []DictionaryFrame{{Key: "legacy", Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700799, Value: math.Float64frombits(0x7ff8000000000042)}}}}
	var plain DictionaryEncoder
	b, err := plain.Encode(fs)
	if err != nil {
		t.Fatal(err)
	}
	var dec DictionaryDeltaDecoder
	defer dec.Close()
	got, err := dec.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	assertDictionaryDeltaFrames(t, got, fs)
	var enc DictionaryDeltaEncoder
	defer enc.Close()
	if _, err := enc.Encode(fs); err != nil {
		t.Fatal(err)
	}
	b, err = enc.Encode(fs)
	if err != nil {
		t.Fatal(err)
	}
	var fresh DictionaryDeltaDecoder
	defer fresh.Close()
	if _, err := fresh.Decode(b); err == nil {
		t.Fatal("accepted continuation without predictor epoch")
	}
}

func FuzzDictionaryDeltaDecoder(f *testing.F) {
	var enc DictionaryDeltaEncoder
	b, _ := enc.Encode([]DictionaryFrame{{Key: "metric", Clock: 1794700800, Points: []points.Point{{Timestamp: 1794700799, Value: math.Copysign(0, -1)}}}})
	enc.Close()
	f.Add(b)
	f.Add([]byte("PDX\x01"))
	f.Fuzz(func(t *testing.T, b []byte) { var dec DictionaryDeltaDecoder; defer dec.Close(); _, _ = dec.Decode(b) })
}
