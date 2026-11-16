package partlog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/rand"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func exactV3(t testing.TB, a, b []DictionaryFrame) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatal("frame count")
	}
	for i, f := range a {
		g := b[i]
		if f.Key != g.Key || !bytes.Equal(f.Definition, g.Definition) || f.Clock != g.Clock || len(f.Points) != len(g.Points) {
			t.Fatal("frame identity", i)
		}
		for j, p := range f.Points {
			q := g.Points[j]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				t.Fatal("point bits", i, j)
			}
		}
	}
}
func v3Fixture() []DictionaryFrame {
	r := rand.New(rand.NewSource(184))
	var fs []DictionaryFrame
	special := []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001, 0x7ff0000000000001, ^uint64(0)}
	for i := 0; i < 2000; i++ {
		f := DictionaryFrame{Key: string([]byte{'a', 0, byte(i % 31)}), Definition: []byte{0, 1, 2}, Clock: int64(r.Uint64())}
		for j := 0; j < 1+i%7; j++ {
			v := r.Uint64()
			if i%3 == 0 {
				v = special[(i+j)%len(special)]
			}
			f.Points = append(f.Points, points.Point{Timestamp: int64(r.Uint64()), Value: math.Float64frombits(v)})
		}
		fs = append(fs, f)
	}
	return fs
}
func TestPredictorV3ExactIndependent(t *testing.T) {
	e := PredictorV3Encoder{Independent: true}
	defer e.Close()
	fs := v3Fixture()
	for _, slice := range [][]DictionaryFrame{fs, fs[100:], fs[:1], fs} {
		b, err := e.Encode(slice)
		if err != nil {
			t.Fatal(err)
		}
		var d PredictorV3Decoder
		got, err := d.Decode(b)
		d.Close()
		if err != nil {
			t.Fatal(err)
		}
		exactV3(t, slice, got)
	}
}
func TestPredictorV3Corruption(t *testing.T) {
	var e PredictorV3Encoder
	defer e.Close()
	b, err := e.Encode(v3Fixture()[:3])
	if err != nil {
		t.Fatal(err)
	}
	var d PredictorV3Decoder
	defer d.Close()
	for i := 0; i < len(b); i++ {
		if _, err := d.Decode(b[:i]); err == nil {
			t.Fatal("accepted truncation", i)
		}
		bad := bytes.Clone(b)
		bad[i] ^= 1
		if _, err := d.Decode(bad); err == nil {
			t.Fatal("accepted bit flip", i)
		}
	}
	bad := bytes.Clone(b)
	binary.LittleEndian.PutUint32(bad[17:], predictorV3MaxRaw+1)
	binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], predictorV3CRC))
	if _, err := d.Decode(bad); err == nil {
		t.Fatal("accepted oversized expansion")
	}
	if _, err = e.Encode([]DictionaryFrame{{Key: "a", Points: []points.Point{{}}}, {Key: "a", Definition: []byte{1}, Points: []points.Point{{}}}}); err == nil {
		t.Fatal("definition changed")
	}
}

func TestPredictorV3SegmentEpoch(t *testing.T) {
	e := PredictorV3Encoder{SeriesClocks: true}
	defer e.Close()
	var d PredictorV3Decoder
	defer d.Close()
	fs := v3Fixture()
	for i := 0; i < 3; i++ {
		b, err := e.Encode(fs[i*100 : (i+1)*100])
		if err != nil {
			t.Fatal(err)
		}
		got, err := d.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		exactV3(t, fs[i*100:(i+1)*100], got)
		if i > 0 {
			var fresh PredictorV3Decoder
			_, err = fresh.Decode(b)
			fresh.Close()
			if err == nil {
				t.Fatal("accepted missing segment epoch")
			}
		}
	}
}

func TestPredictorV3RejectSkippedOrReorderedBlock(t *testing.T) {
	var e PredictorV3Encoder
	defer e.Close()
	fs := v3Fixture()[:20]
	a, err := e.Encode(fs)
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.Encode(fs)
	if err != nil {
		t.Fatal(err)
	}
	c, err := e.Encode(fs)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{c, b} {
		var d PredictorV3Decoder
		if _, err = d.Decode(a); err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(bad, b) {
			if _, err = d.Decode(b); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = d.Decode(bad); err == nil {
			t.Fatal("accepted missing/duplicate predecessor")
		}
		d.Close()
	}
}
func FuzzPredictorV3Decode(f *testing.F) {
	var e PredictorV3Encoder
	b, err := e.Encode(v3Fixture()[:4])
	e.Close()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Add([]byte("PDC\x03"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			t.Skip()
		}
		var d PredictorV3Decoder
		defer d.Close()
		fs, err := d.Decode(b)
		if err == nil {
			var e PredictorV3Encoder
			defer e.Close()
			back, err := e.Encode(fs)
			if err != nil {
				t.Fatal(err)
			}
			got, err := d.Decode(back)
			if err != nil {
				t.Fatal(err)
			}
			exactV3(t, fs, got)
		}
	})
}
func FuzzPredictorV3Roundtrip(f *testing.F) {
	f.Add([]byte{0, 1, 2, 255, 8})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			t.Skip()
		}
		var fs []DictionaryFrame
		for i, x := range b {
			fs = append(fs, DictionaryFrame{Key: string([]byte{x%13 + 1}), Clock: int64(i) * 1000000001, Points: []points.Point{{Timestamp: int64(x) - int64(i), Value: math.Float64frombits(uint64(x) * 0x0101010101010101)}}})
		}
		if len(fs) == 0 {
			return
		}
		e := PredictorV3Encoder{SeriesClocks: len(b) > 0 && b[0]&1 != 0, Independent: len(b) > 0 && b[0]&2 != 0, SparseClocks: len(b) > 0 && b[0]&8 != 0}
		defer e.Close()
		var d PredictorV3Decoder
		defer d.Close()
		encoded, err := e.Encode(fs)
		if err != nil {
			t.Fatal(err)
		}
		got, err := d.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		exactV3(t, fs, got)
	})
}
