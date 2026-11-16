package partlog

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPredictorV3SparseClocksAndAccounting(t *testing.T) {
	for _, independent := range []bool{false, true} {
		for _, sparse := range []bool{false, true} {
			var a PredictorV3Accounting
			e := PredictorV3Encoder{SeriesClocks: true, SparseClocks: sparse, Independent: independent, Accounting: &a}
			var d PredictorV3Decoder
			fs := v3Fixture() // random signed clocks, overflow and special IEEE bits
			var raw, encoded, count int64
			for _, f := range [][]DictionaryFrame{fs[:800], fs[800:], fs} {
				b, err := e.Encode(f)
				if err != nil {
					t.Fatal(err)
				}
				raw += int64(binary.LittleEndian.Uint32(b[17:]))
				encoded += int64(len(b) + 32)
				got, err := d.Decode(b)
				if err != nil {
					t.Fatal(err)
				}
				exactV3(t, f, got)
				for _, x := range f {
					count += int64(len(x.Points))
				}
			}
			e.Close()
			d.Close()
			if a.Points != count || a.Encoded != encoded || a.Names+a.IDs+a.Definitions+a.Clocks+a.Timestamps+a.Values+a.Framing != raw || a.Headers != 3*57 {
				t.Fatalf("accounting %+v raw=%d encoded=%d points=%d", a, raw, encoded, count)
			}
		}
	}
}

func TestPredictorV3AccountingDoesNotAlterBytes(t *testing.T) {
	var stats PredictorV3Accounting
	a := PredictorV3Encoder{SeriesClocks: true}
	b := PredictorV3Encoder{SeriesClocks: true, Accounting: &stats}
	defer a.Close()
	defer b.Close()
	fs := v3Fixture()
	for _, part := range [][]DictionaryFrame{fs[:20], fs[20:], fs} {
		x, e := a.Encode(part)
		if e != nil {
			t.Fatal(e)
		}
		y, e := b.Encode(part)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(x, y) {
			t.Fatal("accounting changed stream")
		}
	}
}
