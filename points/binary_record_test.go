package points

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
)

func equalBinaryPoints(a, b *Points) bool {
	if a.Metric != b.Metric || len(a.Data) != len(b.Data) {
		return false
	}
	for i, p := range a.Data {
		if p.Timestamp != b.Data[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(b.Data[i].Value) {
			return false
		}
	}
	return true
}

func TestUnmarshalBinaryMatchesStream(t *testing.T) {
	random := rand.New(rand.NewSource(11))
	var encoded []byte
	var want []*Points
	for i := 0; i < 1000; i++ {
		p := &Points{Metric: "namespace.空间.metric", Data: make([]Point, random.Intn(30)+1)}
		for j := range p.Data {
			p.Data[j] = Point{Value: math.Float64frombits(random.Uint64()), Timestamp: int64(random.Uint64())}
		}
		record := p.AppendBinary(nil)
		var got Points
		if err := got.UnmarshalBinary(record); err != nil || !equalBinaryPoints(&got, p) {
			t.Fatalf("record %d: %v", i, err)
		}
		// A decoded record must not retain bytes owned by the map or reusable buffer.
		for j := range record {
			record[j] = 0
		}
		if !equalBinaryPoints(&got, p) {
			t.Fatal("retained encoded bytes")
		}
		encoded = p.AppendBinary(encoded)
		want = append(want, &got)
	}
	n := 0
	if err := ReadBinary(bytes.NewReader(encoded), func(p *Points) {
		if n >= len(want) || !equalBinaryPoints(p, want[n]) {
			t.Fatal("stream oracle differs", n)
		}
		n++
	}); err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Fatal("stream record count differs")
	}
}

func TestUnmarshalBinaryRejectsIncompleteRecords(t *testing.T) {
	source := OnePoint("metric", math.Inf(-1), math.MinInt64).Add(math.SmallestNonzeroFloat64, math.MaxInt64)
	data := source.AppendBinary(nil)
	for n := 0; n < len(data); n++ {
		var p Points
		if err := p.UnmarshalBinary(data[:n]); err == nil {
			t.Fatal("accepted truncated length", n)
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(data), 0), {1}, {0, 1}, {0, 0, 0}, bytes.Repeat([]byte{255}, 20)} {
		var p Points
		if err := p.UnmarshalBinary(bad); err == nil {
			t.Fatal("accepted malformed record")
		}
	}
}

func FuzzUnmarshalBinary(f *testing.F) {
	f.Add(OnePoint("a", 1, 2).AppendBinary(nil))
	f.Add([]byte{0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		var p Points
		if err := p.UnmarshalBinary(data); err != nil {
			return
		}
		var again Points
		if err := again.UnmarshalBinary(p.AppendBinary(nil)); err != nil || !equalBinaryPoints(&p, &again) {
			t.Fatal("roundtrip differs", err)
		}
	})
}
