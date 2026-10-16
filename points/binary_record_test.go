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

func TestAppendBinaryPointsReusesDestination(t *testing.T) {
	source := OnePoint("namespace.空间", math.Inf(-1), math.MinInt64).
		Add(math.Float64frombits(0x7ff8000000000001), math.MaxInt64)
	raw := source.AppendBinary(nil)
	prefix := Point{Value: math.Copysign(0, -1), Timestamp: 17}
	dst := make([]Point, 1, 1+len(source.Data))
	dst[0] = prefix
	name, got, err := AppendBinaryPoints(dst, raw)
	if err != nil {
		t.Fatal(err)
	}
	if &got[0] != &dst[0] || !equalBinaryPoints(
		&Points{Metric: string(name), Data: got},
		&Points{Metric: source.Metric, Data: append([]Point{prefix}, source.Data...)},
	) {
		t.Fatal("did not append into the existing destination", got)
	}
	// Only the name borrows the encoded record, never decoded point values.
	for i := range raw {
		raw[i] = 0
	}
	if !bytes.Equal(name, make([]byte, len(name))) || !equalBinaryPoints(
		&Points{Metric: source.Metric, Data: got[1:]}, source,
	) {
		t.Fatal("unexpected ownership of decoded name or points")
	}
}

func TestAppendBinaryPointsPreservesPrefixOnError(t *testing.T) {
	raw := OnePoint("metric", 1, 2).Add(3, 4).AppendBinary(nil)
	dst := make([]Point, 1, 10)
	dst[0] = Point{Value: 9, Timestamp: 10}
	for n := 0; n <= len(raw); n++ {
		bad := raw[:n]
		if n == len(raw) {
			bad = append(bytes.Clone(raw), 0)
		}
		name, got, err := AppendBinaryPoints(dst, bad)
		if err == nil || name != nil || len(got) != 1 || &got[0] != &dst[0] || got[0] != (Point{Value: 9, Timestamp: 10}) {
			t.Fatalf("changed prefix on malformed record of length %d: %v %v", n, got, err)
		}
		p := Points{Metric: "original", Data: dst}
		if err := p.UnmarshalBinary(bad); err == nil || p.Metric != "original" || len(p.Data) != 1 || &p.Data[0] != &dst[0] {
			t.Fatal("unmarshal changed receiver on error")
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
