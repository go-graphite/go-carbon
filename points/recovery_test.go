package points

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestAppendBinaryMatchesWriter(t *testing.T) {
	fixtures := []*Points{
		{Metric: "empty"},
		OnePoint("unicode.μ", math.Copysign(0, -1), -1).Add(math.Inf(1), math.MaxInt64).Add(math.Inf(-1), math.MinInt64),
		OnePoint("duplicates", 1.25, 123).Add(-2.5, 123).Add(0, 99),
		OnePoint(strings.Repeat("m", 4096), math.Float64frombits(0x7ff8000000000001), 0),
	}
	var scratch []byte
	for _, p := range fixtures {
		var oracle bytes.Buffer
		if _, err := p.WriteBinaryTo(&oracle); err != nil {
			t.Fatal(err)
		}
		scratch = p.AppendBinary(scratch[:0])
		if !bytes.Equal(scratch, oracle.Bytes()) {
			t.Fatalf("encoding differs for %q", p.Metric)
		}
	}
}

func TestReadPlainBufferOwnership(t *testing.T) {
	name := strings.Repeat("long", MB/4+1)
	input := "first 1 2\n" + name + " 3 4\nlast 5 6\n"
	var got []*Points
	if err := ReadPlain(strings.NewReader(input), func(p *Points) { got = append(got, p) }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Metric != "first" || got[1].Metric != name || got[2].Metric != "last" {
		t.Fatal("reader reused retained metric storage")
	}
}

func TestReadBinaryInvalidLengths(t *testing.T) {
	for _, data := range [][]byte{
		binary.AppendVarint(nil, -1),
		binary.AppendVarint(nil, MB+1),
		append(binary.AppendVarint(nil, 3), 'a'),
		append(binary.AppendVarint(nil, 1), 'a'),
		binary.AppendVarint(append(binary.AppendVarint(nil, 1), 'a'), -1),
	} {
		if err := ReadBinary(bytes.NewReader(data), func(*Points) { t.Fatal("invalid record was published") }); err == nil {
			t.Fatalf("accepted invalid record %x", data)
		}
	}
}

func BenchmarkWALRecovery(b *testing.B) {
	var plain, encoded bytes.Buffer
	for i := 0; i < 100000; i++ {
		p := OnePoint(fmt.Sprintf("stats.service.host%05d.request.duration", i%10000), 1.25, 1700000000+int64(i))
		if _, err := p.WriteTo(&plain); err != nil {
			b.Fatal(err)
		}
		if _, err := p.WriteBinaryTo(&encoded); err != nil {
			b.Fatal(err)
		}
	}
	for _, format := range []string{"plain", "binary"} {
		b.Run(format, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				count := 0
				callback := func(p *Points) { count += len(p.Data) }
				var err error
				if format == "plain" {
					err = ReadPlain(bytes.NewReader(plain.Bytes()), callback)
				} else {
					err = ReadBinary(bytes.NewReader(encoded.Bytes()), callback)
				}
				if err != nil || count != 100000 {
					b.Fatalf("count=%d: %v", count, err)
				}
			}
		})
	}
}
