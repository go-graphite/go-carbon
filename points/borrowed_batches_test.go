package points

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"
)

type binaryPointIdentity struct {
	name  string
	bits  uint64
	stamp int64
}

func TestBorrowedBatchesPreserveEveryCompletePointAtEveryCut(t *testing.T) {
	p := &Points{Metric: "large.record"}
	for i := range 257 {
		p.Add(math.Float64frombits(uint64(i)*0x123456789abcdef), int64(i%3)-1)
	}
	raw := OnePoint("first", -1, -3).AppendBinary(nil)
	raw = p.AppendBinary(raw)
	raw = OnePoint(p.Metric, 123, 1).AppendBinary(raw)
	for cut := 0; cut <= len(raw); cut++ {
		var expected []binaryPointIdentity
		collect := func(to *[]binaryPointIdentity) func([]byte, []Point) {
			return func(name []byte, data []Point) {
				for _, point := range data {
					*to = append(*to, binaryPointIdentity{string(name), math.Float64bits(point.Value), point.Timestamp})
				}
			}
		}
		wantErr := ReadBinary(bytes.NewReader(raw[:cut]), func(p *Points) { collect(&expected)([]byte(p.Metric), p.Data) })
		limit := []int{1, 32, 128}[cut%3]
		for _, framed := range []bool{false, true} {
			var actual []binaryPointIdentity
			visit := func(name []byte, data []Point) {
				if len(data) > limit {
					t.Fatalf("cut=%d visit=%d exceeds batch=%d", cut, len(data), limit)
				}
				collect(&actual)(name, data)
			}
			var err error
			if framed {
				err = ReadBinaryBytesBorrowedBatches(raw[:cut], limit, visit)
			} else {
				err = ReadBinaryBorrowedBatches(bytes.NewReader(raw[:cut]), 4096, limit, visit)
			}
			if fmt.Sprint(err) != fmt.Sprint(wantErr) || !reflect.DeepEqual(expected, actual) {
				t.Fatalf("cut=%d framed=%v points=%d/%d errors=%v/%v", cut, framed, len(actual), len(expected), err, wantErr)
			}
		}
	}
}

type countedBinaryReader struct {
	io.Reader
	read int
}

func (r *countedBinaryReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.read += n
	return n, err
}

func TestBorrowedBatchesYieldBeforeReadingAWholeLargeRecord(t *testing.T) {
	p := &Points{Metric: "million", Data: make([]Point, 1_000_000)}
	raw := p.AppendBinary(nil)
	for _, bounded := range []bool{false, true} {
		reader := &countedBinaryReader{Reader: bytes.NewReader(raw)}
		firstRead, total, maxBatch := 0, 0, 0
		visit := func(_ []byte, data []Point) {
			if total == 0 {
				firstRead = reader.read
			}
			total += len(data)
			maxBatch = max(maxBatch, len(data))
		}
		var err error
		if bounded {
			err = ReadBinaryBorrowedBatches(reader, 4096, 128, visit)
		} else {
			err = ReadBinaryBorrowedSize(reader, 4096, visit)
		}
		if err != nil || total != len(p.Data) {
			t.Fatalf("bounded=%v total=%d error=%v", bounded, total, err)
		}
		if bounded {
			if maxBatch > 128 || firstRead > 4096 {
				t.Fatalf("bounded reader retained record: batch=%d read-before-visit=%d", maxBatch, firstRead)
			}
		} else if maxBatch != len(p.Data) || firstRead != len(raw) {
			t.Fatal("existing API lost its whole-record semantics")
		}
	}
}

func TestBorrowedBatchesRejectInvalidLimits(t *testing.T) {
	for _, limit := range []int{0, -1} {
		visit := func([]byte, []Point) { t.Fatal("visited invalid batch") }
		if ReadBinaryBorrowedBatches(bytes.NewReader(nil), 4096, limit, visit) == nil || ReadBinaryBytesBorrowedBatches(nil, limit, visit) == nil {
			t.Fatal("invalid batch limit accepted")
		}
	}
}
