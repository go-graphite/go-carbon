package chunkstore

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestTransferStreamForgedCountKeepsMetadataBounded(t *testing.T) {
	var input bytes.Buffer
	input.WriteString("BKTFR002")
	_ = binary.Write(&input, binary.BigEndian, uint32(1))
	// Empty name, version triple, flags/policy, retentions, point digest,
	// counts, followed by a hostile key count and no corresponding bytes.
	input.Write(make([]byte, 4+24+12+4+4+4))
	_ = binary.Write(&input, binary.BigEndian, uint32(math.MaxUint32))
	stream, err := NewTransferStream(&input)
	if err != nil {
		t.Fatal(err)
	}
	record, err := stream.Next()
	if err == nil {
		t.Fatal("truncated hostile stream accepted")
	}
	if cap(record.Keys) > 256 {
		t.Fatalf("untrusted count reserved %d entries", cap(record.Keys))
	}
}

func BenchmarkTransferStreamArchiveGroups(b *testing.B) {
	r := TransferMetric{Config: MetricConfig{Name: "stream.benchmark", Retentions: []Retention{{Step: 60, Count: 100000}}, AggregationMethod: Sum}, Version: TransferVersion{Name: "stream.benchmark", ID: 1, Generation: 1, Revision: 1}, Continuation: true}
	for i := range 245 {
		key := binary.BigEndian.AppendUint32([]byte{'f'}, uint32(i))
		r.Keys = append(r.Keys, RelayoutKV{key, bytes.Repeat([]byte{byte(i)}, 128)})
	}
	var wire bytes.Buffer
	if err := WriteTransfer2(&wire, []TransferMetric{r}); err != nil {
		b.Fatal(err)
	}
	data := wire.Bytes()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		stream, err := NewTransferStream(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		if _, err = stream.Next(); err != nil {
			b.Fatal(err)
		}
		if _, _, err = stream.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}
