package chunkstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"testing"
)

// The v1 writer from before the allocation changes is the wire-format oracle.
func referenceWriteTransfer(w io.Writer, records []TransferMetric) error {
	h := sha256.New()
	out := io.MultiWriter(w, h)
	if _, err := out.Write([]byte("BKTFR001")); err != nil {
		return err
	}
	if err := binary.Write(out, binary.BigEndian, uint32(len(records))); err != nil {
		return err
	}
	write := func(data []byte) error {
		if err := binary.Write(out, binary.BigEndian, uint32(len(data))); err != nil {
			return err
		}
		_, err := out.Write(data)
		return err
	}
	for _, r := range records {
		header := struct {
			Config       MetricConfig
			Version      TransferVersion
			Continuation bool
			Keys         int
			PointDigest  string
			Counts       []int
		}{r.Config, r.Version, r.Continuation, len(r.Keys), r.PointDigest, r.Counts}
		data, err := json.Marshal(header)
		if err != nil {
			return err
		}
		if err := write(data); err != nil {
			return err
		}
		for _, kv := range r.Keys {
			if err := write(kv.Key); err != nil {
				return err
			}
			if err := write(kv.Value); err != nil {
				return err
			}
		}
	}
	_, err := w.Write(h.Sum(nil))
	return err
}

func TestTransferWireCompatibilityAndOwnership(t *testing.T) {
	records := []TransferMetric{{Config: MetricConfig{Name: "wire.a", Retentions: []Retention{{10, 600}}}, Version: TransferVersion{"wire.a", 1, 2, 3}, Keys: []RelayoutKV{{[]byte("first"), []byte("one")}, {[]byte("second"), []byte("two")}}, PointDigest: "digest", Counts: []int{2}}}
	var old, current bytes.Buffer
	if err := referenceWriteTransfer(&old, records); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransfer(&current, records); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old.Bytes(), current.Bytes()) {
		t.Fatal("wire format changed")
	}
	decoded, err := ReadTransfer(bytes.NewReader(old.Bytes()), int64(old.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, records) {
		t.Fatal("round trip differs")
	}
	decoded[0].Keys[0].Value = append(decoded[0].Keys[0].Value, []byte("does not overwrite following key")...)
	if string(decoded[0].Keys[1].Key) != "second" || string(records[0].Keys[0].Value) != "one" {
		t.Fatal("wire fields alias after append")
	}
}

func BenchmarkTransferWire(b *testing.B) {
	value := bytes.Repeat([]byte{1, 3, 7, 11}, 256)
	records := make([]TransferMetric, 1024)
	for i := range records {
		records[i] = TransferMetric{Config: MetricConfig{Name: "benchmark.metric", Retentions: []Retention{{10, 600}}}, Version: TransferVersion{"benchmark.metric", 1, 1, 1}, Keys: []RelayoutKV{{[]byte("block"), value}}}
	}
	b.SetBytes(int64(len(records) * (len(value) + 5)))
	b.ReportAllocs()
	var wire bytes.Buffer
	b.ResetTimer()
	for range b.N {
		wire.Reset()
		if err := WriteTransfer(&wire, records); err != nil {
			b.Fatal(err)
		}
		if _, err := ReadTransfer(bytes.NewReader(wire.Bytes()), int64(wire.Len())); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTransferPointDigestPreservesBits(t *testing.T) {
	snapshot := Snapshot{Archives: []Archive{{Points: []Point{
		{Timestamp: -1, Value: math.Float64frombits(0x8000000000000000)},
		{Timestamp: 0, Value: math.Float64frombits(0x7ff8000000000042)},
		{Timestamp: 1, Value: 1},
	}}, {}}}
	digest, counts := snapshotDigest(snapshot)
	if digest != "00096c1ecb9c07068765452218097b3a3c2cdc0b0ae040eb7d9262c7959a349c" || !reflect.DeepEqual(counts, []int{3, 0}) {
		t.Fatal(digest, counts)
	}
}
