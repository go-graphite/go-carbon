package chunkstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestTransferBinaryMatchesV1RecordsAndRejectsDamage(t *testing.T) {
	records := []TransferMetric{{Config: MetricConfig{Name: "binary.metric", Retentions: []Retention{{10, 600}}, AggregationMethod: Sum, XFilesFactor: .5}, Version: TransferVersion{"binary.metric", 123, 2, 5}, Continuation: true, Keys: []RelayoutKV{{[]byte("key"), []byte("stored bytes")}}, PointDigest: "digest", Counts: []int{7}}}
	var old, wire bytes.Buffer
	if err := WriteTransfer(&old, records); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransfer2(&wire, records); err != nil {
		t.Fatal(err)
	}
	if TransferWireSize2(records) != int64(wire.Len()) {
		t.Fatal("wrong binary size")
	}
	want, err := ReadTransfer(bytes.NewReader(old.Bytes()), int64(old.Len()))
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadTransfer(bytes.NewReader(wire.Bytes()), int64(wire.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("v1/v2 mismatch", got, want)
	}
	for n := 0; n < wire.Len(); n++ {
		if _, err := ReadTransfer(bytes.NewReader(wire.Bytes()[:n]), int64(wire.Len())); err == nil {
			t.Fatal("truncation accepted", n)
		}
	}
	bad := bytes.Clone(wire.Bytes())
	bad[len(bad)-1] ^= 1
	if _, err := ReadTransfer(bytes.NewReader(bad), int64(len(bad))); err == nil {
		t.Fatal("bad checksum accepted")
	}
	// A valid envelope checksum cannot authorize an excessive allocation.
	bad = bytes.Clone(wire.Bytes())
	binary.BigEndian.PutUint32(bad[12:16], 0xffffffff)
	h := sha256.Sum256(bad[:len(bad)-32])
	copy(bad[len(bad)-32:], h[:])
	if _, err := ReadTransfer(bytes.NewReader(bad), int64(len(bad))); err == nil {
		t.Fatal("invalid framing accepted")
	}
}
