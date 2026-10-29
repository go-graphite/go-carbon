package chunkstore

import (
	"bytes"
	"io"
	"testing"
)

func TestTransferDeclaredSizeAndUnknownLength(t *testing.T) {
	records := []TransferMetric{{Config: MetricConfig{Name: "size"}, Version: TransferVersion{Name: "size"}}}
	var wire bytes.Buffer
	if err := WriteTransfer(&wire, records); err != nil {
		t.Fatal(err)
	}
	size, err := TransferWireSize(records)
	if err != nil || size != int64(wire.Len()) {
		t.Fatal("incorrect wire size", size, err)
	}
	for _, declared := range []int64{-1, 0, size, size - 1, size + 1} {
		_, err := ReadTransferSize(bytes.NewReader(wire.Bytes()), size+10, declared)
		if (declared > 0 && declared != size) != (err != nil) {
			t.Fatalf("declared=%d: %v", declared, err)
		}
	}
	if _, err := ReadTransfer(io.LimitReader(bytes.NewReader(wire.Bytes()), size), size); err != nil {
		t.Fatal("unknown length", err)
	}
	if _, err := ReadTransferSize(bytes.NewReader(wire.Bytes()), size-1, size); err == nil {
		t.Fatal("oversized declared length accepted")
	}
	if _, err := ReadTransferSize(bytes.NewReader(append(bytes.Clone(wire.Bytes()), 0)), size+10, size); err == nil {
		t.Fatal("trailing bytes accepted")
	}
}
