package chunkstore

import (
	"bytes"
	"io"
	"reflect"
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

func TestTransferBufferReuse(t *testing.T) {
	var buffer TransferBuffer
	for _, write := range []func(io.Writer, []TransferMetric) error{WriteTransfer, WriteTransfer2} {
		for _, value := range []string{"large stored value", "x", "another stored value"} {
			records := []TransferMetric{{Config: MetricConfig{Name: "reuse"}, Version: TransferVersion{Name: "reuse"}, Keys: []RelayoutKV{{[]byte("key"), []byte(value)}}}}
			var wire bytes.Buffer
			if err := write(&wire, records); err != nil {
				t.Fatal(err)
			}
			for _, size := range []int64{int64(wire.Len()), -1} {
				got, err := buffer.Read(bytes.NewReader(wire.Bytes()), 1024, size)
				if err != nil || !reflect.DeepEqual(got, records) {
					t.Fatalf("reused batch: %v, %v", got, err)
				}
				corrupt := bytes.Clone(wire.Bytes())
				corrupt[len(corrupt)-1] ^= 1
				if _, err := buffer.Read(bytes.NewReader(corrupt), 1024, size); err == nil {
					t.Fatal("corrupt reused batch accepted")
				}
				if _, err := buffer.Read(bytes.NewReader(wire.Bytes()[:wire.Len()-1]), 1024, int64(wire.Len())); err == nil {
					t.Fatal("truncated reused batch accepted")
				}
			}
		}
	}
}
