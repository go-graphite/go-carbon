package main

import (
	"bytes"
	"io"
	"testing"
)

func TestSelectedReaderCrossesGaps(t *testing.T) {
	r := &selectedReader{source: bytes.NewReader([]byte("abcdSKIPefgh")), extents: []extent{{0, 0, 4}, {4, 8, 4}}, size: 8}
	b := make([]byte, 6)
	n, e := r.ReadAt(b, 2)
	if e != nil || n != 6 || string(b) != "cdefgh" {
		t.Fatalf("%q %d %v", b, n, e)
	}
	n, e = r.ReadAt(b, 5)
	if e != io.EOF || n != 3 || string(b[:n]) != "fgh" {
		t.Fatalf("short %q %d %v", b[:n], n, e)
	}
}
