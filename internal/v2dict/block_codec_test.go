package v2dict

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestBlockCodecIndependentFrames(t *testing.T) {
	for _, n := range []int{1, 32768, 33, 131073, 17} {
		want := bytes.Repeat([]byte{byte(n)}, n)
		frame, err := compressBlock(nil, want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decompressBlock(make([]byte, n), frame)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("frame %d: %v", n, err)
		}
	}
}

func TestBlockCodecRejectsWrongLength(t *testing.T) {
	frame, err := compressBlock(nil, bytes.Repeat([]byte("a"), 100))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{99, 101} {
		if _, err := decompressBlock(make([]byte, n), frame); err == nil {
			t.Fatalf("accepted wrong raw length %d", n)
		}
	}
}

func TestBlockCodecRawAndMalformed(t *testing.T) {
	want := make([]byte, 32768)
	rand.New(rand.NewSource(1)).Read(want)
	encoded, err := compressBlock(nil, want)
	if err != nil || len(encoded) != len(want) {
		t.Fatalf("incompressible block: size %d: %v", len(encoded), err)
	}
	got, err := decompressBlock(make([]byte, len(want)), encoded)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("raw block: %v", err)
	}
	if _, err = decompressBlock(make([]byte, 100), []byte{0xff}); err == nil {
		t.Fatal("accepted malformed compressed block")
	}
}
