package v2wire

import (
	"bytes"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func golden() Frame {
	return Frame{Kind: Data, Stream: Stream{Group: "metrics/0/0", Replica: "site-a", Store: "store-a/inc-1", MapDigest: strings.Repeat("a", 64), Revision: 1, Epoch: 1, Producer: "relay-a/inc-1", Lane: "lane-0"}, Sequence: 7, VShard: 42, Name: "example.metric", Bits: 9223372036854775808, Timestamp: 123}
}
func TestGolden(t *testing.T) {
	want, e := os.ReadFile("testdata/frame-v1.hex")
	if e != nil {
		t.Fatal(e)
	}
	var b bytes.Buffer
	key := bytes.Repeat([]byte{0x6b}, 32)
	f := golden()
	if e = Write(&b, f, key); e != nil {
		t.Fatal(e)
	}
	if hex.EncodeToString(b.Bytes()) != strings.TrimSpace(string(want)) {
		t.Fatal("wire differs from shared golden")
	}
	got, e := Read(bytes.NewReader(b.Bytes()), key)
	if e != nil || got != f {
		t.Fatal(got, e)
	}
	packet := b.Bytes()
	packet[len(packet)-1] ^= 1
	if _, e = Read(bytes.NewReader(packet), key); e == nil {
		t.Fatal("accepted unauthenticated data")
	}
}
func TestBoundsAndTruncation(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	var b bytes.Buffer
	if e := Write(&b, golden(), key); e != nil {
		t.Fatal(e)
	}
	for n := 0; n < b.Len(); n++ {
		if _, e := Read(bytes.NewReader(b.Bytes()[:n]), key); e == nil {
			t.Fatalf("accepted truncated %d", n)
		}
	}
	f := golden()
	f.Name = strings.Repeat("x", MaxFrame)
	if e := Write(&b, f, key); e == nil {
		t.Fatal("unbounded write")
	}
	if _, e := Read(bytes.NewReader([]byte("V2A1\xff\xff\xff\xff")), key); e == nil {
		t.Fatal("unbounded read")
	}
}
