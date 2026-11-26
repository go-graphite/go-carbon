package v2dict

import (
	"bytes"
	"testing"
)

func TestScanDecoderIndependentFrames(t *testing.T) {
	var d scanDecoder
	defer d.close()
	for _, n := range []int{1, 32768, 33, 131073, 17} {
		want := bytes.Repeat([]byte{byte(n)}, n)
		frame, err := compressBlock(nil, want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := d.decode(make([]byte, n), frame)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("frame %d: %v", n, err)
		}
	}
}

func TestScanDecoderRejectsWrongLength(t *testing.T) {
	frame, err := compressBlock(nil, bytes.Repeat([]byte("a"), 100))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{99, 101} {
		func() {
			var d scanDecoder
			defer d.close()
			got, err := d.decode(make([]byte, n), frame)
			if err == nil && len(got) == n {
				t.Fatalf("accepted wrong raw length %d", n)
			}
		}()
	}
}
