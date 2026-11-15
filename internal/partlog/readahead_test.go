package partlog

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReadAheadExactBoundariesAndErrors(t *testing.T) {
	for _, n := range []int{0, 1, 31, 32, 33, 63, 64, 65, 1 << 20} {
		want := bytes.Repeat([]byte{0, 17, 255, 44}, n)
		r := newReadAhead(bytes.NewReader(want), 32, [2][]byte{})
		got, err := io.ReadAll(r)
		r.Close()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("n=%d len=%d err=%v", n, len(got), err)
		}
	}
	sentinel := errors.New("disk failure")
	r := newReadAhead(errorReader{sentinel}, 32, [2][]byte{})
	_, err := io.ReadAll(r)
	r.Close()
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	r = newReadAhead(bytes.NewReader(make([]byte, 1024)), 32, [2][]byte{})
	r.Close() // consumer cancellation while producer has queued buffers
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
