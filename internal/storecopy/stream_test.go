package storecopy

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type fragmentedReader struct {
	data []byte
	err  error
}

func (r *fragmentedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p[:min(len(p), 7)], r.data)
	r.data = r.data[n:]
	return n, nil
}

type countedWriter struct {
	buffer bytes.Buffer
	writes int
}

func (w *countedWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.buffer.Write(p)
}

func TestFragmentedCopyWritesAndErrors(t *testing.T) {
	input := bytes.Repeat([]byte("checkpoint"), 103)
	for _, end := range []error{io.EOF, io.ErrUnexpectedEOF, io.ErrClosedPipe} {
		t.Run(end.Error(), func(t *testing.T) {
			var output countedWriter
			reader := &fragmentedReader{data: input, err: end}
			n, err := io.CopyBuffer(&output, fullReader{reader}, make([]byte, 64))
			if !errors.Is(end, io.EOF) && !errors.Is(err, end) || errors.Is(end, io.EOF) && err != nil {
				t.Fatalf("copy error = %v", err)
			}
			if n != int64(len(input)) || !bytes.Equal(input, output.buffer.Bytes()) {
				t.Fatal("fragmented copy changed bytes")
			}
			if output.writes != (len(input)+63)/64 {
				t.Fatalf("fragmentation caused %d writes", output.writes)
			}
		})
	}
}
