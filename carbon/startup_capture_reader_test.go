package carbon

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

func TestCaptureReadWindowHonorsFramesAndPublishedPrefix(t *testing.T) {
	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	var frames, records [][]byte
	var data []byte
	for i, name := range []string{"short", strings.Repeat("segment.", 100), "last"} {
		raw := points.OnePoint(name, float64(i), int64(i)).AppendBinary(nil)
		frame := recovery.EncodeCaptureFrame(enc, raw, nil, 1)
		records = append(records, raw)
		frames = append(frames, frame)
		data = append(data, frame...)
	}
	path := filepath.Join(t.TempDir(), "frames")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	seg := &startupSegment{file: file}
	seg.size.Store(int64(len(data)))
	for _, target := range []int{25, 100, 4096} {
		window := captureReadWindow{target: target}
		offset := int64(0)
		for i, expected := range frames {
			got, err := window.frame(seg, offset, int64(len(data)))
			if err != nil || !bytes.Equal(got, expected) {
				t.Fatal(target, i, err)
			}
			raw, n, err := recovery.DecodeCaptureFrame(dec, got, nil)
			if err != nil || n != 1 || !bytes.Equal(raw, records[i]) {
				t.Fatal("decoded frame changed", target, i, err)
			}
			offset += int64(len(expected))
		}
	}
	// The backing file contains more data than this frontier. It must not be
	// prefetched or exposed until the writer publishes that complete prefix.
	window := captureReadWindow{target: 4096}
	frontier := int64(len(frames[0]))
	if _, err := window.frame(seg, 0, frontier); err != nil {
		t.Fatal(err)
	}
	if len(window.buffer) != int(frontier) {
		t.Fatal("read through unpublished frontier")
	}
	if _, err := window.frame(seg, frontier, frontier); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}
