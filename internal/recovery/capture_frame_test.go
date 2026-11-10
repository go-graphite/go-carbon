package recovery

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

func TestCaptureFrameExactPrefixAndCorruption(t *testing.T) {
	ps := []*points.Points{points.OnePoint("same.timestamp", 1, 2).Add(3, 2), points.OnePoint("other.metric", -0.125, 100)}
	var raw []byte
	for _, p := range ps {
		raw = p.AppendBinary(raw)
	}
	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()
	frame := EncodeCaptureFrame(enc, raw, nil, 3)
	path := filepath.Join(t.TempDir(), "capture.bin")
	for cut := range len(frame) {
		if err := os.WriteFile(path, append(bytes.Clone(frame), frame[:cut]...), 0600); err != nil {
			t.Fatal(err)
		}
		var got []*points.Points
		n, err := ReadCaptureFile(path, func(p *points.Points) { got = append(got, p) })
		if n != 3 || !reflect.DeepEqual(ps, got) || cut > 0 && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) || cut == 0 && err != nil {
			t.Fatal(cut, n, err, got)
		}
	}
	for _, index := range []int{0, 4, 8, 12, 16, CaptureHeaderSize, len(frame) - 1} {
		bad := bytes.Clone(frame)
		bad[index] ^= 1
		_ = os.WriteFile(path, bad, 0600)
		n, err := ReadCaptureFile(path, func(*points.Points) { t.Error("corrupt frame applied") })
		if n != 0 || err == nil {
			t.Fatal(index, n, err)
		}
	}
}
