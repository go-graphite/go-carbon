package recovery

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

func pipelineFixture(t *testing.T) ([]byte, [][]byte) {
	t.Helper()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	var encoded []byte
	var raws [][]byte
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042}
	for i := range 40 {
		p := points.OnePoint("duplicate.timestamp", math.Float64frombits(bits[i%len(bits)]), int64(i%3-1))
		if i%2 == 0 {
			p.Add(math.Float64frombits(bits[(i+1)%len(bits)]), int64(i%3-1))
		}
		raw := p.AppendBinary(nil)
		raws = append(raws, raw)
		encoded = append(encoded, EncodeCaptureFrame(enc, raw, nil, len(p.Data))...)
	}
	return encoded, raws
}

func TestCapturePipelineDifferential(t *testing.T) {
	encoded, raws := pipelineFixture(t)
	for _, cut := range []int{0, 1, 24, len(encoded) / 2, len(encoded) - 1, len(encoded)} {
		var want, got [][]byte
		wn, we := ReadCaptureFramesBuffer(bytes.NewReader(encoded[:cut]), 4096, func(raw []byte, n int) error { want = append(want, bytes.Clone(raw)); return nil })
		var ends []int64
		gn, ge := ReadCaptureFramesPipeline(bytes.NewReader(encoded[:cut]), 4096, func(raw []byte, n int, stop <-chan struct{}) (int64, error) { return 0, nil }, func(raw []byte, n int, end, cost int64) error {
			// A second decode/preparation may run while this buffer is borrowed.
			before := bytes.Clone(raw)
			time.Sleep(time.Microsecond)
			if !bytes.Equal(raw, before) {
				t.Error("borrowed frame overwritten")
			}
			got = append(got, bytes.Clone(raw))
			ends = append(ends, end)
			return nil
		})
		if wn != gn || (we == nil) != (ge == nil) || we != nil && we.Error() != ge.Error() {
			t.Fatalf("cut=%d: %d/%v != %d/%v", cut, wn, we, gn, ge)
		}
		if len(got) != len(want) {
			t.Fatalf("cut=%d frame count", cut)
		}
		for i := range got {
			if !bytes.Equal(got[i], want[i]) || !bytes.Equal(got[i], raws[i]) {
				t.Fatalf("cut=%d frame=%d", cut, i)
			}
			if i > 0 && ends[i] <= ends[i-1] {
				t.Fatal("unordered frontier")
			}
		}
		if cut == len(encoded) && ends[len(ends)-1] != int64(len(encoded)) {
			t.Fatal("physical frontier")
		}
	}
}

func TestCapturePipelineCancelPreparation(t *testing.T) {
	encoded, _ := pipelineFixture(t)
	waiting := make(chan struct{})
	done := make(chan error, 1)
	wanted := errors.New("application failed")
	go func() {
		prepared := 0
		_, err := ReadCaptureFramesPipeline(bytes.NewReader(encoded), 4096, func(raw []byte, n int, stop <-chan struct{}) (int64, error) {
			prepared++
			if prepared == 2 {
				close(waiting)
				<-stop
				return 0, io.ErrClosedPipe
			}
			return 0, nil
		}, func(raw []byte, n int, end, cost int64) error { <-waiting; return wanted })
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, wanted) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("preparation did not cancel")
	}
}

func TestCapturePipelinePreparationFailure(t *testing.T) {
	encoded, _ := pipelineFixture(t)
	wanted := errors.New("admission stopped")
	prepared, applied := 0, 0
	n, err := ReadCaptureFramesPipeline(bytes.NewReader(encoded), 4096, func(raw []byte, n int, stop <-chan struct{}) (int64, error) {
		prepared++
		if prepared == 3 {
			return 0, wanted
		}
		return 0, nil
	}, func(raw []byte, n int, end, cost int64) error { applied++; return nil })
	if !errors.Is(err, wanted) || applied != 2 || n != 3 {
		t.Fatal(n, err, prepared, applied)
	}
}
