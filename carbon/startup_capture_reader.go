package carbon

import (
	"io"

	"github.com/go-graphite/go-carbon/internal/recovery"
)

// Each reader owns this published-prefix window. Prefetching complete frames
// amortizes pread/header calls without reading through a writer's frontier.
type captureReadWindow struct {
	segment *startupSegment
	base    int64
	buffer  []byte
	target  int
}

func (w *captureReadWindow) ensure(seg *startupSegment, offset, frontier int64, n int) error {
	if w.segment == seg && offset >= w.base && offset-w.base <= int64(len(w.buffer)) && int64(n) <= int64(len(w.buffer))-(offset-w.base) {
		return nil
	}
	if offset < 0 || frontier-offset < int64(n) {
		return io.ErrUnexpectedEOF
	}
	length := int(min(int64(max(w.target, n)), frontier-offset))
	if cap(w.buffer) < length {
		w.buffer = make([]byte, length)
	} else {
		w.buffer = w.buffer[:length]
	}
	if _, err := seg.file.ReadAt(w.buffer, offset); err != nil {
		return err
	}
	w.segment, w.base = seg, offset
	return nil
}
func (w *captureReadWindow) frame(seg *startupSegment, offset, frontier int64) ([]byte, error) {
	if err := w.ensure(seg, offset, frontier, recovery.CaptureHeaderSize); err != nil {
		return nil, err
	}
	index := int(offset - w.base)
	n, err := recovery.CaptureFrameSize(w.buffer[index : index+recovery.CaptureHeaderSize])
	if err != nil {
		return nil, err
	}
	if err = w.ensure(seg, offset, frontier, n); err != nil {
		return nil, err
	}
	index = int(offset - w.base)
	return w.buffer[index : index+n], nil
}
