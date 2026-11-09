package tcp

import (
	"encoding/binary"
	"math"
)

// Capture receives complete legacy binary point records borrowed until Write
// returns. Each connection encodes privately and submits bounded batches, so
// draining input does not serialize a lock and allocation for every point.
// Capture is installed before handlers start and remains until Detach returns.
type Capture interface {
	Write([]byte, int) error
}

type captureBuffer struct {
	out Capture
	buf []byte
	n   int
	err error
}

func (b *captureBuffer) point(name []byte, value float64, timestamp int64) {
	if b.err != nil {
		return
	}
	b.buf = binary.AppendVarint(b.buf, int64(len(name)))
	b.buf = append(b.buf, name...)
	b.buf = binary.AppendVarint(b.buf, 1)
	b.buf = binary.AppendVarint(b.buf, int64(math.Float64bits(value)))
	b.buf = binary.AppendVarint(b.buf, timestamp)
	b.n++
	if len(b.buf) >= plainReadBufferSize {
		b.flush()
	}
}

func (b *captureBuffer) flush() error {
	if b.n == 0 || b.err != nil {
		return b.err
	}
	b.err = b.out.Write(b.buf, b.n)
	b.buf, b.n = b.buf[:0], 0
	return b.err
}
