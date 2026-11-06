package tcp

import (
	"encoding/binary"
	"math"

	"github.com/zeebo/xxh3"
)

// Capture receives complete legacy binary point records borrowed until Write
// returns. Each connection encodes privately and submits bounded batches, so
// draining input does not serialize a lock and allocation for every point.
// Capture is installed before handlers start and remains until Detach returns.
type Capture interface {
	Write([]byte, int) error
}

// ShardedCapture routes a metric to one independent append stream. The shard
// count is fixed before handlers start; a connection keeps its own buffers.
type ShardedCapture interface {
	CaptureShards() []Capture
}

// captureSubmitter reuses connection-owned acknowledgements. A submission
// still lends its bytes only until the matching acknowledgement is received.
type captureSubmitter interface{ SubmitCapture([]byte, int, chan error) }
type captureSubmission interface {
	BeginWrite([]byte, int) func() error
}

type captureBuffer struct {
	pending   func() error
	done      chan error
	submitted bool
	shards    []captureBuffer
	out       Capture
	buf       []byte
	n         int
	err       error
}

func newCaptureBuffer(out Capture) captureBuffer {
	b := captureBuffer{out: out}
	if s, ok := out.(ShardedCapture); ok {
		for _, out := range s.CaptureShards() {
			b.shards = append(b.shards, captureBuffer{out: out})
		}
	}
	return b
}

func (b *captureBuffer) point(name []byte, value float64, timestamp int64) {
	if len(b.shards) > 0 {
		b.shards[xxh3.Hash(name)%uint64(len(b.shards))].point(name, value, timestamp)
		return
	}
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
	// Submit all shards before waiting for disk. Serially waiting for shard 0,
	// then 1, across every connection creates avoidable head-of-line blocking.
	for i := range b.shards {
		part := &b.shards[i]
		if part.n == 0 || part.err != nil {
			continue
		}
		if out, ok := part.out.(captureSubmitter); ok {
			if part.done == nil {
				part.done = make(chan error, 1)
			}
			out.SubmitCapture(part.buf, part.n, part.done)
			part.submitted = true
		} else if out, ok := part.out.(captureSubmission); ok {
			part.pending = out.BeginWrite(part.buf, part.n)
		} else {
			part.err = part.out.Write(part.buf, part.n)
		}
	}
	for i := range b.shards {
		part := &b.shards[i]
		if part.submitted {
			part.err = <-part.done
			part.submitted = false
		}
		if part.pending != nil {
			part.err = part.pending()
			part.pending = nil
		}
		part.buf, part.n = part.buf[:0], 0
		if b.err == nil {
			b.err = part.err
		}
	}
	if b.err != nil {
		return b.err
	}
	if b.n == 0 || b.err != nil {
		return b.err
	}
	b.err = b.out.Write(b.buf, b.n)
	b.buf, b.n = b.buf[:0], 0
	return b.err
}
