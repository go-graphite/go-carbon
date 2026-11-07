package tcp

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// Capture receives complete legacy binary point records borrowed until Write
// returns. Each connection encodes privately and submits bounded batches, so
// draining input does not serialize a lock and allocation for every point.
// Capture is installed before handlers start; SwitchCapture can retire it.
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

// The exclusive fence joins every submitted batch, including connection-local
// flushes, before sealing the spool. Buffered but unsubmitted records then use
// the ordinary sink before their connection accepts its next direct point.
type captureSink struct {
	sync.RWMutex
	direct atomic.Pointer[func(*points.Points)]
}

func (rcv *TCP) SwitchCapture(out func(*points.Points), seal func() error) error {
	s := &rcv.captureSink
	s.Lock()
	defer s.Unlock()
	if s.direct.Load() != nil {
		return nil
	}
	if err := seal(); err != nil {
		return err
	}
	s.direct.Store(&out)
	return nil
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
	sink      *captureSink
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
	if b.sink != nil {
		b.sink.RLock()
		defer b.sink.RUnlock()
		if out := b.sink.direct.Load(); out != nil {
			for i := range b.shards {
				part := &b.shards[i]
				if part.err == nil && part.n > 0 {
					part.err = points.ReadBinary(bytes.NewReader(part.buf), *out)
				}
				part.buf, part.n = part.buf[:0], 0
				if b.err == nil {
					b.err = part.err
				}
			}
			if b.err == nil && b.n > 0 {
				b.err = points.ReadBinary(bytes.NewReader(b.buf), *out)
			}
			b.buf, b.n = b.buf[:0], 0
			return b.err
		}
	}
	return b.flushCaptured()
}

func (b *captureBuffer) flushCaptured() error {
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
