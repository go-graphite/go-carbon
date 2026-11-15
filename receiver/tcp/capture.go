package tcp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

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

// BoundedCapture limits submissions admitted inside the receiver's ordering
// fence. Waiting connections acquire these shared slots before the read lock,
// so a direct switch joins bounded work rather than every connected handler.
type BoundedCapture interface {
	CaptureSlots() chan struct{}
}

// captureSubmitter reuses connection-owned acknowledgements. A submission
// still lends its bytes only until the matching acknowledgement is received.
type captureSubmitter interface{ SubmitCapture([]byte, int, chan error) }
type captureSubmission interface {
	BeginWrite([]byte, int) func() error
}

// captureAdmission measures batches waiting outside the direct-write fence.
type captureAdmission struct {
	total, maximum, calls atomic.Int64
	enabled               atomic.Bool
	next                  atomic.Uint64
	events                [4096]atomic.Pointer[CaptureEvent]
}

// CaptureEvent records receiver admission and its durable acknowledgement.
type CaptureEvent struct {
	Started, Admitted, Ack int64
	Points                 int
}

func (rcv *TCP) TraceCapture() { rcv.captureAdmission.enabled.Store(true) }
func (rcv *TCP) CaptureEvents() []CaptureEvent {
	var out []CaptureEvent
	for i := range rcv.captureAdmission.events {
		if e := rcv.captureAdmission.events[i].Load(); e != nil {
			out = append(out, *e)
		}
	}
	return out
}

func (a *captureAdmission) observe(start time.Time) {
	elapsed := time.Since(start).Nanoseconds()
	a.total.Add(elapsed)
	a.calls.Add(1)
	for old := a.maximum.Load(); elapsed > old; old = a.maximum.Load() {
		if a.maximum.CompareAndSwap(old, elapsed) {
			break
		}
	}
}

// CaptureAdmission reports bounded receiver admission time, before disk append.
func (rcv *TCP) CaptureAdmission() (total, maximum, calls int64) {
	a := &rcv.captureAdmission
	return a.total.Load(), a.maximum.Load(), a.calls.Load()
}

// The exclusive fence joins every submitted batch, including connection-local
// flushes, before sealing the spool. Buffered but unsubmitted records then use
// the ordinary sink before their connection accepts its next direct point.
type captureSink struct {
	sync.RWMutex
	direct  atomic.Pointer[func(*points.Points)]
	fencing atomic.Bool
}

func (rcv *TCP) SwitchCapture(out func(*points.Points), seal func() error) error {
	s := &rcv.captureSink
	if !s.fencing.CompareAndSwap(false, true) {
		return errors.New("capture submissions are still joining a previous direct fence")
	}
	joined, cancelled := make(chan struct{}), make(chan struct{})
	go func() {
		s.Lock()
		select {
		case joined <- struct{}{}: // Transfer the exclusive lock to this caller.
		case <-cancelled:
			s.Unlock()
			s.fencing.Store(false)
		}
	}()
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-joined:
		defer func() { s.Unlock(); s.fencing.Store(false) }()
	case <-timer.C:
		// Do not hold the spool monitor behind a disk-reserve writer: it
		// must continue retiring prefixes so that writer can acknowledge.
		close(cancelled)
		return errors.New("capture submissions did not quiesce before direct switch")
	}
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
	admission *captureAdmission
	pending   func() error
	done      chan error
	submitted bool
	shards    []captureBuffer
	out       Capture
	buf       []byte
	n         int
	err       error
	slots     chan struct{}
	sink      *captureSink
}

func newCaptureBuffer(out Capture) captureBuffer {
	b := captureBuffer{out: out}
	if bounded, ok := out.(BoundedCapture); ok {
		b.slots = bounded.CaptureSlots()
	}
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
	// Protocol reset frames and a frame ending exactly on the batch boundary
	// carry no outstanding work. Do not queue them behind durable submissions.
	empty := b.n == 0 && b.pending == nil && !b.submitted
	for i := range b.shards {
		part := &b.shards[i]
		if b.err == nil {
			b.err = part.err
		}
		empty = empty && part.n == 0 && part.pending == nil && !part.submitted
	}
	if empty {
		return b.err
	}

	if b.sink != nil {
		if b.slots != nil && b.sink.direct.Load() == nil {
			started := time.Now()
			n := b.n
			for i := range b.shards {
				n += b.shards[i].n
			}
			b.slots <- struct{}{}
			if b.admission != nil {
				b.admission.observe(started)
			}
			if b.admission != nil && b.admission.enabled.Load() {
				admitted := time.Now().UnixNano()
				defer func() {
					e := &CaptureEvent{Started: started.UnixNano(), Admitted: admitted, Ack: time.Now().UnixNano(), Points: n}
					a := b.admission
					a.events[a.next.Add(1)%uint64(len(a.events))].Store(e)
				}()
			}
			defer func() { <-b.slots }()
		}
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
