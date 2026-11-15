package inputlog

import (
	"errors"
	"sync"
	"sync/atomic"
)

// AppendReceipt identifies the original append generation and its durable tail.
// Application progress is not persistence authority for that generation.
type AppendReceipt struct {
	Generation uint64
	Tail       Position
	Salt       uint64
}

type appendRequest struct {
	batches [partitions]*Batch
	reply   chan appendReply
}
type appendReply struct {
	receipt AppendReceipt
	err     error
}
type sourceAttachment struct {
	src    Source
	shards int
	reply  chan error
}

// StartAppend takes durable append ownership before a cache exists. Open's
// historical inventory stays immutable and replayable while new segments grow.
// Appended generations stay pinned: the startup coordinator must establish
// original-source persistence coverage before recycling can be enabled.
// Legacy capture spools must be retired or moved aside before the first Append.
func (l *Log) StartAppend() { l.StartRecovery(nil, partitions) }

// AttachSource installs cache collection on the writer goroutine. A first cycle
// establishes the cache's current generation before this call returns. Historical
// and append pins remain independent of the cache's new generation counters.
func (l *Log) AttachSource(src Source, shards int) error {
	if !l.started {
		return ErrClosed
	}
	if src == nil || shards < partitions || shards%partitions != 0 {
		return errors.New("invalid input log source attachment")
	}
	reply := make(chan error, 1)
	select {
	case l.attaches <- sourceAttachment{src: src, shards: shards, reply: reply}:
		return <-reply
	case <-l.done:
		return ErrClosed
	}
}

// Append borrows a bounded batch until its group commit is durable. Names must
// already be canonical and routed to their final journal partition; preserving
// that assignment also preserves correction order before encoder grouping.
// Concurrent callers are coalesced through the ordinary partition encoders,
// compression workers and positioned write/sync path. No cache admission runs
// on, or holds, the writer goroutine.
func (l *Log) Append(partition uint16, batch *Batch) (AppendReceipt, error) {
	if !l.started {
		return AppendReceipt{}, ErrClosed
	}
	if partition >= partitions {
		return AppendReceipt{}, errors.New("invalid input log append partition")
	}
	var batches [partitions]*Batch
	batches[partition] = batch
	return l.AppendGroup(batches)
}

// AppendGroup borrows all final partitions of one submission until one durable
// group commit. The writer preserves submission order in every partition.
func (l *Log) AppendGroup(batches [partitions]*Batch) (AppendReceipt, error) {
	if !l.started {
		return AppendReceipt{}, ErrClosed
	}
	n, bytes := 0, 0
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		size, err := validateAppendBatch(batch)
		if err != nil {
			return AppendReceipt{}, err
		}
		n += batch.Len()
		bytes += size
		if n > maxSortRows || bytes > maxFrameRaw {
			return AppendReceipt{}, errors.New("input log append group exceeds frame budget")
		}
	}
	if n == 0 {
		return AppendReceipt{}, errors.New("empty input log append group")
	}

	reply := make(chan appendReply, 1)
	select {
	case l.appends <- appendRequest{batches: batches, reply: reply}:
		r := <-reply
		return r.receipt, r.err
	case <-l.done:
		return AppendReceipt{}, ErrClosed
	}
}

func validateAppendBatch(batch *Batch) (int, error) {
	if batch == nil || batch.Len() == 0 || len(batch.Refs) != batch.Len() || batch.Len() > maxSortRows {
		return 0, errors.New("invalid input log append batch")
	}
	bytes := batch.Len() * 32
	for _, ref := range batch.Refs {
		if ref == nil {
			return 0, errors.New("nil input log append reference")
		}
		bytes += len(ref.Metric)
		if bytes > maxFrameRaw {
			return 0, errors.New("input log append batch exceeds frame budget")
		}
	}
	return bytes, nil
}

func (l *Log) appendCycle(first appendRequest) {
	requests := []appendRequest{first}
	// Requests borrow caller-owned buffers. Bound each group; excess callers
	// wait on the unbuffered channel without copying points into another spool.
Drain:
	for len(requests) < 64 {
		select {
		case request := <-l.appends:
			requests = append(requests, request)
		default:
			break Drain
		}
	}
	err := l.appendErr
	var receipt AppendReceipt
	if err == nil {
		// Append commits share the current cache generation. Only a cache
		// collection advances it; socket submission rate is not elapsed time.
		gen, seq := l.gen, l.target()
		if l.appendPin == nil {
			offset := int64(segmentHeaderSize)
			if l.active != nil && l.active.seq == seq {
				offset = l.active.end
			}
			l.appendPin = &Position{Segment: seq, Offset: offset}
		}
		l.encodeAppends(seq, requests)
		err = l.write(gen, seq)
		if err == nil {
			receipt = AppendReceipt{Generation: gen, Tail: Position{Segment: l.active.seq, Offset: l.active.end}, Salt: l.active.salt}
		}
	}
	// A failed group cannot be followed in the same encoder stream: callers
	// received no durability acknowledgment and the underlying write may be torn.
	if err != nil {
		l.appendErr = err
	}
	for _, request := range requests {
		request.reply <- appendReply{receipt: receipt, err: err}
	}
}

func (l *Log) encodeAppends(seq uint64, requests []appendRequest) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := range l.workers {
		wg.Go(func() {
			scratch := &l.workers[w]
			for p := int(next.Add(1) - 1); p < partitions; p = int(next.Add(1) - 1) {
				part := &l.parts[p]
				part.out, part.frames = part.out[:0], part.frames[:0]
				part.enc.begin(seq)
				emit := func(raw []byte, rows int) []byte { l.compress(part, p, raw, rows); return raw }
				for _, request := range requests {
					batch := request.batches[p]
					if batch == nil {
						continue
					}
					if len(scratch.rows)+batch.Len() > maxSortRows || len(scratch.names) > maxFrameRaw {
						scratch.raw = part.enc.flush(&scratch.streamScratch, scratch.raw, emit)
					}
					part.enc.addBatch(&scratch.streamScratch, batch)
				}
				scratch.raw = part.enc.flush(&scratch.streamScratch, scratch.raw, emit)
			}
		})
	}
	wg.Wait()
}
