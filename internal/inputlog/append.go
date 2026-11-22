package inputlog

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
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
	queued  time.Time
	staged  chan error
	weight  int
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
	completion, err := l.submitAppend(batches, false)
	if err != nil {
		return AppendReceipt{}, err
	}
	return completion.Wait()
}

// AppendCompletion separates buffer ownership from persistence. Wait returns an
// ordered receipt only after the containing file has successfully synced.
// A completion has one waiter; callers that only stage may discard it.
type AppendCompletion struct{ done chan appendReply }

func (c *AppendCompletion) Wait() (AppendReceipt, error) {
	r := <-c.done
	return r.receipt, r.err
}

// StageGroup borrows batches only until the encoder has consumed their columns
// and names. Returning permits immediate reuse; it is not a durability receipt.
// Sync, Close, and synchronous AppendGroup force all preceding staged work.
func (l *Log) StageGroup(batches [partitions]*Batch) (*AppendCompletion, error) {
	return l.submitAppend(batches, true)
}

func (l *Log) submitAppend(batches [partitions]*Batch, stage bool) (*AppendCompletion, error) {
	if !l.started {
		return nil, ErrClosed
	}
	n, bytes := 0, 0
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		size, err := validateAppendBatch(batch)
		if err != nil {
			return nil, err
		}
		n += batch.Len()
		bytes += size
		if n > maxSortRows || bytes > maxFrameRaw {
			return nil, errors.New("input log append group exceeds frame budget")
		}
	}
	if n == 0 {
		return nil, errors.New("empty input log append group")
	}

	reply := make(chan appendReply, 1)
	var staged chan error
	if stage {
		staged = make(chan error, 1)
	}
	completion := &AppendCompletion{done: reply}
	select {
	case l.appends <- appendRequest{batches: batches, reply: reply, queued: time.Now(), staged: staged, weight: bytes}:
		if staged != nil {
			if err := <-staged; err != nil {
				return nil, err
			}
		}
		return completion, nil
	case <-l.done:
		return nil, ErrClosed
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

// A pending receipt holds no borrowed input columns or names.
type pendingAppend struct {
	reply   chan appendReply
	receipt AppendReceipt
}

func (l *Log) appendCycle(first appendRequest) {
	admitted := time.Now()
	requests := []appendRequest{first}
	weight := first.weight
Drain:
	for len(requests) < 64 && weight < 8<<20 {
		select {
		case r := <-l.appends:
			requests = append(requests, r)
			weight += r.weight
		default:
			break Drain
		}
	}
	var event *AppendEvent
	if l.appendTrace.enabled.Load() {
		event = &AppendEvent{Admitted: admitted.UnixNano(), Requests: len(requests)}
		for _, r := range requests {
			event.Queued = append(event.Queued, r.queued.UnixNano())
			for _, b := range r.batches {
				if b != nil {
					event.Points += b.Len()
				}
			}
		}
	}
	if l.appendPendingSince.IsZero() {
		l.appendPendingSince = first.queued
	}
	err := l.appendErr
	if err == nil && l.testAppendHook != nil {
		err = l.testAppendHook("before-encode")
	}
	force := false
	for _, r := range requests {
		force = force || r.staged == nil
	}
	gen, seq := l.gen, l.target()
	if err == nil {
		if l.appendPin == nil {
			offset := int64(segmentHeaderSize)
			if l.active != nil && l.active.seq == seq {
				offset = l.active.end
			}
			l.appendPin = &Position{Segment: seq, Offset: offset}
		}
		at := time.Now()
		if event != nil {
			event.EncodeStart = at.UnixNano()
		}
		l.encodeAppends(seq, requests)
		l.total.appendEncodeNanos.Add(time.Since(at).Nanoseconds())
		if l.testAppendHook != nil {
			err = l.testAppendHook("after-encode")
		}
	}
	if event != nil {
		event.EncodeEnd = time.Now().UnixNano()
	}
	// No later operation dereferences request.batches: this is Stage's boundary.
	for i := range requests {
		requests[i].batches = [partitions]*Batch{}
		if requests[i].staged != nil {
			requests[i].staged <- err
		}
	}
	if err == nil {
		err = l.writeFrames(gen, seq)
	}
	if event != nil {
		l.pendingAppendEvents = append(l.pendingAppendEvents, event)
	}
	if l.appendPendingSince.IsZero() {
		l.appendPendingSince = first.queued
	}
	receipt := AppendReceipt{}
	if err == nil {
		receipt = AppendReceipt{Generation: gen, Tail: Position{Segment: l.active.seq, Offset: l.active.end}, Salt: l.active.salt}
	}
	for _, r := range requests {
		l.pendingAppends = append(l.pendingAppends, pendingAppend{r.reply, receipt})
	}
	if err != nil {
		l.finishAppends(err)
	} else if force || l.total.bytes.Load()-l.appendSyncedBytes >= 64<<20 || len(l.pendingAppends) >= 65536 {
		_ = l.commitAppends()
	}
}

func (l *Log) finishAppends(err error) {
	if errors.Is(err, ErrClosed) && len(l.pendingAppends) == 0 {
		return
	}
	if err != nil {
		if l.appendErr == nil {
			l.total.writeErrors.Add(1)
			l.logger.Error("input log append failed", zap.Error(err))
		}
		l.appendErr = err
	}
	for _, pending := range l.pendingAppends {
		receipt := pending.receipt
		if err != nil {
			receipt = AppendReceipt{}
		}
		pending.reply <- appendReply{receipt: receipt, err: err}
	}
	for _, event := range l.pendingAppendEvents {
		if err == nil {
			event.Synced = time.Now().UnixNano()
			event.SyncStart = l.lastSyncStarted.UnixNano()
		}
		l.appendTrace.events[l.appendTrace.next.Add(1)%uint64(len(l.appendTrace.events))].Store(event)
	}
	clear(l.pendingAppendEvents)
	l.pendingAppendEvents = l.pendingAppendEvents[:0]
	l.appendPendingSince = time.Time{}
	clear(l.pendingAppends)
	l.pendingAppends = l.pendingAppends[:0]
}

// Writer-owned: publication follows successful fdatasync, never encode/write.
func (l *Log) commitAppends() error {
	if l.appendErr != nil {
		l.finishAppends(l.appendErr)
		return l.appendErr
	}
	err := l.flush()
	if err == nil {
		err = l.syncDirty()
	}
	if err == nil {
		l.appendSyncedBytes = l.total.bytes.Load()
	}
	if err == nil && l.testAppendHook != nil {
		err = l.testAppendHook("before-publish")
	}
	if err == nil {
		l.total.appendDurablePoints.Store(l.total.points.Load())
		if !l.appendPendingSince.IsZero() {
			elapsed := time.Since(l.appendPendingSince).Nanoseconds()
			for old := l.total.appendMaxExposure.Load(); elapsed > old; old = l.total.appendMaxExposure.Load() {
				if l.total.appendMaxExposure.CompareAndSwap(old, elapsed) {
					break
				}
			}
		}
	}
	l.finishAppends(err)
	if err == nil && l.testAppendHook != nil {
		if err = l.testAppendHook("after-publish"); err != nil {
			l.finishAppends(err)
		}
	}
	return err
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

// AppendEvent records one durable group on a common Unix nanosecond clock.
// Synced precedes acknowledgement; Queued includes writer-channel admission.
type AppendEvent struct {
	Queued                                              []int64
	Admitted, EncodeStart, EncodeEnd, SyncStart, Synced int64
	Requests, Points                                    int
}
type appendTrace struct {
	enabled atomic.Bool
	next    atomic.Uint64
	events  [4096]atomic.Pointer[AppendEvent]
}

// TraceAppends enables a bounded diagnostic ring; call before offering load.
func (l *Log) TraceAppends() { l.appendTrace.enabled.Store(true) }

// AppendEvents snapshots completed groups; order by Admitted when analysing.
func (l *Log) AppendEvents() []AppendEvent {
	var out []AppendEvent
	for i := range l.appendTrace.events {
		if e := l.appendTrace.events[i].Load(); e != nil {
			out = append(out, *e)
		}
	}
	return out
}
