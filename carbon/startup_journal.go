package carbon

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/lomik/zapwriter"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
)

const startupJournalPartitions = 16

// The startup suffix retains its original log, independently of cache log
// generations. Cache source identities survive replacement, retry and in-flight
// persistence. A single conservative pin covers all of them until the suffix
// stops growing and every identity has been confirmed and synced.
type startupJournal struct {
	owner        *startupInput
	log          *inputlog.Log
	dir          string
	tags         bool
	slots        chan struct{}
	captureSlots chan struct{}
	buffers      chan *startupJournalBuffer
	appendTiming captureTiming
	parts        [startupJournalPartitions]struct {
		stream  cache.RecoveryStream
		source  *cache.RecoverySource
		applied atomic.Pointer[inputlog.ReplayCursor]
	}
	fenced   atomic.Bool
	failure  atomic.Pointer[startupWriteError]
	joined   atomic.Bool
	released bool // monitor owner
}

type startupJournalBuffer struct {
	batches [startupJournalPartitions]inputlog.Batch
	refs    map[string]*inputlog.StreamRef
	decoder points.FrameDecoder
}

func newStartupJournal(s *startupInput, l *inputlog.Log, dir string, tagged bool) *startupJournal {
	j := &startupJournal{owner: s, log: l, dir: dir, tags: tagged, slots: make(chan struct{}, 32), captureSlots: make(chan struct{}, 32), buffers: make(chan *startupJournalBuffer, 32)}
	for i := range j.parts {
		j.parts[i].source = j.parts[i].stream.Next()
	}
	return j
}
func (j *startupJournal) write(raw []byte, expected int) error {
	// Bound decoded batches independently of TCP connection count. Slots are
	// acquired before allocating native columns or canonical name references.
	select {
	case j.slots <- struct{}{}:
	case <-j.owner.stop:
		return inputlog.ErrClosed
	}
	defer func() { <-j.slots }()
	// Acknowledgement ends the writer's borrow. Reuse the bounded columns only
	// after it returns, clearing references even on validation/write failures.
	var buffer *startupJournalBuffer
	select {
	case buffer = <-j.buffers:
	default:
		buffer = &startupJournalBuffer{refs: make(map[string]*inputlog.StreamRef)}
	}
	defer func() {
		for i := range buffer.batches {
			buffer.batches[i].Reset()
		}
		clear(buffer.refs)
		j.buffers <- buffer
	}()
	batches, refs := &buffer.batches, buffer.refs
	count := 0
	err := buffer.decoder.Read(raw, func(name []byte, data []points.Point) {
		p := points.Points{Metric: string(name), Data: data}
		if len(p.Metric) > points.MaxMetricNameBytes {
			return
		}
		if j.tags {
			name, err := tags.Normalize(p.Metric)
			if err != nil {
				return
			}
			p.Metric = name
		}
		part := journalPartition(p.Metric)
		ref := refs[p.Metric]
		if ref == nil {
			ref = inputlog.NewStreamRef(p.Metric)
			refs[p.Metric] = ref
		}
		batches[part].AppendPoints(ref, p.Data)
		count += len(p.Data)
	})
	if err != nil {
		return err
	}
	// Invalid names have the same rejection semantics as ordinary cache intake.
	if count > expected {
		return errors.New("startup journal point count exceeds submission")
	}
	select {
	case <-j.owner.stop:
		return inputlog.ErrClosed
	default:
	}
	for j.owner.diskDanger(int64(len(raw)) + 1<<20) {
		j.owner.notePause()
		select {
		case <-j.owner.stop:
			return inputlog.ErrClosed
		case <-time.After(10 * time.Millisecond):
		}
	}
	var group [startupJournalPartitions]*inputlog.Batch
	for part := range batches {
		if batches[part].Len() > 0 {
			group[part] = &batches[part]
		}
	}
	if count > 0 {
		started := time.Now()
		_, err := j.log.AppendGroup(group)
		j.appendTiming.observe(started)
		if err != nil {
			return err
		}
	}
	j.owner.points.Add(int64(count))
	j.owner.rawBytes.Add(int64(len(raw)))
	return nil
}
func (j *startupJournal) cursors() map[uint16]inputlog.ReplayCursor {
	cursors := make(map[uint16]inputlog.ReplayCursor)
	for part := range j.parts {
		if c := j.parts[part].applied.Load(); c != nil {
			cursors[uint16(part)] = *c
		}
	}
	return cursors
}
func (j *startupJournal) replay(sink *startupSink) error {
	var wg sync.WaitGroup
	errs := make(chan error, startupJournalPartitions)
	for part := range j.parts {
		wg.Go(func() {
			if err := j.replayPartition(uint16(part), sink); err != nil {
				j.failure.CompareAndSwap(nil, &startupWriteError{err: err})
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	var err error
	for e := range errs {
		err = errors.Join(err, e)
	}
	j.joined.Store(true)
	return err
}
func (j *startupJournal) replayPartition(part uint16, sink *startupSink) error {
	p := &j.parts[part]
	var reader *inputlog.SegmentReplay
	var file *os.File
	var name string
	var cursor inputlog.ReplayCursor
	if j.owner.resume != nil {
		cursor = j.owner.resume.JournalEnds[part]
	}
	defer func() {
		if reader != nil {
			reader.Close()
		}
		if file != nil {
			_ = file.Close()
		}
	}()
	for {
		if j.fenced.Load() {
			return nil
		}
		if failure := j.failure.Load(); failure != nil {
			return failure.err
		}
		select {
		case <-j.owner.replayStop:
			return errStartupReplayStopped
		default:
		}
		state, err := j.log.AppendState()
		if j.fenced.Load() {
			return nil
		}
		if err != nil {
			return err
		}
		for _, entry := range state.Files {
			if entry.Name < name {
				continue
			}
			if name != entry.Name {
				if reader != nil {
					reader.Close()
					_ = file.Close()
				}
				file, err = os.Open(filepath.Join(j.dir, entry.Name))
				if err != nil {
					return err
				}
				reader, err = inputlog.NewSegmentReplay(file)
				if err != nil {
					return err
				}
				name = entry.Name
			}
			for {
				records, end, n, err := reader.NextPartition(entry.Size, part)
				seq, salt := reader.Identity()
				if errors.Is(err, io.EOF) {
					p.applied.Store(&inputlog.ReplayCursor{Segment: seq, Salt: salt, Offset: end})
					break
				}
				if err != nil {
					return err
				}
				// Resume cursors point to complete frame ends; dictionary prefixes are
				// decoded but never applied twice after a committed cache snapshot.
				if seq < state.From.Segment || seq == state.From.Segment && end <= state.From.Offset {
					continue
				}
				if seq < cursor.Segment || seq == cursor.Segment && salt == cursor.Salt && end <= cursor.Offset {
					continue
				}
				for _, point := range records {
					metric := sink.admission.(interface {
						ResolveRecoveryMetricString(string) cache.RecoveryMetric
					}).ResolveRecoveryMetricString(point.Metric)
					for start := 0; start < len(point.Data); {
						count := len(point.Data) - start
						for !sink.admission.HasRecoveryCapacityBatch(count, metric.OverheadBytes()) {
							select {
							case <-j.owner.replayStop:
								return errStartupReplayStopped
							default:
							}
							if failure := j.failure.Load(); failure != nil {
								return failure.err
							}
							if count > 1 {
								count = max(1, count/2)
							} else {
								time.Sleep(time.Millisecond)
							}
						}
						select {
						case <-j.owner.replayStop:
							return errStartupReplayStopped
						default:
						}
						chunk := points.Points{Metric: point.Metric, Data: point.Data[start : start+count]}
						sink.groupedAdd(metric, &chunk, p.source, false)
						start += count
					}
				}
				j.owner.replayedPoints.Add(int64(n))
				j.log.RecordAppendReplay(int64(n))
				p.applied.Store(&inputlog.ReplayCursor{Segment: seq, Salt: salt, Offset: end})
			}
		}
		if j.owner.direct.Load() {
			return nil
		}
		select {
		case <-j.owner.replayStop:
			return errStartupReplayStopped
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Called under transition and the receiver's exclusive submission fence. It
// drains a fixed committed frontier: no journal lock spans cache admission.
func (j *startupJournal) fence() error {
	if failure := j.failure.Load(); failure != nil {
		return failure.err
	}
	state, err := j.log.AppendState()
	if err != nil {
		return err
	}
	// The last durable acknowledgement can precede its reader by one poll or
	// cache admission. Intake is now fenced, so let that fixed tail drain rather
	// than reopening input on every attempt and perpetually moving the target.
	deadline := time.NewTimer(250 * time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		caught := true
		for i := range j.parts {
			cursor := j.parts[i].applied.Load()
			if len(state.Files) > 0 && (cursor == nil || cursor.Segment != state.To.Segment || cursor.Offset != state.To.Offset) {
				caught = false
				break
			}
		}
		if caught {
			break
		}
		if failure := j.failure.Load(); failure != nil {
			return failure.err
		}
		select {
		case <-j.owner.stop:
			return inputlog.ErrClosed
		case <-j.owner.replayStop:
			return errStartupReplayStopped
		case <-deadline.C:
			return errors.New("startup journal readers have not drained the fenced tail")
		case <-tick.C:
		}
	}
	j.fenced.Store(true)
	return nil
}
func (j *startupJournal) maybeDirect(sink *startupSink, now time.Time) {
	s := j.owner
	if sink == nil || sink.directAdd == nil || !s.historyReady.Load() || s.receiver == nil {
		return
	}
	if err := s.receiver.SwitchCapture(sink.directAdd, j.fence); err != nil {
		s.refuseDirect(now, err.Error())
		return
	}
	s.direct.Store(true)
	zapwriter.Logger("app").Info("startup input log switched to direct writes", zap.Int64("startup_points", s.points.Load()), zap.Duration("since_start", time.Since(s.started)))
}
func (j *startupJournal) releasePersisted() {
	if j.released || !j.owner.direct.Load() || !j.joined.Load() {
		return
	}
	for i := range j.parts {
		if j.parts[i].source.Pending() != 0 {
			return
		}
	}
	sink := j.owner.sink.Load()
	if sink == nil {
		return
	}
	if sink.syncStore != nil {
		if err := sink.syncStore(); err != nil {
			zapwriter.Logger("app").Error("startup journal persistence sync failed", zap.Error(err))
			return
		}
	}
	if err := j.log.FinishAppendPersistence(); err != nil {
		zapwriter.Logger("app").Error("startup journal retention release failed", zap.Error(err))
		return
	}
	j.released = true
	if sink.drain != nil {
		sink.drain(false)
	}
}

func journalPartition(name string) uint64 {
	return (xxh3.HashString(name) & (cache.ShardCount - 1)) / (cache.ShardCount / startupJournalPartitions)
}
