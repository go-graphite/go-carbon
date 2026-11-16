package main

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"slices"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

// loggedCore is an offline wiring proof for the existing numeric-ID prototype.
// The opaque production envelope must also carry canonical names, policy epochs
// and original apply groups; this numeric experiment does not manufacture them.
// Each producer calls append serially. Optional followers accept journaled live
// input immediately while one ordered consumer per stream catches up.
type loggedCore struct {
	journal     *partlog.Log
	engine      *coreEngine
	cancel      context.CancelFunc
	followers   chan error
	failure     atomic.Pointer[logApplyFailure]
	rows        [][]row
	catalogs    []*replayCatalog
	historyRate uint64 // Optional aggregate numeric benchmark cap; zero is unlimited.
}

func openLoggedCore(logDir, partDir string, cfg coreConfig, opts partlog.Options) (*loggedCore, error) {
	return openLoggedCoreWithHooks(logDir, partDir, cfg, opts, coreHooks{})
}
func openLoggedCoreWithHooks(logDir, partDir string, cfg coreConfig, opts partlog.Options, hooks coreHooks) (*loggedCore, error) {
	opts.Streams = cfg.Producers
	log, err := partlog.Open(logDir, opts)
	if err != nil {
		return nil, err
	}
	l, err := attachLoggedCore(log, partDir, cfg, hooks)
	if err != nil {
		return nil, err
	}
	l.catalogs, err = openReplayCatalogs(logDir, cfg.Producers, false)
	if err != nil {
		return nil, errors.Join(err, l.close())
	}
	return l, nil
}

// attachLoggedCore transfers journal ownership to the adapter. Intake may use
// OpenAdmission and append before this storage-open boundary; only followers
// consume those records, after the validated historical prefix.
func attachLoggedCore(log *partlog.Log, partDir string, cfg coreConfig, hooks coreHooks) (*loggedCore, error) {
	cfg.JournalIdentity = log.Identity()
	engine, err := openCoreWithHooks(partDir, cfg, hooks)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	return &loggedCore{journal: log, engine: engine, rows: make([][]row, cfg.Producers)}, nil
}

const logEnvelope = "NLOG0001"
const logEnvelopeBytes = 16

func encodeLogRows(rows []row) []byte {
	return encodeLogRowsInto(nil, rows)
}
func encodeLogRowsInto(b []byte, rows []row) []byte {
	n := logEnvelopeBytes + len(rows)*recordBytes
	if cap(b) < n {
		b = make([]byte, n)
	} else {
		b = b[:n]
		clear(b[:logEnvelopeBytes])
	}
	copy(b, logEnvelope)
	for i, r := range rows {
		putRow(b[logEnvelopeBytes+i*recordBytes:], r)
	}
	return b
}
func decodeLogRows(payload []byte) ([]row, error) { return decodeLogRowsInto(nil, payload) }
func decodeLogRowsInto(rows []row, payload []byte) ([]row, error) {
	if len(payload) <= logEnvelopeBytes || string(payload[:8]) != logEnvelope || payload[8] != 0 || (len(payload)-logEnvelopeBytes)%recordBytes != 0 {
		return nil, os.ErrInvalid
	}
	payload = payload[logEnvelopeBytes:]
	count := len(payload) / recordBytes
	if cap(rows) < count {
		rows = make([]row, count)
	} else {
		rows = rows[:count]
	}
	for i := range rows {
		rows[i] = getRow(payload[i*recordBytes:])
	}
	return rows, nil
}

// The batch envelope is supplied by the caller; it never infers grouping,
// clocks or policy from timestamps, numeric IDs, or replay time.
func encodeLogBatches(batches []coreBatch) ([]byte, error) {
	if len(batches) == 0 {
		return nil, os.ErrInvalid
	}
	out := make([]byte, logEnvelopeBytes)
	copy(out, logEnvelope)
	out[8] = 1
	binary.LittleEndian.PutUint32(out[12:], uint32(len(batches)))
	for _, b := range batches {
		if len(b.Points) == 0 || len(b.Policy) > 1<<20 {
			return nil, os.ErrInvalid
		}
		for _, r := range b.Points {
			if r.ID != b.ID {
				return nil, os.ErrInvalid
			}
		}
		out = binary.LittleEndian.AppendUint64(out, b.ID)
		out = binary.LittleEndian.AppendUint64(out, b.Order)
		out = binary.LittleEndian.AppendUint64(out, uint64(b.Clock))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(b.Policy)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(b.Points)))
		out = append(out, b.Policy...)
		for _, r := range b.Points {
			var buf [recordBytes]byte
			putRow(buf[:], r)
			out = append(out, buf[:]...)
		}
		if len(out) > 8<<20 {
			return nil, os.ErrInvalid
		}
	}
	return out, nil
}
func decodeLogBatches(payload []byte) ([]coreBatch, error) {
	if len(payload) < logEnvelopeBytes || string(payload[:8]) != logEnvelope || payload[8] != 1 {
		return nil, os.ErrInvalid
	}
	n := binary.LittleEndian.Uint32(payload[12:])
	payload = payload[logEnvelopeBytes:]
	if n == 0 || uint64(n)*32 > uint64(len(payload)) {
		return nil, os.ErrInvalid
	}
	batches := make([]coreBatch, 0, n)
	for range n {
		if len(payload) < 32 {
			return nil, os.ErrInvalid
		}
		b := coreBatch{ID: binary.LittleEndian.Uint64(payload), Order: binary.LittleEndian.Uint64(payload[8:]), Clock: int64(binary.LittleEndian.Uint64(payload[16:]))}
		policy, count := uint64(binary.LittleEndian.Uint32(payload[24:])), uint64(binary.LittleEndian.Uint32(payload[28:]))
		payload = payload[32:]
		if count == 0 || policy > 1<<20 || policy+count*recordBytes > uint64(len(payload)) {
			return nil, os.ErrInvalid
		}
		b.Policy = append([]byte(nil), payload[:policy]...)
		payload = payload[policy:]
		b.Points = make([]row, count)
		for i := range b.Points {
			b.Points[i] = getRow(payload[:recordBytes])
			if b.Points[i].ID != b.ID {
				return nil, os.ErrInvalid
			}
			payload = payload[recordBytes:]
		}
		batches = append(batches, b)
	}
	if len(payload) != 0 {
		return nil, os.ErrInvalid
	}
	return batches, nil
}
func (l *loggedCore) apply(producer int, r partlog.Record, seal bool) error {
	if len(r.Payload) >= logEnvelopeBytes && r.Payload[8] == 1 {
		batches, e := decodeLogBatches(r.Payload)
		if e != nil {
			return e
		}
		return l.engine.submitBatches(producer, batches, r.Cursor, seal)
	}
	if producer < 0 || producer >= len(l.rows) {
		return os.ErrInvalid
	}
	var rows []row
	var e error
	if partlog.IsCatalogRecord(r.Payload) {
		if len(l.catalogs) != len(l.rows) {
			return errors.New("missing numeric PDC4 catalog")
		}
		rows, e = l.catalogs[producer].decode(l.rows[producer], r.Payload)
	} else {
		rows, e = decodeLogRowsInto(l.rows[producer], r.Payload)
	}
	if e != nil {
		return e
	}
	l.rows[producer] = rows
	return l.engine.submit(producer, rows, r.Cursor, seal)
}
func (l *loggedCore) appendBatches(producer int, batches []coreBatch, seal bool) error {
	payload, e := encodeLogBatches(batches)
	if e != nil {
		return e
	}
	cursor, e := l.journal.Append(producer, batches[0].Clock, payload)
	if e != nil {
		return e
	}
	if l.cancel != nil {
		return l.followError()
	}
	return l.apply(producer, partlog.Record{Cursor: cursor, Clock: batches[0].Clock, Payload: payload}, seal)
}
func (l *loggedCore) append(producer int, clock int64, rows []row, seal bool) error {
	payload := encodeLogRows(rows)
	cursor, e := l.journal.Append(producer, clock, payload)
	if e != nil {
		return e
	}
	if l.cancel != nil {
		return l.followError()
	}
	return l.apply(producer, partlog.Record{Cursor: cursor, Clock: clock, Payload: payload}, seal)
}

// replay runs independent producer streams in parallel into the normal owner
// queues. Each stream preserves cursor order; owners skip committed prefixes.
func (l *loggedCore) replay() error {
	from := l.frontier()
	done := make(chan error, len(from))
	for p, c := range from {
		go func() {
			last := c
			err := l.journal.Replay(p, c, func(r partlog.Record) error { last = r.Cursor; return l.apply(p, r, false) })
			if err == nil && last > 0 {
				err = l.engine.submit(p, nil, last, true)
			}
			done <- err
		}()
	}
	var err error
	for range from {
		err = errors.Join(err, <-done)
	}
	return err
}

type logApplyFailure struct{ err error }

func (l *loggedCore) followError() error {
	if failure := l.failure.Load(); failure != nil {
		return failure.err
	}
	return nil
}

// startReplay is called before opening intake, never concurrently with append.
// Its ready channel reports the frozen historical prefix has entered each core
// stream. Intake may append before ready: the same followers consume that suffix
// after the backlog, so a new cursor can never overtake old records.
func (l *loggedCore) startReplay(ctx context.Context) <-chan error {
	ready := make(chan error, 1)
	tails, err := l.journal.RecoveredTails(ctx)
	if err != nil {
		ready <- err
		close(ready)
		return ready
	}
	ctx, l.cancel = context.WithCancel(ctx)
	from := l.frontier()
	l.followers = make(chan error, len(from))
	initial := make(chan error, len(from))
	for p, c := range from {
		go func() {
			pacer := replayPacer{rate: l.historyRate, streams: len(from)}
			announced := false
			announce := func() {
				if !announced {
					announced = true
					initial <- nil
				}
			}
			if c >= tails[p] {
				announce()
			}
			err := l.journal.Follow(ctx, p, c, func(r partlog.Record) error {
				if l.historyRate > 0 && r.Cursor <= tails[p] {
					// The paced benchmark history uses flat numeric records only.
					if len(r.Payload) <= logEnvelopeBytes || string(r.Payload[:8]) != logEnvelope || r.Payload[8] != 0 || (len(r.Payload)-logEnvelopeBytes)%recordBytes != 0 {
						return os.ErrInvalid
					}
					if err := pacer.wait(ctx.Done(), uint64((len(r.Payload)-logEnvelopeBytes)/recordBytes)); err != nil {
						return err
					}
				}
				if err := l.apply(p, r, false); err != nil {
					return err
				}
				if r.Cursor >= tails[p] {
					announce()
				}
				return nil
			}, func(last uint64) error {
				if last >= tails[p] {
					announce()
				}
				return nil
			})
			if errors.Is(err, context.Canceled) {
				err = nil
			}
			if err != nil {
				l.failure.CompareAndSwap(nil, &logApplyFailure{err})
			}
			if !announced {
				initial <- err
			}
			l.followers <- err
		}()
	}
	go func() {
		var err error
		for range from {
			err = errors.Join(err, <-initial)
		}
		ready <- err
		close(ready)
	}()
	return ready
}
func (l *loggedCore) trim() error {
	// Sync before observing committed vectors: never retire a journal prefix
	// whose durability has not been acknowledged. Concurrent seals can only
	// advance these atomic vectors, so their minimum remains conservative.
	if e := l.journal.Sync(); e != nil {
		return e
	}
	return l.journal.Truncate(l.engine.durableFrontiers())
}

// stopReplay is called after intake stops. It joins the stream consumers then
// drains their accepted suffix into final durable owner barriers.
func (l *loggedCore) stopReplay() error {
	if l.cancel == nil {
		return nil
	}
	l.cancel()
	var err error
	for range l.engine.cfg.Producers {
		err = errors.Join(err, <-l.followers)
	}
	l.cancel = nil
	return errors.Join(err, l.replay())
}
func (l *loggedCore) close() error {
	return errors.Join(l.stopReplay(), l.engine.close(), l.journal.Close(), closeReplayCatalogs(l.catalogs))
}

// Owners may still be sealing when followers stop; only atomic published
// frontiers are safe to inspect while draining the accepted journal suffix.
func (l *loggedCore) frontier() []uint64 {
	vectors := l.engine.durableFrontiers()
	low := slices.Clone(vectors[0])
	for _, vector := range vectors[1:] {
		for p, c := range vector {
			low[p] = min(low[p], c)
		}
	}
	return low
}
