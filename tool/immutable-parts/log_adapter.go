package main

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

// loggedCore is an offline wiring proof for the existing numeric-ID prototype.
// The opaque production envelope must also carry canonical names, policy epochs
// and original apply groups; this numeric experiment does not manufacture them.
// Each producer calls append serially. Startup replay finishes before intake.
type loggedCore struct {
	journal *partlog.Log
	engine  *coreEngine
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
	cfg.JournalIdentity = log.Identity()
	engine, err := openCoreWithHooks(partDir, cfg, hooks)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	return &loggedCore{log, engine}, nil
}

const logEnvelope = "NLOG0001"
const logEnvelopeBytes = 16

func encodeLogRows(rows []row) []byte {
	b := make([]byte, logEnvelopeBytes+len(rows)*recordBytes)
	copy(b, logEnvelope)
	for i, r := range rows {
		putRow(b[logEnvelopeBytes+i*recordBytes:], r)
	}
	return b
}
func decodeLogRows(payload []byte) ([]row, error) {
	if len(payload) <= logEnvelopeBytes || string(payload[:8]) != logEnvelope || payload[8] != 0 || (len(payload)-logEnvelopeBytes)%recordBytes != 0 {
		return nil, os.ErrInvalid
	}
	payload = payload[logEnvelopeBytes:]
	rows := make([]row, len(payload)/recordBytes)
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
	rows, e := decodeLogRows(r.Payload)
	if e != nil {
		return e
	}
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
	return l.apply(producer, partlog.Record{Cursor: cursor, Clock: batches[0].Clock, Payload: payload}, seal)
}
func (l *loggedCore) append(producer int, clock int64, rows []row, seal bool) error {
	payload := encodeLogRows(rows)
	cursor, e := l.journal.Append(producer, clock, payload)
	if e != nil {
		return e
	}
	return l.apply(producer, partlog.Record{Cursor: cursor, Clock: clock, Payload: payload}, seal)
}
func (l *loggedCore) replay() error {
	// Per-producer frontiers are minima across all storage owners. Ahead owners
	// reject records <=their atomic frontier inside the normal submit/run path.
	from := l.engine.globalFrontier()
	for p, c := range from {
		var last uint64
		if e := l.journal.Replay(p, c, func(r partlog.Record) error { last = r.Cursor; return l.apply(p, r, false) }); e != nil {
			return e
		}
		if last > 0 {
			if e := l.engine.submit(p, nil, last, true); e != nil {
				return e
			}
		}
	}
	return nil
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
func (l *loggedCore) close() error { return errors.Join(l.engine.close(), l.journal.Close()) }
