package main

import (
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
	opts.Streams = cfg.Producers
	log, err := partlog.Open(logDir, opts)
	if err != nil {
		return nil, err
	}
	cfg.JournalIdentity = log.Identity()
	engine, err := openCore(partDir, cfg)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	return &loggedCore{log, engine}, nil
}
func encodeLogRows(rows []row) []byte {
	b := make([]byte, len(rows)*recordBytes)
	for i, r := range rows {
		putRow(b[i*recordBytes:], r)
	}
	return b
}
func decodeLogRows(payload []byte) ([]row, error) {
	if len(payload) == 0 || len(payload)%recordBytes != 0 {
		return nil, os.ErrInvalid
	}
	rows := make([]row, len(payload)/recordBytes)
	for i := range rows {
		rows[i] = getRow(payload[i*recordBytes:])
	}
	return rows, nil
}
func (l *loggedCore) apply(producer int, r partlog.Record, seal bool) error {
	rows, e := decodeLogRows(r.Payload)
	if e != nil {
		return e
	}
	return l.engine.submit(producer, rows, r.Cursor, seal)
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
	// Call only at startup or after joining every producer and issuing a sealed
	// barrier on each producer queue. Core owns the frontier/data atomic commit.
	frontiers := make([][]uint64, len(l.engine.owners))
	for i, o := range l.engine.owners {
		frontiers[i] = append([]uint64(nil), o.meta.Cursors...)
	}
	return l.journal.Truncate(frontiers)
}
func (l *loggedCore) close() error { return errors.Join(l.engine.close(), l.journal.Close()) }
