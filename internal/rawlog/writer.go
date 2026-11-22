package rawlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

type wireConnection struct {
	flags byte
	wire  *codec.WireReader
}

// Writer serializes capture records. Stage never decodes point payloads. A Stage
// return is NOT a durable acknowledgement: only Sync's returned frontier is.
// The caller must keep each connection's Stage calls ordered. Connection IDs
// are never reused within a Writer. Rotation is an exclusive backpressure fence.
type Writer struct {
	mu                    sync.Mutex
	dir                   string
	file                  *os.File
	out                   *bufio.Writer
	conns                 map[uint64]wireConnection
	seq, durable          uint64
	segment               int
	failed                error
	syncs                 uint64
	syncTime              time.Duration
	bytes, pendingBytes   int64
	points, durablePoints uint64
}

type WriterStats struct {
	Staged, Durable, Syncs uint64
	SyncTime               time.Duration
	Segments               int
	Bytes, PendingBytes    int64
	Points, DurablePoints  uint64
}

// Create requires an existing directory, and exclusively creates its first
// segment. It never opens an existing journal for mutation or repairs one.
func Create(dir string) (*Writer, error) {
	w := &Writer{dir: dir, conns: make(map[uint64]wireConnection)}
	if err := w.newSegment(nil); err != nil {
		if w.file != nil {
			_ = w.file.Close()
		}
		return nil, err
	}
	return w, nil
}

func (w *Writer) newSegment(snapshots []record) error {
	f, err := os.OpenFile(filepath.Join(w.dir, fmt.Sprintf("%020d.raw", w.segment)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	w.file = f
	w.out = bufio.NewWriterSize(f, 1<<20)
	if _, err = w.out.WriteString(magic); err != nil {
		return err
	}
	for _, r := range snapshots {
		if err = r.write(w.out); err != nil {
			return err
		}
	}
	if err = (record{kind: readyRecord, seq: w.seq}).write(w.out); err != nil {
		return err
	}
	if _, err = w.syncLocked(); err != nil {
		return err
	}
	// A data fsync alone does not make a newly created segment name durable.
	d, err := os.Open(w.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Begin records a checked connection header and an empty initial dictionary.
func (w *Writer) Begin(conn uint64, preamble []byte) (uint64, error) {
	flags, err := codec.ParsePreamble(preamble)
	if err != nil {
		return 0, err
	}
	wire, err := codec.NewWireReader(codec.Options{Flags: flags})
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, w.failed
	}
	if _, ok := w.conns[conn]; ok {
		return 0, errors.New("connection id already exists")
	}
	w.seq++
	if err = (record{kind: openRecord, seq: w.seq, conn: conn, data: preamble}).write(w.out); err != nil {
		w.failed = err
		return 0, err
	}
	w.conns[conn] = wireConnection{flags: flags, wire: wire}
	return w.seq, nil
}

// Stage appends one wire frame unchanged after checking header bounds and CRC.
// arrival is the admission wall clock in Unix nanoseconds, not a point timestamp.
func (w *Writer) Stage(conn uint64, frame []byte, arrival int64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, w.failed
	}
	c, ok := w.conns[conn]
	if !ok {
		return 0, errors.New("unknown connection")
	}
	info, err := c.wire.Validate(frame)
	if err != nil {
		return 0, err
	}
	w.seq++
	if err := (record{kind: frameRecord, seq: w.seq, conn: conn, arrival: arrival, data: frame}).write(w.out); err != nil {
		w.failed = err
		return 0, err
	}
	w.points += uint64(info.Rows)
	w.bytes += int64(headerSize + len(frame))
	w.pendingBytes += int64(headerSize + len(frame))
	// Bound dirty, unacknowledged capture bytes independently of receiver slots.
	// The timer owner also calls Sync once per second.
	if w.pendingBytes >= 64<<20 {
		if _, err := w.syncLocked(); err != nil {
			return 0, err
		}
	}
	return w.seq, nil
}

func (w *Writer) syncLocked() (uint64, error) {
	if w.failed != nil {
		return w.durable, w.failed
	}
	start := time.Now()
	if err := w.out.Flush(); err != nil {
		w.failed = err
		return w.durable, err
	}
	if err := w.file.Sync(); err != nil {
		w.failed = err
		return w.durable, err
	}
	w.syncs++
	w.syncTime += time.Since(start)
	w.durable = w.seq
	w.durablePoints = w.points
	w.pendingBytes = 0
	return w.durable, nil
}

// Sync returns the highest sequence whose bytes have passed file.Sync.
func (w *Writer) Sync() (uint64, error) { w.mu.Lock(); defer w.mu.Unlock(); return w.syncLocked() }

// Rotate first syncs every frame, then decodes this now-durable segment to
// synthesize connection snapshots. New intake waits behind this fence. This is
// intentionally a simple prototype; production should reuse replay-worker state.
// A failed snapshot leaves the old segment pinned and permanently fails Writer.
func (w *Writer) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.syncLocked(); err != nil {
		return err
	}
	end, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		w.failed = err
		return err
	}
	states, err := scanStates(io.NewSectionReader(w.file, 0, end))
	if err != nil {
		w.failed = err
		return err
	}
	defer closeStates(states)
	ids := make([]uint64, 0, len(states))
	for id := range states {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var snapshots []record
	for _, id := range ids {
		s := states[id]
		p, _ := codec.AppendPreamble(nil, s.flags)
		snapshots = append(snapshots, record{kind: openRecord, conn: id, data: p})
		data, snapshotErr := s.decoder.Snapshot(nil)
		if snapshotErr != nil {
			w.failed = snapshotErr
			return snapshotErr
		}
		reader, _ := codec.NewWireReader(codec.Options{Flags: s.flags})
		in := bytes.NewReader(data)
		for in.Len() > 0 {
			frame, _, readErr := reader.Read(in)
			if readErr != nil {
				w.failed = readErr
				return readErr
			}
			snapshots = append(snapshots, record{kind: snapshotRecord, conn: id, data: bytes.Clone(frame)})
		}
	}
	if err = w.file.Close(); err != nil {
		w.failed = err
		return err
	}
	w.segment++
	err = w.newSegment(snapshots)
	if err != nil {
		w.failed = err
	}
	return err
}

func (w *Writer) Stats() WriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WriterStats{Staged: w.seq, Durable: w.durable, Syncs: w.syncs, SyncTime: w.syncTime, Segments: w.segment + 1, Bytes: w.bytes, PendingBytes: w.pendingBytes, Points: w.points, DurablePoints: w.durablePoints}
}
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.syncLocked()
	closeErr := w.file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
