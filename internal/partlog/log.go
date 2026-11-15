// Package partlog journals ordered ingest batches for immutable-part stores.
// Payloads are opaque: the consumer owns canonical names, policy epochs, original
// apply boundaries and clocks. Replay calls the same consumer as live ingest.
package partlog

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-graphite/go-carbon/points"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/inputlog"
)

const headerSize = 32
const maxPayload = 8 << 20
const magic = 0x31474c50 // PLG1
var checksum = crc32.MakeTable(crc32.Castagnoli)
var ErrClosed = errors.New("part log closed")

// Options fixes independent producer streams. Producers must route a metric to
// one stream consistently. There is no global append lock. SyncInterval defaults
// to one second; it is a scheduling period, not an upper bound on stalled I/O.
type Options struct {
	Streams      int
	SyncInterval time.Duration
	SegmentBytes int64
	// Hook is a fault-injection observer. It runs on a stream owner and must not
	// call back into that stream. A nil hook adds no point-level work.
	Hook func(stream int, phase string)
}

// Record is borrowed only during its callback. Cursor increases per stream and
// survives segment retirement. Clock is the original apply clock, not replay time.
type Record struct {
	Cursor  uint64
	Clock   int64
	Payload []byte
}
type request struct {
	record Record
	op     string
	from   uint64
	apply  func(Record) error
	done   chan answer
}
type answer struct {
	cursor uint64
	err    error
}
type segment struct {
	path        string
	first, last uint64
	size        int64
}
type stream struct {
	log           *Log
	id            int
	requests      chan request
	done          chan struct{}
	file          *os.File
	segments      []segment
	last, durable uint64
	failure       error
}
type Log struct {
	dir, identity string
	opts          Options
	streams       []*stream
	unlock        func() error
	closeOnce     sync.Once
	errors        chan error
	closeErr      error
}

func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

// Open owns dir exclusively until Close. A fresh segment is always created
// after restart; incomplete final records are removed before any new append.
func Open(dir string, opts Options) (_ *Log, err error) {
	if opts.Streams < 1 || opts.Streams > 4096 {
		return nil, os.ErrInvalid
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = time.Second
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}
	if err = os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	lock := filepath.Join(dir, "LOCK")
	f, e := os.OpenFile(lock, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	_ = f.Close()
	if handoff.Locked(lock) {
		return nil, errors.New("part log already owned")
	}
	release, e := handoff.LockExclusive(lock)
	if e != nil {
		return nil, e
	}
	l := &Log{dir: dir, opts: opts, unlock: release, errors: make(chan error, opts.Streams)}
	defer func() {
		if err != nil {
			for _, s := range l.streams {
				if s.file != nil {
					_ = s.file.Close()
				}
			}
			_ = release()
		}
	}()
	type identity struct {
		ID      string
		Streams int
		Version int
	}
	path := filepath.Join(dir, "IDENTITY")
	data, e := os.ReadFile(path)
	var ident identity
	if errors.Is(e, os.ErrNotExist) {
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			return nil, err
		}
		ident = identity{hex.EncodeToString(id[:]), opts.Streams, 1}
		data, _ = json.Marshal(ident)
		f, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(data)
		err = errors.Join(err, f.Sync(), f.Close(), syncDir(dir))
		if err != nil {
			return nil, err
		}
	} else if e != nil {
		return nil, e
	} else if err = json.Unmarshal(data, &ident); err != nil {
		return nil, err
	}
	if ident.Streams != opts.Streams || ident.Version != 1 || len(ident.ID) != 32 {
		return nil, errors.New("part log identity/config mismatch")
	}
	l.identity = ident.ID
	for id := 0; id < opts.Streams; id++ {
		s := &stream{log: l, id: id, requests: make(chan request, 2), done: make(chan struct{})}
		l.streams = append(l.streams, s)
		if err = s.recover(); err != nil {
			return nil, err
		}
	}
	for _, s := range l.streams {
		go s.run()
	}
	return l, nil
}
func (l *Log) Identity() string { return l.identity }

// Errors reports each stream's first fatal write or background sync failure.
// The daemon must surface it and fail intake; no producer may silently drop it.
func (l *Log) Errors() <-chan error { return l.errors }
func (s *stream) setFailure(e error) {
	if e != nil && s.failure == nil {
		s.failure = e
		s.log.errors <- fmt.Errorf("part log stream %d: %w", s.id, e)
	}
}
func (l *Log) call(id int, r request) (uint64, error) {
	if id < 0 || id >= len(l.streams) {
		return 0, os.ErrInvalid
	}
	s := l.streams[id]
	r.done = make(chan answer, 1)
	select {
	case <-s.done:
		return 0, ErrClosed
	case s.requests <- r:
	}
	select {
	case a := <-r.done:
		return a.cursor, a.err
	case <-s.done:
		select {
		case a := <-r.done:
			return a.cursor, a.err
		default:
			return 0, ErrClosed
		}
	}
}

// Append acknowledges the write, before group sync; its payload is borrowed
// until return. Call the normal ingest consumer only after this succeeds.
// Durable parts may reach this cursor before the next log sync.
func (l *Log) Append(id int, clock int64, payload []byte) (uint64, error) {
	if len(payload) == 0 || len(payload) > maxPayload {
		return 0, os.ErrInvalid
	}
	return l.call(id, request{op: "append", record: Record{Clock: clock, Payload: payload}})
}

// Sync is an explicit durability barrier. Ordinary intake uses periodic sync.
func (l *Log) Sync() error {
	var err error
	for id := range l.streams {
		_, e := l.call(id, request{op: "sync"})
		err = errors.Join(err, e)
	}
	return err
}

// Replay skips complete records at or below from. The callback must complete
// synchronously, preserve its clock/boundary and propagate errors; it must not
// append to this same stream. Replaying a prefix already in parts is harmless
// only when the consumer atomically persists data and its cursor.
func (l *Log) Replay(id int, from uint64, apply func(Record) error) error {
	if apply == nil {
		return os.ErrInvalid
	}
	_, e := l.call(id, request{op: "replay", from: from, apply: apply})
	return e
}

// Truncate takes one cursor vector per storage shard. Every vector must include
// every producer, even idle ones. Only sealed segments completely below the
// minimum frontier are retired. Callers publish frontiers only with durable data.
func (l *Log) Truncate(frontiers [][]uint64) error {
	if len(frontiers) == 0 {
		return os.ErrInvalid
	}
	low := make([]uint64, len(l.streams))
	for i := range low {
		low[i] = ^uint64(0)
	}
	for _, v := range frontiers {
		if len(v) != len(low) {
			return os.ErrInvalid
		}
		for i, c := range v {
			low[i] = min(low[i], c)
		}
	}
	var err error
	for i, c := range low {
		_, e := l.call(i, request{op: "trim", from: c})
		err = errors.Join(err, e)
	}
	return err
}
func (l *Log) Close() error {
	l.closeOnce.Do(func() {
		for id := range l.streams {
			_, e := l.call(id, request{op: "close"})
			l.closeErr = errors.Join(l.closeErr, e)
		}
		l.closeErr = errors.Join(l.closeErr, l.unlock())
	})
	return l.closeErr
}
func (s *stream) hook(phase string) {
	if s.log.opts.Hook != nil {
		s.log.opts.Hook(s.id, phase)
	}
}
func (s *stream) sync() error {
	if s.failure != nil {
		return s.failure
	}
	if s.file == nil || s.durable == s.last {
		return nil
	}
	s.hook("before-sync")
	if e := s.file.Sync(); e != nil {
		return e
	}
	s.durable = s.last
	s.hook("after-sync")
	return nil
}
func (s *stream) run() {
	defer close(s.done)
	tick := time.NewTicker(s.log.opts.SyncInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if e := s.sync(); e != nil {
				s.setFailure(e)
			}
		case r := <-s.requests:
			var e error
			switch r.op {
			case "append":
				if s.failure != nil {
					e = s.failure
				} else {
					e = s.append(r.record)
				}
			case "sync":
				e = s.sync()
			case "replay":
				e = s.replay(r.from, r.apply)
			case "trim":
				e = s.trim(r.from)
			case "close":
				e = s.sync()
				e = errors.Join(e, s.file.Close())
				r.done <- answer{s.last, e}
				return
			}
			if e != nil && r.op != "replay" && r.op != "trim" {
				s.setFailure(e)
			}
			r.done <- answer{s.last, e}
		}
	}
}
func (s *stream) rotate() error {
	if s.file != nil {
		if e := s.sync(); e != nil {
			return e
		}
		if e := s.file.Close(); e != nil {
			return e
		}
	}
	path := filepath.Join(s.log.dir, fmt.Sprintf("stream-%04d-%020d.plog", s.id, s.last+1))
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	// The durable header preserves the next cursor even if all preceding files
	// are retired and this segment receives no records before a crash.
	var h [headerSize]byte
	binary.LittleEndian.PutUint32(h[:], magic)
	binary.LittleEndian.PutUint64(h[8:], s.last+1)
	identityBytes, _ := hex.DecodeString(s.log.identity)
	copy(h[16:], identityBytes)
	_, e = f.Write(h[:])
	e = errors.Join(e, f.Sync(), syncDir(s.log.dir))
	if e != nil {
		_ = f.Close()
		return e
	}
	s.file = f
	s.segments = append(s.segments, segment{path, s.last + 1, s.last, headerSize})
	return nil
}
func (s *stream) append(r Record) error {
	if s.segments[len(s.segments)-1].size+int64(headerSize+len(r.Payload)) > s.log.opts.SegmentBytes && s.segments[len(s.segments)-1].last >= s.segments[len(s.segments)-1].first {
		if e := s.rotate(); e != nil {
			return e
		}
	}
	b := make([]byte, headerSize+len(r.Payload))
	binary.LittleEndian.PutUint32(b, magic)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(r.Payload)))
	binary.LittleEndian.PutUint64(b[8:], s.last+1)
	binary.LittleEndian.PutUint64(b[16:], uint64(r.Clock))
	copy(b[headerSize:], r.Payload)
	crc := crc32.Update(crc32.Checksum(b[:24], checksum), checksum, r.Payload)
	binary.LittleEndian.PutUint32(b[24:], crc)
	if n, e := s.file.Write(b); e != nil {
		return e
	} else if n != len(b) {
		return io.ErrShortWrite
	}
	s.last++
	seg := &s.segments[len(s.segments)-1]
	seg.last = s.last
	seg.size += int64(len(b))
	s.hook("after-write")
	return nil
}

// scan validates complete frames before callbacks. An incomplete last frame may
// be discarded only in the newest file; checksum failures always fail closed.
func (s *stream) scan(path string, tail bool, apply func(Record) error) (segment, error) {
	f, e := os.OpenFile(path, os.O_RDWR, 0600)
	if e != nil {
		return segment{}, e
	}
	defer f.Close()
	seg := segment{path: path, size: headerSize}
	var h [headerSize]byte
	if _, e = io.ReadFull(f, h[:]); e != nil {
		return seg, e
	}
	identityBytes, _ := hex.DecodeString(s.log.identity)
	if binary.LittleEndian.Uint32(h[:]) != magic || !slices.Equal(h[16:], identityBytes) {
		return seg, errors.New("part log segment identity mismatch")
	}
	seg.first = binary.LittleEndian.Uint64(h[8:])
	seg.last = seg.first - 1
	for {
		n, e := io.ReadFull(f, h[:])
		if e == io.EOF {
			break
		}
		if e != nil {
			if tail && n < headerSize {
				return seg, errors.Join(f.Truncate(seg.size), f.Sync())
			}
			return seg, e
		}
		length := binary.LittleEndian.Uint32(h[4:])
		cursor := binary.LittleEndian.Uint64(h[8:])
		if binary.LittleEndian.Uint32(h[:]) != magic || length == 0 || length > maxPayload || cursor != seg.last+1 {
			return seg, errors.New("invalid part log record")
		}
		payload := make([]byte, length)
		if _, e = io.ReadFull(f, payload); e != nil {
			if tail && (e == io.EOF || e == io.ErrUnexpectedEOF) {
				return seg, errors.Join(f.Truncate(seg.size), f.Sync())
			}
			return seg, e
		}
		crc := crc32.Update(crc32.Checksum(h[:24], checksum), checksum, payload)
		if binary.LittleEndian.Uint32(h[24:]) != crc {
			return seg, errors.New("part log checksum mismatch")
		}
		if apply != nil {
			if e = apply(Record{cursor, int64(binary.LittleEndian.Uint64(h[16:])), payload}); e != nil {
				return seg, e
			}
		}
		seg.last = cursor
		seg.size += int64(headerSize + length)
	}
	if tail {
		return seg, f.Sync()
	}
	return seg, nil
}
func (s *stream) recover() error {
	paths, e := filepath.Glob(filepath.Join(s.log.dir, fmt.Sprintf("stream-%04d-*.plog", s.id)))
	if e != nil {
		return e
	}
	slices.Sort(paths)
	if len(paths) > 0 {
		p := paths[len(paths)-1]
		info, err := os.Stat(p)
		if err != nil {
			return err
		}
		if info.Size() < headerSize {
			if err = inputlog.RetireSegment(p, os.Remove, syncDir); err != nil {
				return err
			}
			paths = paths[:len(paths)-1]
		}
	}
	for i, p := range paths {
		seg, e := s.scan(p, i == len(paths)-1, nil)
		if e != nil {
			return e
		}
		if i > 0 && seg.first != s.last+1 {
			return errors.New("part log sequence gap")
		}
		s.segments = append(s.segments, seg)
		s.last = seg.last
	}
	s.durable = s.last
	// Reuse an empty last segment. Creating another with the same start would
	// collide after a restart that occurred before its first append.
	if len(s.segments) > 0 {
		seg := s.segments[len(s.segments)-1]
		if seg.last < seg.first {
			f, e := os.OpenFile(seg.path, os.O_RDWR|os.O_APPEND, 0600)
			s.file = f
			return e
		}
	}
	return s.rotate()
}
func (s *stream) replay(from uint64, apply func(Record) error) error {
	if from > s.last {
		return errors.New("parts frontier exceeds journal tail")
	}
	if len(s.segments) > 0 && from+1 < s.segments[0].first {
		return errors.New("parts frontier precedes retained journal")
	}
	for _, seg := range s.segments {
		if seg.last <= from {
			continue
		}
		_, e := s.scan(seg.path, false, func(r Record) error {
			if r.Cursor <= from {
				return nil
			}
			return apply(r)
		})
		if e != nil {
			return e
		}
	}
	return nil
}
func (s *stream) trim(frontier uint64) error {
	if frontier > s.last {
		return errors.New("frontier exceeds journal tail")
	}
	for len(s.segments) > 1 && s.segments[0].last < frontier {
		seg := s.segments[0]
		s.hook("before-retire")
		if e := inputlog.RetireSegment(seg.path, os.Remove, syncDir); e != nil {
			return e
		}
		s.segments = s.segments[1:]
	}
	return nil
}

// Batch is one original apply group. PolicyEpoch refers to an immutable policy
// catalogue that must itself be durably recoverable. Canonical metric spelling
// is recorded directly; encounter-order IDs are not portable identities.
type Batch struct {
	Metric      string
	PolicyEpoch uint64
	Points      []points.Point
}

// EncodeBatch preserves every float bit, timestamp and apply-group boundary.
func EncodeBatch(b Batch) ([]byte, error) {
	if len(b.Metric) == 0 || len(b.Metric) > 1<<20 || len(b.Points) == 0 || len(b.Points) > (maxPayload-16-len(b.Metric))/16 {
		return nil, os.ErrInvalid
	}
	out := make([]byte, 16+len(b.Metric)+16*len(b.Points))
	binary.LittleEndian.PutUint64(out, b.PolicyEpoch)
	binary.LittleEndian.PutUint32(out[8:], uint32(len(b.Metric)))
	binary.LittleEndian.PutUint32(out[12:], uint32(len(b.Points)))
	copy(out[16:], b.Metric)
	for i, p := range b.Points {
		o := 16 + len(b.Metric) + 16*i
		binary.LittleEndian.PutUint64(out[o:], math.Float64bits(p.Value))
		binary.LittleEndian.PutUint64(out[o+8:], uint64(p.Timestamp))
	}
	return out, nil
}

// DecodeBatch owns its returned memory. The surrounding Record carries Clock.
func DecodeBatch(payload []byte) (Batch, error) {
	if len(payload) < 16 || len(payload) > maxPayload {
		return Batch{}, os.ErrInvalid
	}
	name := uint64(binary.LittleEndian.Uint32(payload[8:]))
	count := uint64(binary.LittleEndian.Uint32(payload[12:]))
	if name == 0 || name > 1<<20 || count == 0 || 16+name+16*count != uint64(len(payload)) {
		return Batch{}, os.ErrInvalid
	}
	b := Batch{Metric: string(payload[16 : 16+name]), PolicyEpoch: binary.LittleEndian.Uint64(payload), Points: make([]points.Point, count)}
	for i := range b.Points {
		o := 16 + int(name) + 16*i
		b.Points[i] = points.Point{Value: math.Float64frombits(binary.LittleEndian.Uint64(payload[o:])), Timestamp: int64(binary.LittleEndian.Uint64(payload[o+8:]))}
	}
	return b, nil
}
