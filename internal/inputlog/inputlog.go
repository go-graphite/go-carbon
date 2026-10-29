// Package inputlog keeps a continuous log of the points accepted into the
// cache, so that an unclean exit loses at most about one sync interval of
// input instead of every point not yet persisted.
//
// The cache records each accepted point in a buffer of the shard that accepts
// it, under the shard lock it already holds. Once per sync interval the log
// takes those buffers, encodes them as compressed frames on a few workers,
// appends the frames to a preallocated segment with one write, and makes them
// durable with one fdatasync (group commit). Frames refer to metric names by
// ids of a dictionary each segment builds from its own frames, and encode
// points against the previous point of the same metric (see codecStream).
//
// Each sync cycle is a generation. The cache tracks, for every cached or
// in-flight item, the generation of its oldest point not yet persisted; the
// minimum over all items is the low-water mark. A segment holding only older
// generations is recycled, and every frame records where replay must start.
// See Source for the cache side.
package inputlog

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

// Entry is one accepted point. The cache appends entries under its shard
// lock; Ref is the cached item's.
type Entry struct {
	Ref       *StreamRef
	Value     float64
	Timestamp int64
	// cached is Ref's stream id when the entry was made, see StreamRef.
	cached uint64
}

// StreamRef names a metric for the log. The cache keeps one per item, so the
// log can remember the item's id in the stream that last encoded it: an
// entry carries that id, and the encoder uses it while the stream lasts
// instead of looking the name up for every point. Only the log writes the
// id, once per item and stream; the cache only reads it.
type StreamRef struct {
	Metric string
	state  atomic.Uint64 // stream epoch << 32 | id
}

// NewStreamRef returns a reference to metric with no stream id.
func NewStreamRef(metric string) *StreamRef { return &StreamRef{Metric: metric} }

// Entry records a point of r's metric.
func (r *StreamRef) Entry(value float64, timestamp int64) Entry {
	return Entry{Ref: r, Value: value, Timestamp: timestamp, cached: r.state.Load()}
}

// Metric returns the entry's metric name.
func (e Entry) Metric() string { return e.Ref.Metric }

// Source is the cache side of the log. The cache records each point at its
// shard's current generation and tracks, for every item not yet persisted,
// the generation of its oldest point.
type Source interface {
	// TakeLogEntries returns the entries shard recorded since the previous
	// call, keeps spare (empty) for the next ones, and starts generation gen.
	TakeLogEntries(shard int, gen uint64, spare []Entry) []Entry
	// LogLowWater returns the oldest generation that may still hold a point
	// the persister has not confirmed.
	LogLowWater() uint64
	// RelogBefore records again, at the current generation, unpersisted
	// points first recorded before gen, about budget of them at most. It
	// returns how many points it recorded.
	RelogBefore(gen uint64, budget int) int
	// LogDropped counts points not recorded because a shard buffer was full.
	LogDropped() uint64
}

type Options struct {
	// SyncInterval is the group commit period. A crash loses at most the
	// input of one interval plus one cycle's encode and write time.
	SyncInterval time.Duration
	// SegmentSize is the preallocated size of each segment file. Every
	// segment defines the names it uses once, so it should hold at least a
	// minute of input.
	SegmentSize int64
	// MaxAge re-records points still unpersisted after this long, so a write
	// that keeps failing cannot pin old segments forever. The log holds at
	// most about MaxAge+TruncateLag of input.
	MaxAge time.Duration
	// TruncateLag delays reuse of a segment after its points are confirmed:
	// a confirmed point may still be in the page cache or an unsynced store
	// WAL. Zero protects against process crashes only.
	TruncateLag time.Duration
	// Workers encode frames in parallel; zero selects a quarter of the CPUs,
	// at most 8.
	Workers int
	Logger  *zap.Logger
	// ObserveCycle, if set, is called on the log's goroutine with the
	// duration of every cycle that committed points: collect to sync.
	ObserveCycle func(time.Duration)
}

const (
	logSuffix   = ".log"
	spareSuffix = ".spare"
	// partitions splits each cycle's shards into independently replayable
	// frame streams. It is part of the format: replay runs one stream per
	// worker, preserving each stream's order.
	partitions = 16
	// maxFrameRaw bounds a frame before compression, and so the memory a
	// worker holds.
	maxFrameRaw = 4 << 20
	// maxSpares bounds recycled segments kept beyond the live ones.
	maxSpares = 4
	// relogBudget bounds the points one cycle re-records.
	relogBudget = 1 << 21
)

var ErrClosed = errors.New("input log closed")

type segment struct {
	seq, salt uint64
	path      string
	file      *os.File
	size      int64
	end       int64 // offset after the last frame
	synced    int64 // offset up to which writes are durable
	maxGen    uint64
}

type frameRef struct {
	seg *segment
	off int64
	h   frameHeader
}

type tick struct {
	gen uint64
	pos position
	at  time.Time // when the cycle committed
}

type sample struct {
	at  time.Time
	low uint64
}

// partitionBuffer holds one partition's stream state and its encoded frames
// for the current cycle.
type partitionBuffer struct {
	enc    streamEncoder
	out    []byte // compressed payloads, back to back
	frames []frameMeta
}

// workerBuffer is an encoding worker's memory.
type workerBuffer struct {
	streamScratch
	raw []byte
}

type frameMeta struct {
	start int
	h     frameHeader
}

type Log struct {
	dir    string
	opts   Options
	logger *zap.Logger
	unlock func() error

	// Valid segments found by Open, and their frames from the replay start.
	// The frames before it in the same segment define names the pending
	// frames use.
	recovered []*segment
	prefix    []frameRef
	pending   []frameRef
	nextSeq   uint64

	// Owned by the cycle goroutine after Start.
	src       Source
	shardsPer int
	gen       uint64
	active    *segment
	live      []*segment // oldest first; active is last
	spares    []string
	ticks     []tick // first frame of every cycle that wrote, oldest first
	lowWaters []sample
	durable   uint64 // low-water old enough for TruncateLag
	spare     [][]Entry
	parts     [partitions]partitionBuffer
	workers   []workerBuffer
	steady    int64 // bytes of the last cycle that did not start a segment
	wbuf      []byte
	wseg      *segment
	woff      int64
	dirty     []*segment
	enc       *zstd.Encoder

	syncs   chan chan error
	copies  chan copyRequest
	copyPin *Position
	stop    chan struct{}
	done    chan struct{}
	started bool
	closed  bool

	stat  stats
	total totals
	// spareBytes holds the size of each spare, by path; owned like spares.
	spareBytes map[string]int64
}

// totals are cumulative counters and gauges for Metrics, updated per frame,
// write or sync and never per point.
type totals struct {
	bytes, points, syncs, writeErrors, relogged, unlogged atomic.Int64
	replayed, replayNanos                                 atomic.Int64
	diskBytes, oldestNanos                                atomic.Int64
	cycles                                                atomic.Int64
	stageNanos                                            [numStages]atomic.Int64
}

// Stages of a cycle, timed for Metrics. Encoding stages are summed over the
// workers that run them in parallel.
const (
	// StageCollect takes the shards' recorded entries.
	StageCollect = iota
	// StageEncode assigns name ids, sorts rows and builds the columns.
	StageEncode
	// StageCompress compresses frames.
	StageCompress
	// StageWrite writes frames to the segment.
	StageWrite
	// StageSync syncs segment data.
	StageSync
	// StageRoll creates or recycles a segment.
	StageRoll
	// StageRelog records again points older than MaxAge.
	StageRelog
	// StageTruncate recycles persisted segments and refreshes gauges.
	StageTruncate
	numStages
)

// StageNames name the stages, by index.
var StageNames = [numStages]string{"collect", "encode", "compress", "write", "sync", "roll", "relog", "truncate"}

func (l *Log) stage(s int, d time.Duration) { l.total.stageNanos[s].Add(int64(d)) }

// Metrics are the log's cumulative counters since Open, and gauges.
type Metrics struct {
	// Cycles counts group commit cycles, and Stages their time by stage.
	Cycles                                          int64
	Stages                                          [numStages]time.Duration
	BytesWritten, PointsWritten, Syncs, WriteErrors int64
	// Relogged counts points recorded again after MaxAge; Unlogged, points
	// a full shard buffer did not record.
	Relogged, Unlogged int64
	// ReplayedPoints and ReplayDuration describe the replay before Start.
	ReplayedPoints int64
	ReplayDuration time.Duration
	// Segments counts segments in use; DiskBytes, the bytes of every
	// segment and spare file.
	Segments, DiskBytes int64
	// OldestUnpersisted is the age of the oldest cycle still holding a
	// point not yet persisted, zero when none.
	OldestUnpersisted time.Duration
}

// Metrics reports the log's counters. It is safe to call concurrently.
func (l *Log) Metrics() Metrics {
	var stages [numStages]time.Duration
	for i := range stages {
		stages[i] = time.Duration(l.total.stageNanos[i].Load())
	}
	return Metrics{
		Cycles: l.total.cycles.Load(), Stages: stages,
		BytesWritten: l.total.bytes.Load(), PointsWritten: l.total.points.Load(), Syncs: l.total.syncs.Load(),
		WriteErrors: l.total.writeErrors.Load(), Relogged: l.total.relogged.Load(), Unlogged: l.total.unlogged.Load(),
		ReplayedPoints: l.total.replayed.Load(), ReplayDuration: time.Duration(l.total.replayNanos.Load()),
		Segments: l.stat.liveSegments.Load(), DiskBytes: l.total.diskBytes.Load(),
		OldestUnpersisted: time.Duration(l.total.oldestNanos.Load()),
	}
}

type stats struct {
	bytes, frames, writes, syncs, syncNanos, points  atomic.Int64
	created, recycled, removed, errors, relogged     atomic.Int64
	liveSegments, spareSegments, pinned, lastDropped atomic.Int64
}

// Open locks dir, waiting while a predecessor holds it, and finds the points
// to replay. Call Replay, then Start.
func Open(dir string, opts Options) (*Log, error) {
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = time.Second
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 4 << 30
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = 30 * time.Minute
	}
	if opts.Workers <= 0 {
		opts.Workers = min(max(runtime.GOMAXPROCS(0)/4, 1), 8)
	}
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(dir, "LOCK")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	_ = lockFile.Close()
	if handoff.Locked(lockPath) {
		opts.Logger.Info("waiting for the previous instance to release the input log", zap.String("path", dir))
	}
	unlock, err := handoff.LockExclusive(lockPath)
	if err != nil {
		return nil, err
	}
	l := &Log{dir: dir, opts: opts, logger: opts.Logger, unlock: unlock, nextSeq: 1}
	if err = l.scan(); err != nil {
		l.closeFiles()
		_ = unlock()
		return nil, err
	}
	return l, nil
}

// scan classifies the files of dir by their headers, not their names: a
// rename need not be durable for a segment's frames to be found.
func (l *Log) scan() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || (!strings.HasSuffix(name, logSuffix) && !strings.HasSuffix(name, spareSuffix)) {
			continue
		}
		path := filepath.Join(l.dir, name)
		seg, err := openSegment(path)
		if err != nil {
			return err
		}
		if seg == nil {
			l.spares = append(l.spares, path)
			if info, err := os.Stat(path); err == nil {
				l.spareSize(path, info.Size())
			}
			continue
		}
		l.recovered = append(l.recovered, seg)
		l.nextSeq = max(l.nextSeq, seg.seq+1)
	}
	slices.SortFunc(l.recovered, func(a, b *segment) int { return compareSeq(a.seq, b.seq) })
	var frames []frameRef
	for _, seg := range l.recovered {
		if frames, err = seg.scanFrames(frames); err != nil {
			return err
		}
	}
	if len(frames) == 0 {
		return nil
	}
	from := frames[len(frames)-1].h.replayFrom
	i, _ := slices.BinarySearchFunc(frames, from, func(f frameRef, p position) int {
		switch q := (position{f.seg.seq, f.off}); {
		case q.less(p):
			return -1
		case p.less(q):
			return 1
		}
		return 0
	})
	j := i
	for j > 0 && frames[j-1].seg == frames[i].seg {
		j--
	}
	l.prefix, l.pending = frames[j:i], frames[i:]
	return nil
}

func compareSeq(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// openSegment returns nil for a file without a valid segment header.
func openSegment(path string) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	header := make([]byte, segmentHeaderSize)
	if n, _ := f.ReadAt(header, 0); n < segmentHeaderSize {
		_ = f.Close()
		return nil, nil
	}
	h, ok := parseSegmentHeader(header)
	if !ok {
		_ = f.Close()
		return nil, nil
	}
	return &segment{seq: h.seq, salt: h.salt, path: path, file: f, size: info.Size(), end: segmentHeaderSize, synced: info.Size()}, nil
}

// scanFrames appends the segment's valid frames, stopping at the first frame
// that is torn, zero, or left from an earlier use of the file.
func (s *segment) scanFrames(frames []frameRef) ([]frameRef, error) {
	header := make([]byte, frameHeaderSize)
	for s.end+frameHeaderSize <= s.size {
		if _, err := s.file.ReadAt(header, s.end); err != nil {
			return frames, err
		}
		h, ok := parseFrameHeader(header)
		if !ok || h.salt != s.salt || (h.codec != codecRecords && h.codec != codecStream) ||
			h.partition >= partitions || h.rawLength > 2*maxFrameRaw ||
			int64(h.length) > s.size-s.end-frameHeaderSize {
			break
		}
		frames = append(frames, frameRef{seg: s, off: s.end, h: h})
		s.end += frameHeaderSize + int64(h.length)
	}
	return frames, nil
}

// NeedsReplay reports whether the log holds points not known to be persisted.
func (l *Log) NeedsReplay() bool { return len(l.pending) > 0 }

// Start begins group commit for the shards of src. Points Replay found are
// kept until src reports them persisted at generation zero.
func (l *Log) Start(src Source, shards int) {
	l.src = src
	l.shardsPer = max(shards/partitions, 1)
	l.spare = make([][]Entry, l.shardsPer*partitions)
	l.workers = make([]workerBuffer, l.opts.Workers)
	l.enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(l.opts.Workers), zstd.WithEncoderCRC(false))
	l.adoptRecovered()
	l.syncs, l.stop, l.done = make(chan chan error), make(chan struct{}), make(chan struct{})
	l.copies = make(chan copyRequest)
	l.started = true
	go l.run()
}

// adoptRecovered keeps the segments from the replay start on, pinned at
// generation zero with the items Replay loaded, and recycles the rest.
func (l *Log) adoptRecovered() {
	first := uint64(0)
	if len(l.pending) > 0 {
		first = l.pending[0].seg.seq
		l.ticks = append(l.ticks, tick{gen: 0, pos: position{first, l.pending[0].off}, at: time.Now()})
	}
	for _, seg := range l.recovered {
		if len(l.pending) > 0 && seg.seq >= first {
			l.live = append(l.live, seg)
		} else {
			l.free(seg)
		}
	}
	l.recovered, l.prefix, l.pending = nil, nil, nil
}

func (l *Log) run() {
	phasecpu.Set(phasecpu.InputLog)
	defer phasecpu.Set(phasecpu.Other)
	defer close(l.done)
	ticker := time.NewTicker(l.opts.SyncInterval)
	defer ticker.Stop()
	for {
		var reply chan error
		select {
		case <-ticker.C:
		case reply = <-l.syncs:
		case request := <-l.copies:
			l.copyRequest(request)
			continue
		case <-l.stop:
			return
		}
		err := l.cycle()
		if reply != nil {
			reply <- err
		}
	}
}

// Sync runs a cycle now and waits until every point recorded before the call
// is durable.
func (l *Log) Sync() error {
	if !l.started {
		return ErrClosed
	}
	reply := make(chan error, 1)
	select {
	case l.syncs <- reply:
		return <-reply
	case <-l.done:
		return ErrClosed
	}
}

// Close makes every recorded point durable and releases the directory. The
// segments stay for the next instance to replay.
func (l *Log) Close() error {
	if l.closed {
		return nil
	}
	var err error
	if l.started {
		err = l.Sync()
	}
	l.shutdown()
	return errors.Join(err, l.unlock())
}

// Retire deletes every segment, then releases the directory. Call it once a
// graceful dump durably holds every point that is not yet persisted.
func (l *Log) Retire() error {
	if l.closed {
		return nil
	}
	l.shutdown()
	entries, err := os.ReadDir(l.dir)
	for _, e := range entries {
		if name := e.Name(); strings.HasSuffix(name, logSuffix) || strings.HasSuffix(name, spareSuffix) {
			err = errors.Join(err, os.Remove(filepath.Join(l.dir, name)))
		}
	}
	return errors.Join(err, syncDir(l.dir), l.unlock())
}

// Kill stops the log as a crash would: without a final cycle, keeping only
// what earlier cycles wrote. Tests use it to simulate kill -9.
func (l *Log) Kill() {
	if !l.closed {
		l.shutdown()
		_ = l.unlock()
	}
}

func (l *Log) shutdown() {
	l.closed = true
	if l.started {
		close(l.stop)
		<-l.done
		l.enc.Close()
	}
	l.closeFiles()
}

func (l *Log) closeFiles() {
	for _, seg := range slices.Concat(l.recovered, l.live) {
		_ = seg.file.Close()
	}
}

// cycle commits one generation: everything the shards recorded before
// switching to the next one. A cycle's frames all go to one segment.
func (l *Log) cycle() error {
	started := time.Now()
	gen := l.gen
	l.gen++
	var err error
	seq := l.target()
	if l.encode(gen, seq) > 0 {
		err = l.write(gen, seq)
		if l.opts.ObserveCycle != nil {
			l.opts.ObserveCycle(time.Since(started))
		}
	}
	truncated := time.Now()
	relog := l.truncate()
	l.stage(StageRelog, relog)
	l.stage(StageTruncate, time.Since(truncated)-relog)
	l.total.cycles.Add(1)
	return err
}

// target returns the segment for the next cycle's frames: the active one
// while a cycle like the last ordinary one fits, else the next to create.
// Frames continue their partition's stream in that segment, so a cycle that
// turns out larger runs past the preallocated size instead of rolling.
func (l *Log) target() uint64 {
	if l.active != nil && l.active.end+l.steady+l.steady/4 <= l.active.size {
		return l.active.seq
	}
	return l.nextSeq
}

func (l *Log) encode(gen, seq uint64) int {
	var next, total atomic.Int64
	var wg sync.WaitGroup
	for w := range l.workers {
		wg.Go(func() {
			for p := int(next.Add(1) - 1); p < partitions; p = int(next.Add(1) - 1) {
				total.Add(int64(l.encodePartition(p, gen, seq, &l.workers[w])))
			}
		})
	}
	wg.Wait()
	return int(total.Load())
}

func (l *Log) encodePartition(p int, gen, seq uint64, w *workerBuffer) int {
	part := &l.parts[p]
	part.out, part.frames = part.out[:0], part.frames[:0]
	part.enc.begin(seq)
	started := time.Now()
	var collect, compress time.Duration
	emit := func(raw []byte, rows int) []byte {
		at := time.Now()
		l.compress(part, p, raw, rows)
		compress += time.Since(at)
		return raw
	}
	defer func() {
		l.stage(StageCollect, collect)
		l.stage(StageCompress, compress)
		l.stage(StageEncode, time.Since(started)-collect-compress)
	}()
	total := 0
	for shard := p * l.shardsPer; shard < (p+1)*l.shardsPer; shard++ {
		at := time.Now()
		entries := l.src.TakeLogEntries(shard, gen+1, l.spare[shard])
		collect += time.Since(at)
		for _, e := range entries {
			part.enc.add(&w.streamScratch, e)
		}
		// A segment's first cycle defines every name: bound their bytes too.
		if len(w.rows) >= maxSortRows || len(w.names) >= 2*maxFrameRaw {
			w.raw = part.enc.flush(&w.streamScratch, w.raw, emit)
		}
		total += len(entries)
		// Drop the names so a popped item is not retained by its log record.
		// The segment's dictionary keeps them until the next segment.
		clear(entries)
		l.spare[shard] = entries[:0]
	}
	w.raw = part.enc.flush(&w.streamScratch, w.raw, emit)
	return total
}

func (l *Log) compress(part *partitionBuffer, p int, raw []byte, rows int) {
	start := len(part.out)
	part.out = l.enc.EncodeAll(raw, part.out)
	payload := part.out[start:]
	part.frames = append(part.frames, frameMeta{start: start, h: frameHeader{
		codec: codecStream, partition: uint16(p), length: uint32(len(payload)), rawLength: uint32(len(raw)),
		points: uint32(rows), crc: crc32.Checksum(payload, castagnoli),
	}})
}

// write appends the cycle's frames to segment seq with one write, then syncs
// it. A new segment's header goes out with them.
func (l *Log) write(gen, seq uint64) error {
	var errs error
	if l.active == nil || l.active.seq != seq {
		errs = errors.Join(l.flush(), l.syncDirty(), l.roll())
	}
	seg := l.active
	if seg == nil {
		// The frames are lost; the cache still has their points.
		for p := range l.parts {
			l.stat.errors.Add(int64(len(l.parts[p].frames)))
		}
		l.logger.Error("input log write failed; points of this cycle are not durable", zap.Error(errs))
		l.total.writeErrors.Add(1)
		return errs
	}
	first := position{seg.seq, seg.end}
	from := first
	if len(l.ticks) > 0 {
		from = l.ticks[0].pos
	}
	if l.copyPin != nil {
		p := position{l.copyPin.Segment, l.copyPin.Offset}
		if p.less(from) {
			from = p
		}
	}
	rolled := seg.end == segmentHeaderSize
	for p := range l.parts {
		part := &l.parts[p]
		for _, f := range part.frames {
			payload := part.out[f.start : f.start+int(f.h.length)]
			h := f.h
			h.salt, h.replayFrom = seg.salt, from
			l.wbuf = h.append(l.wbuf)
			l.wbuf = append(l.wbuf, payload...)
			seg.end += int64(frameHeaderSize + len(payload))
			l.stat.frames.Add(1)
			l.stat.points.Add(int64(h.points))
			l.total.points.Add(int64(h.points))
		}
	}
	seg.maxGen = gen
	if !rolled {
		l.steady = seg.end - first.off
	}
	errs = errors.Join(errs, l.flush(), l.syncDirty())
	l.ticks = append(l.ticks, tick{gen: gen, pos: first, at: time.Now()})
	if errs != nil {
		l.total.writeErrors.Add(1)
		l.logger.Error("input log write failed; points of this cycle are not durable", zap.Error(errs))
	}
	return errs
}

// flush issues the buffered bytes as one positioned write.
func (l *Log) flush() error {
	if len(l.wbuf) == 0 || l.wseg == nil {
		l.wbuf = l.wbuf[:0]
		return nil
	}
	seg := l.wseg
	defer func(at time.Time) { l.stage(StageWrite, time.Since(at)) }(time.Now())
	_, err := seg.file.WriteAt(l.wbuf, l.woff)
	l.stat.writes.Add(1)
	l.stat.bytes.Add(int64(len(l.wbuf)))
	l.total.bytes.Add(int64(len(l.wbuf)))
	if !slices.Contains(l.dirty, seg) {
		l.dirty = append(l.dirty, seg)
	}
	l.woff += int64(len(l.wbuf))
	l.wbuf = l.wbuf[:0]
	if err != nil {
		// Later frames must not follow a torn one in this segment: replay
		// would stop before them.
		l.abandon(seg)
		return err
	}
	return nil
}

func (l *Log) syncDirty() error {
	var errs error
	for _, seg := range l.dirty {
		started := time.Now()
		err := datasync(seg.file)
		took := time.Since(started)
		l.stat.syncs.Add(1)
		l.stat.syncNanos.Add(int64(took))
		l.total.syncs.Add(1)
		l.stage(StageSync, took)
		if err != nil {
			l.abandon(seg)
			errs = errors.Join(errs, err)
			continue
		}
		dropCache(seg.file, seg.synced, seg.end-seg.synced)
		seg.synced = seg.end
	}
	l.dirty = l.dirty[:0]
	return errs
}

func (l *Log) abandon(seg *segment) {
	if l.active == seg {
		l.active = nil
	}
	if l.wseg == seg {
		l.wseg = nil
	}
}

// roll starts the segment l.nextSeq, the one target chose. Its header goes
// out with the first write of frames.
func (l *Log) roll() error {
	defer func(at time.Time) { l.stage(StageRoll, time.Since(at)) }(time.Now())
	l.active, l.wseg = nil, nil
	seg, err := l.newSegment(l.opts.SegmentSize)
	if err != nil {
		return err
	}
	l.live = append(l.live, seg)
	l.active, l.wseg, l.woff = seg, seg, 0
	l.wbuf = segmentHeader{seq: seg.seq, salt: seg.salt, created: time.Now().UnixNano()}.append(l.wbuf[:0])
	return nil
}

// newSegment recycles a spare when one is large enough. A recycled file's
// blocks are allocated and written already, so syncing it commits no
// metadata; only a new file's size and allocation are synced once here.
func (l *Log) newSegment(size int64) (*segment, error) {
	seq := l.nextSeq
	l.nextSeq++
	path := filepath.Join(l.dir, fmt.Sprintf("%016x%s", seq, logSuffix))
	seg := &segment{seq: seq, salt: rand.Uint64(), path: path, end: segmentHeaderSize}
	for len(l.spares) > 0 {
		spare := l.spares[len(l.spares)-1]
		l.spares = l.spares[:len(l.spares)-1]
		delete(l.spareBytes, spare)
		f, info, err := reuse(spare, path)
		if err == nil && info.Size() < size {
			err = errors.Join(preallocate(f, size), f.Sync())
		}
		if err != nil {
			if f != nil {
				_ = f.Close()
			}
			_ = os.Remove(path)
			_ = os.Remove(spare)
			continue
		}
		seg.file, seg.size = f, max(size, info.Size())
		l.stat.recycled.Add(1)
		return seg, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	if err = errors.Join(preallocate(f, size), f.Sync(), syncDir(l.dir)); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	seg.file, seg.size = f, size
	l.stat.created.Add(1)
	return seg, nil
}

func reuse(spare, path string) (*os.File, os.FileInfo, error) {
	if err := os.Rename(spare, path); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return f, nil, err
	}
	return f, info, nil
}

// truncate recycles segments whose generations are all persisted, and
// re-records points that have waited longer than MaxAge, returning the time
// that took.
func (l *Log) truncate() (relog time.Duration) {
	now := time.Now()
	low := l.src.LogLowWater()
	l.lowWaters = append(l.lowWaters, sample{now, low})
	for len(l.lowWaters) > 0 && now.Sub(l.lowWaters[0].at) >= l.opts.TruncateLag {
		l.durable = max(l.durable, l.lowWaters[0].low)
		l.lowWaters = l.lowWaters[1:]
	}
	for len(l.ticks) > 0 && l.ticks[0].gen < l.durable {
		l.ticks = l.ticks[1:]
	}
	for l.copyPin == nil && len(l.live) > 0 && l.live[0] != l.active && l.live[0].maxGen < l.durable {
		l.free(l.live[0])
		l.live = l.live[1:]
	}
	if age := max(uint64(l.opts.MaxAge/l.opts.SyncInterval), 2); l.gen > age && low < l.gen-age {
		at := time.Now()
		n := int64(l.src.RelogBefore(l.gen-age, relogBudget))
		relog = time.Since(at)
		l.stat.relogged.Add(n)
		l.total.relogged.Add(n)
	}
	l.updateGauges(now, low)
	l.stat.liveSegments.Store(int64(len(l.live)))
	l.stat.spareSegments.Store(int64(len(l.spares)))
	l.stat.pinned.Store(int64(l.gen - min(low, l.gen)))
	return relog
}

// free keeps a segment for reuse, or removes it beyond maxSpares. Its old
// frames are behind every replay start, so neither step needs to be durable.
func (l *Log) free(seg *segment) {
	_ = seg.file.Close()
	if len(l.spares) < maxSpares {
		spare := filepath.Join(l.dir, fmt.Sprintf("%016x%s", seg.seq, spareSuffix))
		if os.Rename(seg.path, spare) == nil {
			l.spares = append(l.spares, spare)
			l.spareSize(spare, max(seg.size, seg.end))
			return
		}
	}
	_ = os.Remove(seg.path)
	l.stat.removed.Add(1)
}

func (l *Log) spareSize(path string, size int64) {
	if l.spareBytes == nil {
		l.spareBytes = make(map[string]int64)
	}
	l.spareBytes[path] = size
}

// updateGauges refreshes the Metrics gauges once per cycle: disk usage, the
// age of the oldest cycle low still pins, and the cache's unlogged points.
func (l *Log) updateGauges(now time.Time, low uint64) {
	var disk int64
	for _, seg := range l.live {
		disk += max(seg.size, seg.end)
	}
	for _, size := range l.spareBytes {
		disk += size
	}
	l.total.diskBytes.Store(disk)
	var oldest time.Duration
	for _, t := range l.ticks {
		if t.gen >= low {
			oldest = now.Sub(t.at)
			break
		}
	}
	l.total.oldestNanos.Store(int64(oldest))
	l.total.unlogged.Store(int64(l.src.LogDropped()))
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// Stat reports counters since the previous call, and gauges.
func (l *Log) Stat(send func(metric string, value float64)) {
	for _, c := range []struct {
		name string
		v    *atomic.Int64
	}{
		{"bytes", &l.stat.bytes}, {"frames", &l.stat.frames}, {"writes", &l.stat.writes}, {"syncs", &l.stat.syncs},
		{"points", &l.stat.points}, {"segmentsCreated", &l.stat.created}, {"segmentsRecycled", &l.stat.recycled},
		{"segmentsRemoved", &l.stat.removed}, {"errors", &l.stat.errors}, {"relogged", &l.stat.relogged},
	} {
		send(c.name, float64(c.v.Swap(0)))
	}
	send("syncTimeMs", float64(l.stat.syncNanos.Swap(0))/float64(time.Millisecond))
	send("liveSegments", float64(l.stat.liveSegments.Load()))
	send("spareSegments", float64(l.stat.spareSegments.Load()))
	send("pinnedCycles", float64(l.stat.pinned.Load()))
	if l.src != nil {
		dropped := int64(l.src.LogDropped())
		send("dropped", float64(dropped-l.stat.lastDropped.Swap(dropped)))
	}
}
