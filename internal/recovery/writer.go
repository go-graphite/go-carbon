package recovery

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// Writer writes an ordinary legacy binary source and records its exact extents.
// Both cache dump and concurrent input diversion can share one Builder.
type Writer struct {
	mu         sync.Mutex
	file       *os.File
	buffer     *sink
	builder    *Builder
	source     int
	format     Format
	input      *compactInput // compact input files
	inputDone  bool
	scratch    []byte
	encoded    []byte
	closed     bool
	err        error
	points     int64 // accepted by WritePoints as records
	descriptor File
	syncOnce   sync.Once
	syncErr    error
}

// InputFileBuffer sizes the buffers of input files: the WAL a dump stop
// diverts input to and the handoff tail. Each buffer costs a dispatch to the
// writers and hashers; with 4 KB ones, 8 receivers wrote one-point records at
// about 1M points/s, with 64 KB ones at 2-2.5M.
const InputFileBuffer = 64 << 10

func NewWriter(path string, source, bufferSize int, builder *Builder) (*Writer, error) {
	return NewWriterFormat(path, source, bufferSize, builder, FormatLegacy)
}

// NewWriterFormat writes records of format f. A compact input file (source
// 1) needs the checkpoint's builder: its rows refer to builder ids.
func NewWriterFormat(path string, source, bufferSize int, builder *Builder, f Format) (*Writer, error) {
	if f == FormatCompact && source == 1 && builder == nil {
		return nil, fmt.Errorf("a compact input file needs a checkpoint builder")
	}
	if f == FormatBlocks && source != 0 {
		return nil, fmt.Errorf("input files have no %v format", f)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	w := &Writer{file: file, buffer: newSink(file, bufferSize), builder: builder, source: source, format: f}
	if f == FormatCompact && source == 1 {
		w.input = newCompactInput(builder)
	}
	return w, nil
}

// appendFrame appends a complete compact input frame.
func (w *Writer) appendFrame(frame []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return os.ErrClosed
	}
	n, err := w.buffer.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	w.err = err
	return err
}

// Format reports the record format the writer writes.
func (w *Writer) Format() Format { return w.format }

// sink writes buffers at their file offsets on a pool of goroutines and hashes
// each fixed checksum chunk on its own goroutine, so neither the file write nor
// hashing is a serial bottleneck. Bytes within a chunk are hashed in file order.
// A buffer is released once it is written and every chunk it overlaps has
// hashed it. No whole-file digest is computed: validChecksumShape accepts its
// absence for multi-chunk files (older binaries then use ordered restore).
type sink struct {
	file     io.WriterAt
	size     int
	current  []byte
	free     chan []byte
	offset   int64
	region   *sinkRegion
	chunks   []*[sha256.Size]byte
	writes   chan func()
	writers  sync.WaitGroup
	pending  sync.WaitGroup
	inflight chan struct{}
	mu       sync.Mutex
	err      error
	flushed  bool
}

type sinkBlock struct {
	s       *sink
	buf     []byte
	refs    atomic.Int32
	release func([]byte)
}

func (b *sinkBlock) done() {
	if b.refs.Add(-1) == 0 {
		if b.release != nil {
			b.release(b.buf)
		}
		<-b.s.inflight
	}
}

type sinkPiece struct {
	blk  *sinkBlock
	data []byte
}

// sinkRegion hashes one checksum chunk; pieces arrive in file order.
type sinkRegion struct {
	pieces chan sinkPiece
	filled int
}

const sinkInflight = 256

// sinkWriters is how many goroutines write buffers at their offsets. XFS
// serializes buffered writes to one file: on a production NVMe store, 1, 2, 4
// and 8 writers all reached ~1.9GB/s (TestSinkHostThroughput), while 8
// writers spent ~31 CPU-seconds of a production dump spinning on the lock.
var sinkWriters = 2

func newSink(file io.WriterAt, size int) *sink {
	s := &sink{file: file, size: size, free: make(chan []byte, sinkInflight), writes: make(chan func(), sinkInflight), inflight: make(chan struct{}, sinkInflight)}
	s.current = make([]byte, 0, size)
	for range sinkWriters {
		s.writers.Go(func() {
			for job := range s.writes {
				job()
			}
		})
	}
	return s
}

func (s *sink) fail(err error) {
	if err != nil {
		s.mu.Lock()
		s.err = errors.Join(s.err, err)
		s.mu.Unlock()
	}
}
func (s *sink) failed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *sink) recycle(buf []byte) {
	select {
	case s.free <- buf[:0]:
	default:
	}
}

func (s *sink) startRegion() {
	r := &sinkRegion{pieces: make(chan sinkPiece, 64)}
	digest := new([sha256.Size]byte)
	s.chunks = append(s.chunks, digest)
	s.region = r
	s.pending.Go(func() {
		h := sha256.New()
		for p := range r.pieces {
			_, _ = h.Write(p.data)
			p.blk.done()
		}
		h.Sum(digest[:0])
	})
}

// dispatch hands buf to the writers and the chunk hashers.
func (s *sink) dispatch(buf []byte, release func([]byte)) {
	s.inflight <- struct{}{}
	blk := &sinkBlock{s: s, buf: buf, release: release}
	blk.refs.Store(1) // the write
	off := s.offset
	s.offset += int64(len(buf))
	for data := buf; len(data) > 0; {
		if s.region == nil {
			s.startRegion()
		}
		take := min(len(data), checksumChunkSize-s.region.filled)
		blk.refs.Add(1)
		s.region.pieces <- sinkPiece{blk, data[:take]}
		s.region.filled += take
		data = data[take:]
		if s.region.filled == checksumChunkSize {
			close(s.region.pieces)
			s.region = nil
		}
	}
	s.pending.Add(1)
	s.writes <- func() {
		defer s.pending.Done()
		if s.failed() == nil {
			n, err := s.file.WriteAt(buf, off)
			if err == nil && n != len(buf) {
				err = io.ErrShortWrite
			}
			s.fail(err)
		}
		blk.done()
	}
}

// Write copies data into pooled buffers. It reports an earlier failure; bytes
// accepted here may still fail later, which Flush reports.
func (s *sink) Write(data []byte) (int, error) {
	if err := s.failed(); err != nil {
		return 0, err
	}
	n := len(data)
	for len(data) > 0 {
		take := min(len(data), s.size-len(s.current))
		s.current = append(s.current, data[:take]...)
		data = data[take:]
		if len(s.current) == s.size {
			s.dispatch(s.current, s.recycle)
			s.current = s.next()
		}
	}
	return n, nil
}

func (s *sink) next() []byte {
	select {
	case buf := <-s.free:
		return buf
	default:
		return make([]byte, 0, s.size)
	}
}

// Submit writes a complete buffer without copying it. The caller must not
// modify buf until release (if any) is called with it. Pending bytes from
// Write go first.
func (s *sink) Submit(buf []byte, release func([]byte)) error {
	if err := s.failed(); err != nil {
		return err
	}
	if len(buf) == 0 {
		return nil
	}
	if len(s.current) > 0 {
		s.dispatch(s.current, s.recycle)
		s.current = s.next()
	}
	s.dispatch(buf, release)
	return nil
}

// Flush writes the final buffer and waits for every write and hash. It is
// terminal.
func (s *sink) Flush() error {
	if s.flushed {
		return s.failed()
	}
	s.flushed = true
	if len(s.current) > 0 {
		s.dispatch(s.current, nil)
	}
	s.current = nil
	if s.region != nil {
		close(s.region.pieces)
		s.region = nil
	}
	s.pending.Wait()
	close(s.writes)
	s.writers.Wait()
	return s.failed()
}

// descriptor describes the flushed bytes.
func (s *sink) descriptor(name string, size int64) File {
	f := File{Name: name, Size: size}
	if len(s.chunks) == 0 {
		f.SHA256 = sha256.Sum256(nil)
		return f
	}
	f.ChunkSize = checksumChunkSize
	for _, c := range s.chunks {
		f.Chunks = append(f.Chunks, *c)
	}
	if len(f.Chunks) == 1 {
		f.SHA256 = f.Chunks[0]
	}
	return f
}

func (w *Writer) WritePoints(p *points.Points) error {
	if w.input != nil {
		if len(p.Data) == 0 {
			return nil
		}
		return w.input.write(w, p)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return os.ErrClosed
	}
	if len(p.Data) == 0 {
		return nil
	}
	if w.format == FormatBlocks {
		return fmt.Errorf("a %v cache dump is written in segments", w.format)
	}
	if w.format == FormatCompact {
		w.encoded = AppendCompactPoints(w.encoded[:0], p.Data)
		w.scratch = AppendCompactRecord(w.scratch[:0], p.Metric, w.encoded, len(p.Data))
	} else {
		w.scratch = p.AppendBinary(w.scratch[:0])
	}
	n, err := w.buffer.Write(w.scratch)
	if err == nil && n != len(w.scratch) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		return err
	}
	if w.builder != nil {
		w.err = w.builder.Add(w.source, p, n)
	}
	if w.err == nil {
		w.points += int64(len(p.Data))
	}
	return w.err
}

// Points reports how many points WritePoints accepted as records; compact
// input files count none.
func (w *Writer) Points() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.points
}

// segmentChunk bounds each encoded buffer; records never span two buffers.
const segmentChunk = 1 << 20

var chunkPool = sync.Pool{New: func() any { return make([]byte, 0, segmentChunk+4096) }}

func releaseChunk(buf []byte) {
	if cap(buf) == segmentChunk+4096 {
		chunkPool.Put(buf[:0]) //nolint:staticcheck // slices are the pooled values
	}
}

type segment struct {
	chunks  [][]byte
	entries []BatchEntry
	err     error
	done    chan struct{}
}

// Segment collects one concurrently encoded part of a WriteSegments call.
type Segment struct {
	s       *segment
	cur     []byte
	builder *Builder
	format  Format
	encoded []byte
	blocks  *blockScratch // FormatBlocks: records waiting for their blocks
	enc     *zstd.Encoder // FormatBlocks: names
}

// Format reports the record format of the segment's file.
func (g *Segment) Format() Format { return g.format }

// WritePoints encodes p as one record of the file's format.
func (g *Segment) WritePoints(p *points.Points) error {
	if len(p.Data) == 0 {
		return nil
	}
	if g.format.Compact() {
		g.encoded = AppendCompactPoints(g.encoded[:0], p.Data)
		return g.WriteCompact(p.Metric, g.encoded, len(p.Data))
	}
	start := len(g.cur)
	g.cur = p.AppendBinary(g.cur)
	return g.added(p.Metric, len(g.cur)-start, len(p.Data))
}

// WriteCompact appends a compact record of the n points encoded in data,
// which must be a compact encoding (see FormatCompact). It is not validated.
func (g *Segment) WriteCompact(metric string, data []byte, n int) error {
	if !g.format.Compact() {
		return fmt.Errorf("compact record in a %v recovery file", g.format)
	}
	if n <= 0 {
		return nil
	}
	if g.blocks != nil {
		if len(metric) == 0 || len(metric) > points.MB || len(data) < n {
			return fmt.Errorf("invalid compact recovery record")
		}
		g.blocks.add(metric, data, n)
		return nil
	}
	start := len(g.cur)
	g.cur = AppendCompactRecord(g.cur, metric, data, n)
	return g.added(metric, len(g.cur)-start, n)
}

// WriteCompactBorrowed writes a compact record whose point bytes remain
// immutable until WriteSegments returns. Block dumps retain them while
// sorting metric names, avoiding a second copy of the points. Other formats
// write them immediately as WriteCompact does. Transient or reused scratch
// must use WriteCompact instead.
func (g *Segment) WriteCompactBorrowed(metric string, data []byte, n int) error {
	if g.blocks == nil {
		return g.WriteCompact(metric, data, n)
	}
	if n <= 0 {
		return nil
	}
	if len(metric) == 0 || len(metric) > points.MB || len(data) < n {
		return fmt.Errorf("invalid compact recovery record")
	}
	g.blocks.borrow(metric, data, n)
	return nil
}

// WriteRaw appends one complete legacy record for metric holding count points,
// already encoded (e.g. mapped from a previous dump). It is not validated.
func (g *Segment) WriteRaw(metric string, raw []byte, count int) error {
	if g.format != FormatLegacy {
		return fmt.Errorf("legacy record in a %v recovery file", g.format)
	}
	if len(raw) == 0 || count <= 0 {
		return fmt.Errorf("invalid raw recovery record")
	}
	g.cur = append(g.cur, raw...)
	return g.added(metric, len(raw), count)
}

func (g *Segment) added(metric string, size, count int) error {
	var id uint32
	if g.builder != nil {
		var err error
		if id, err = g.builder.ID(metric); err != nil {
			return err
		}
	}
	g.s.entries = append(g.s.entries, BatchEntry{ID: id, Size: size, Count: count})
	if len(g.cur) >= segmentChunk {
		g.s.chunks = append(g.s.chunks, g.cur)
		g.cur = chunkPool.Get().([]byte)
	}
	return nil
}

// finishBlocks writes the segment's records as blocks: each block's header
// and names, skipped by the index, then its records.
func (g *Segment) finishBlocks() error {
	return g.blocks.appendBlocks(g.enc, func(head []byte, records []blockRef) error {
		first, offset := len(g.s.chunks), len(g.cur)
		g.s.entries = slices.Grow(g.s.entries, len(records)+1)
		g.s.entries = append(g.s.entries, BatchEntry{Size: len(head), gap: true})
		g.cur = append(g.cur, head...)
		for _, r := range records {
			g.cur = r.appendTo(g.cur)
			if err := g.added(r.name, r.size(), r.n); err != nil {
				return err
			}
		}
		// Records have reached their immutable output buffers. A block
		// may span chunks, but its header is wholly in the first one.
		var header []byte
		var crc uint32
		if first == len(g.s.chunks) {
			header = g.cur[offset : offset+blockHeaderSize]
			crc = crc32.Checksum(g.cur[offset+blockHeaderSize:], castagnoli)
		} else {
			chunk := g.s.chunks[first]
			header = chunk[offset : offset+blockHeaderSize]
			crc = crc32.Checksum(chunk[offset+blockHeaderSize:], castagnoli)
			for _, chunk := range g.s.chunks[first+1:] {
				crc = crc32.Update(crc, castagnoli, chunk)
			}
			crc = crc32.Update(crc, castagnoli, g.cur)
		}
		binary.LittleEndian.PutUint32(header[24:], crc)
		binary.LittleEndian.PutUint32(header[28:], crc32.Checksum(header[:28], castagnoli))
		return nil
	})
}

// WriteSegments encodes n independent record sequences on up to parallel
// goroutines and appends them to the file in segment order, as if written
// serially. fill(i, seg) must write segment i's records in their required
// order; segments must not share a metric, so per-metric chains keep their
// order. Metric ids are resolved by the workers; the ordered append does only
// array work. Workers stay within a bounded window ahead of the append, so
// encoded buffers are recycled instead of the whole file being held in memory.
func (w *Writer) WriteSegments(n, parallel int, fill func(i int, seg *Segment) error) error {
	w.mu.Lock()
	builder, format := w.builder, w.format
	w.mu.Unlock()
	parallel = max(1, min(parallel, n))
	var enc *zstd.Encoder
	if format == FormatBlocks {
		enc = newBlockEncoder(parallel)
		defer enc.Close()
	}
	segs := make([]segment, n)
	for i := range segs {
		segs[i].done = make(chan struct{})
	}
	window := make(chan struct{}, 2*parallel)
	var next atomic.Int64
	stop := make(chan struct{})
	var workers sync.WaitGroup
	for range parallel {
		workers.Go(func() {
			var blocks *blockScratch
			if enc != nil {
				blocks = blockScratchPool.Get().(*blockScratch)
				defer blockScratchPool.Put(blocks)
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				select {
				case window <- struct{}{}:
				case <-stop:
					return
				}
				i := int(next.Add(1) - 1)
				if i >= n {
					<-window
					return
				}
				s := &segs[i]
				g := &Segment{s: s, cur: chunkPool.Get().([]byte), builder: builder, format: format, enc: enc, blocks: blocks}
				s.err = fill(i, g)
				if g.blocks != nil {
					if s.err == nil {
						s.err = g.finishBlocks()
					}
					g.blocks.reset()
				}
				if len(g.cur) > 0 {
					s.chunks = append(s.chunks, g.cur)
				} else {
					releaseChunk(g.cur)
				}
				close(s.done)
			}
		})
	}
	var err error
	for i := range segs {
		<-segs[i].done
		if err == nil {
			err = segs[i].err
		}
		if err == nil {
			err = w.appendSegment(&segs[i])
		} else {
			for _, c := range segs[i].chunks {
				releaseChunk(c)
			}
		}
		segs[i] = segment{done: segs[i].done}
		<-window
		if err != nil {
			// Workers finish their current segment and exit; their chunks
			// are simply dropped.
			close(stop)
			workers.Wait()
			return err
		}
	}
	workers.Wait()
	return nil
}

func (w *Writer) appendSegment(s *segment) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return os.ErrClosed
	}
	for _, chunk := range s.chunks {
		if w.err = w.buffer.Submit(chunk, releaseChunk); w.err != nil {
			return w.err
		}
	}
	if w.builder != nil && len(s.entries) > 0 {
		w.err = w.builder.AddBatch(w.source, s.entries)
	}
	return w.err
}

// Close flushes, synchronizes and closes the file.
func (w *Writer) Close() (File, error) {
	f, err := w.Flush()
	return f, errors.Join(err, w.Sync())
}

// Flush writes every accepted record and describes the file, without waiting
// for it to reach the disk; Sync completes durability. No records are accepted
// afterwards.
func (w *Writer) Flush() (File, error) {
	var inputErr error
	w.mu.Lock()
	input := w.input != nil && !w.inputDone
	w.inputDone = true
	w.mu.Unlock()
	if input {
		inputErr = w.input.close(w)
	}
	w.mu.Lock()
	w.err = errors.Join(w.err, inputErr)
	defer w.mu.Unlock()
	if w.closed {
		return w.descriptor, w.err
	}
	w.closed = true
	// Cache diversion can retain this closed writer until process exit. The
	// caller owns the completed builder; do not retain its construction tables.
	w.builder = nil
	w.err = errors.Join(w.err, w.buffer.Flush())
	if w.err == nil {
		info, err := w.file.Stat()
		w.err = err
		if err == nil {
			w.descriptor = w.buffer.descriptor(filepath.Base(w.file.Name()), info.Size())
		}
	}
	return w.descriptor, w.err
}

// Sync makes the flushed file durable and closes it. It is idempotent and may
// run concurrently with Close.
func (w *Writer) Sync() error {
	w.syncOnce.Do(func() {
		if _, err := w.Flush(); err != nil {
			w.syncErr = errors.Join(err, w.file.Close())
			return
		}
		w.syncErr = errors.Join(w.file.Sync(), w.file.Close())
	})
	return w.syncErr
}

// WriteIndex creates the hidden index sidecar. It is not synchronized: the
// index only accelerates recovery, and Publish records its chunk checksums. If
// a crash loses unwritten index pages, opening the bundle fails its checksum
// and startup uses the ordinary ordered restore of the two source files, which
// callers synchronize before publishing. A normal restart reads the index from
// the page cache.
func WriteIndex(dir string, builder *Builder) (File, error) {
	file, err := os.CreateTemp(dir, ".pending-index-*.bin")
	if err != nil {
		return File{}, err
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(file.Name())
		}
	}()
	buffer := newSink(file, 1<<20)
	if err = builder.Write(buffer); err != nil {
		return File{}, err
	}
	if err = buffer.Flush(); err != nil {
		return File{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return File{}, err
	}
	result := buffer.descriptor(filepath.Base(file.Name()), info.Size())
	if err = file.Close(); err != nil {
		return File{}, err
	}
	keep = true
	return result, nil
}
