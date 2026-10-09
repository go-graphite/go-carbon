package recovery

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-graphite/go-carbon/points"
)

// Writer writes an ordinary legacy binary source and records its exact extents.
// Both cache dump and concurrent input diversion can share one Builder.
type Writer struct {
	mu         sync.Mutex
	file       *os.File
	buffer     *pipeline
	hash       *fileDigester
	builder    *Builder
	source     int
	scratch    []byte
	closed     bool
	err        error
	descriptor File
}

func NewWriter(path string, source, bufferSize int, builder *Builder) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	digest := newFileDigester()
	return &Writer{file: file, buffer: newPipeline(file, bufferSize, digest.wholeWriter(), digest.chunkWriter()), hash: digest, builder: builder, source: source}, nil
}

// pipeline keeps file writes and checksums off the encoding goroutine. Full
// buffers pass in order through the file write and then each hash stage, every
// stage on its own goroutine, so the digests always describe exactly the bytes
// written, in file order, while hashing overlaps the write.
// block is a buffer moving through the stages; only pooled ones are recycled.
type block struct {
	buf    []byte
	pooled bool
}

type pipeline struct {
	current []byte
	size    int
	full    chan block
	free    chan []byte
	done    chan struct{}
	mu      sync.Mutex
	err     error
}

const pipelineBuffers = 4

func newPipeline(file io.Writer, size int, hashes ...io.Writer) *pipeline {
	p := &pipeline{size: size, full: make(chan block, pipelineBuffers), free: make(chan []byte, pipelineBuffers), done: make(chan struct{})}
	p.current = make([]byte, 0, size)
	for i := 1; i < pipelineBuffers; i++ {
		p.free <- make([]byte, 0, size)
	}
	in := p.full
	for i, stage := range append([]io.Writer{file}, hashes...) {
		out := make(chan block, pipelineBuffers)
		last := i == len(hashes)
		go func(in <-chan block, out chan<- block) {
			if last {
				defer close(p.done)
			} else {
				defer close(out)
			}
			for b := range in {
				if i == 0 {
					if p.failed() == nil {
						n, err := stage.Write(b.buf)
						if err == nil && n != len(b.buf) {
							err = io.ErrShortWrite
						}
						p.fail(err)
					}
				} else {
					_, _ = stage.Write(b.buf)
				}
				if !last {
					out <- b
				} else if b.pooled {
					p.free <- b.buf[:0]
				}
			}
		}(in, out)
		in = out
	}
	return p
}

func (p *pipeline) fail(err error) {
	if err != nil {
		p.mu.Lock()
		p.err = errors.Join(p.err, err)
		p.mu.Unlock()
	}
}
func (p *pipeline) failed() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Write reports an earlier stage failure; bytes accepted here may still fail
// later, which Flush reports.
func (p *pipeline) Write(data []byte) (int, error) {
	if err := p.failed(); err != nil {
		return 0, err
	}
	n := len(data)
	for len(data) > 0 {
		take := min(len(data), p.size-len(p.current))
		p.current = append(p.current, data[:take]...)
		data = data[take:]
		if len(p.current) == p.size {
			p.full <- block{p.current, true}
			p.current = <-p.free
		}
	}
	return n, nil
}

// Submit passes a complete buffer through every stage without copying it. The
// caller must not modify buf afterwards. Pending bytes from Write go first.
func (p *pipeline) Submit(buf []byte) error {
	if err := p.failed(); err != nil {
		return err
	}
	if len(buf) == 0 {
		return nil
	}
	if len(p.current) > 0 {
		p.full <- block{p.current, true}
		p.current = <-p.free
	}
	p.full <- block{buf, false}
	return nil
}

// Flush hands over the final buffer and waits for both stages. It is terminal.
func (p *pipeline) Flush() error {
	if p.full == nil {
		return p.failed()
	}
	if len(p.current) > 0 {
		p.full <- block{p.current, true}
	}
	close(p.full)
	<-p.done
	p.full, p.current = nil, nil
	return p.failed()
}

func (w *Writer) WritePoints(p *points.Points) error {
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
	w.scratch = p.AppendBinary(w.scratch[:0])
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
	return w.err
}

// segmentChunk bounds each encoded buffer; records never span two buffers.
const segmentChunk = 4 << 20

type segment struct {
	chunks  [][]byte
	entries []BatchEntry
	err     error
}

// Segment collects one concurrently encoded part of a WriteSegments call.
type Segment struct {
	s       *segment
	cur     []byte
	builder *Builder
}

// WritePoints encodes p as one legacy record.
func (g *Segment) WritePoints(p *points.Points) error {
	if len(p.Data) == 0 {
		return nil
	}
	start := len(g.cur)
	g.cur = p.AppendBinary(g.cur)
	return g.added(p.Metric, len(g.cur)-start, len(p.Data))
}

// WriteRaw appends one complete legacy record for metric holding count points,
// already encoded (e.g. mapped from a previous dump). It is not validated.
func (g *Segment) WriteRaw(metric string, raw []byte, count int) error {
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
		g.cur = make([]byte, 0, segmentChunk+4096)
	}
	return nil
}

// WriteSegments encodes n independent record sequences concurrently and appends
// them to the file in segment order, as if written serially. fill(i, seg) must
// write segment i's records in their required order; segments must not share a
// metric, so per-metric chains keep their order. Metric ids are resolved in the
// segment goroutines; the ordered append does only array work.
func (w *Writer) WriteSegments(n int, fill func(i int, seg *Segment) error) error {
	w.mu.Lock()
	builder := w.builder
	w.mu.Unlock()
	segs := make([]segment, n)
	done := make([]chan struct{}, n)
	for i := range segs {
		done[i] = make(chan struct{})
		go func() {
			defer close(done[i])
			g := &Segment{s: &segs[i], cur: make([]byte, 0, segmentChunk+4096), builder: builder}
			g.s.err = fill(i, g)
			if len(g.cur) > 0 {
				g.s.chunks = append(g.s.chunks, g.cur)
			}
		}()
	}
	var err error
	for i := range segs {
		<-done[i]
		if err == nil {
			err = segs[i].err
		}
		if err == nil {
			err = w.appendSegment(&segs[i])
		}
		segs[i] = segment{}
	}
	return err
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
		if w.err = w.buffer.Submit(chunk); w.err != nil {
			return w.err
		}
	}
	if w.builder != nil && len(s.entries) > 0 {
		w.err = w.builder.AddBatch(w.source, s.entries)
	}
	return w.err
}

func (w *Writer) Close() (File, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.descriptor, w.err
	}
	w.closed = true
	// Cache diversion can retain this closed writer until process exit. The
	// caller owns the completed builder; do not retain its construction tables.
	w.builder = nil
	w.err = errors.Join(w.err, w.buffer.Flush(), w.file.Sync())
	if w.err == nil {
		info, err := w.file.Stat()
		w.err = err
		if err == nil {
			w.descriptor = w.hash.descriptor(filepath.Base(w.file.Name()), info.Size())
		}
	}
	w.err = errors.Join(w.err, w.file.Close())
	return w.descriptor, w.err
}

// WriteIndex creates a synchronized hidden sidecar. Until Publish succeeds, the
// two ordinary source files remain independently recoverable by older binaries.
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
	digest := newFileDigester()
	buffer := newPipeline(file, 1<<20, digest.wholeWriter(), digest.chunkWriter())
	if err = builder.Write(buffer); err != nil {
		return File{}, err
	}
	if err = buffer.Flush(); err != nil {
		return File{}, err
	}
	if err = file.Sync(); err != nil {
		return File{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return File{}, err
	}
	result := digest.descriptor(filepath.Base(file.Name()), info.Size())
	if err = file.Close(); err != nil {
		return File{}, err
	}
	keep = true
	return result, nil
}
