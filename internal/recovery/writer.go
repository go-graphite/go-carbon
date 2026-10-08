package recovery

import (
	"bufio"
	"errors"
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
	return &Writer{file: file, buffer: newPipeline(file, digest, bufferSize), hash: digest, builder: builder, source: source}, nil
}

// pipeline keeps file writes and checksums off the encoding goroutine. Full
// buffers pass in order through a write stage and then a hash stage, so the
// digest always describes exactly the bytes written, in file order.
type pipeline struct {
	current       []byte
	size          int
	full, written chan []byte
	free          chan []byte
	done          chan struct{}
	mu            sync.Mutex
	err           error
}

const pipelineBuffers = 4

func newPipeline(file io.Writer, digest io.Writer, size int) *pipeline {
	p := &pipeline{size: size, full: make(chan []byte, pipelineBuffers), written: make(chan []byte, pipelineBuffers), free: make(chan []byte, pipelineBuffers), done: make(chan struct{})}
	p.current = make([]byte, 0, size)
	for i := 1; i < pipelineBuffers; i++ {
		p.free <- make([]byte, 0, size)
	}
	go func() {
		defer close(p.written)
		for buf := range p.full {
			if p.failed() == nil {
				n, err := file.Write(buf)
				if err == nil && n != len(buf) {
					err = io.ErrShortWrite
				}
				p.fail(err)
			}
			p.written <- buf
		}
	}()
	go func() {
		defer close(p.done)
		for buf := range p.written {
			_, _ = digest.Write(buf)
			p.free <- buf[:0]
		}
	}()
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
			p.full <- p.current
			p.current = <-p.free
		}
	}
	return n, nil
}

// Flush hands over the final buffer and waits for both stages. It is terminal.
func (p *pipeline) Flush() error {
	if p.full == nil {
		return p.failed()
	}
	if len(p.current) > 0 {
		p.full <- p.current
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
	buffer := bufio.NewWriterSize(io.MultiWriter(file, digest), 1<<20)
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
