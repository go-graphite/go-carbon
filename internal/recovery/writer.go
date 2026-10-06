package recovery

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"hash"
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
	buffer     *bufio.Writer
	hash       hash.Hash
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
	digest := sha256.New()
	return &Writer{file: file, buffer: bufio.NewWriterSize(io.MultiWriter(file, digest), bufferSize), hash: digest, builder: builder, source: source}, nil
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
	w.err = errors.Join(w.err, w.buffer.Flush(), w.file.Sync())
	if w.err == nil {
		info, err := w.file.Stat()
		w.err = err
		if err == nil {
			w.descriptor = File{Name: filepath.Base(w.file.Name()), Size: info.Size()}
			copy(w.descriptor.SHA256[:], w.hash.Sum(nil))
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
	digest := sha256.New()
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
	result := File{Name: filepath.Base(file.Name()), Size: info.Size()}
	copy(result.SHA256[:], digest.Sum(nil))
	if err = file.Close(); err != nil {
		return File{}, err
	}
	keep = true
	return result, nil
}
