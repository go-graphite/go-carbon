package persister

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// Logical parity fixtures keep independent handles, sidecars and rename/remove
// semantics without device I/O. Each fixture is used by one test goroutine.
type storageMemoryFS map[string]*[]byte

var storageMemoryCases = make(chan struct{}, 8)

func parallelStorageCase(t *testing.T) {
	t.Helper()
	t.Parallel()
	storageMemoryCases <- struct{}{}
	t.Cleanup(func() { <-storageMemoryCases })
}

func newStorageMemoryBackend(t *testing.T, kind string, now *atomic.Int64) *storageBackend {
	s := newStorageBackend(t, kind, now)
	if s.db == nil {
		s.files = make(storageMemoryFS)
	}
	return s
}

func (fs storageMemoryFS) OpenFile(name string, flags int, _ os.FileMode) (whisper.StorageFile, error) {
	data, exists := fs[name]
	if !exists {
		if flags&os.O_CREATE == 0 {
			return nil, os.ErrNotExist
		}
		data = new([]byte)
		fs[name] = data
	} else if flags&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL {
		return nil, os.ErrExist
	}
	if flags&os.O_TRUNC != 0 {
		*data = nil
	}
	return &storageMemoryFile{name: name, data: data}, nil
}

func (fs storageMemoryFS) Stat(name string) (os.FileInfo, error) {
	data, ok := fs[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return storageMemoryInfo{name: filepath.Base(name), size: int64(len(*data))}, nil
}

func (fs storageMemoryFS) Remove(name string) error {
	if _, ok := fs[name]; !ok {
		return os.ErrNotExist
	}
	delete(fs, name)
	return nil
}

func (fs storageMemoryFS) Rename(old, name string) error {
	data, ok := fs[old]
	if !ok {
		return os.ErrNotExist
	}
	if old == name {
		return nil
	}
	fs[name] = data
	delete(fs, old)
	return nil
}

type storageMemoryFile struct {
	name   string
	data   *[]byte
	offset int64
}

func (f *storageMemoryFile) Name() string { return f.name }
func (f *storageMemoryFile) Fd() uintptr  { return ^uintptr(0) }
func (f *storageMemoryFile) Close() error { return nil }

func (f *storageMemoryFile) ReadAt(p []byte, off int64) (int, error) {
	return bytes.NewReader(*f.data).ReadAt(p, off)
}

func (f *storageMemoryFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *storageMemoryFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, os.ErrInvalid
	}
	if end := off + int64(len(p)); end > int64(len(*f.data)) {
		*f.data = append(*f.data, make([]byte, end-int64(len(*f.data)))...)
	}
	return copy((*f.data)[off:], p), nil
}

func (f *storageMemoryFile) Write(p []byte) (int, error) {
	n, err := f.WriteAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *storageMemoryFile) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		off += f.offset
	case io.SeekEnd:
		off += int64(len(*f.data))
	default:
		return 0, os.ErrInvalid
	}
	if off < 0 {
		return 0, os.ErrInvalid
	}
	f.offset = off
	return off, nil
}

func (f *storageMemoryFile) Truncate(size int64) error {
	if size < 0 {
		return os.ErrInvalid
	}
	if size > int64(len(*f.data)) {
		*f.data = append(*f.data, make([]byte, size-int64(len(*f.data)))...)
	} else {
		*f.data = (*f.data)[:size]
	}
	return nil
}

type storageMemoryInfo struct {
	name string
	size int64
}

func (i storageMemoryInfo) Name() string     { return i.name }
func (i storageMemoryInfo) Size() int64      { return i.size }
func (storageMemoryInfo) Mode() os.FileMode  { return 0666 }
func (storageMemoryInfo) ModTime() time.Time { return time.Time{} }
func (storageMemoryInfo) IsDir() bool        { return false }
func (storageMemoryInfo) Sys() interface{}   { return nil }
