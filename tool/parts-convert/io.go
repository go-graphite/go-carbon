//go:build linux || darwin

package main

import (
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// pacedFS charges source reads, external SST writes, WALs, verification and
// compaction to one budget. Install it before opening any Pebble handles.
// Filesystem metadata and small JSON journals are outside this data-byte cap.
// Cached reads count too; this is deliberately more conservative than device IO.
type pacedFS struct {
	vfs.FS
	pace *ioPacer
}

type ioPacer struct {
	mu             sync.Mutex
	bytesPerSecond uint64
	next           time.Time
	last           time.Time
	bytes          uint64
	now            func() time.Time
	sleep          func(time.Duration)
}

func newPacedFS(fs vfs.FS, bytesPerSecond uint64) *pacedFS {
	return &pacedFS{FS: fs, pace: &ioPacer{bytesPerSecond: bytesPerSecond, now: time.Now, sleep: time.Sleep}}
}

func (p *ioPacer) charge(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	// Tiny reads must retain time already paid by an oversleep. Resetting
	// next to now on every call turns each read into a full timer tick.
	// Bound that credit and discard it after idle time.
	const timerCredit = time.Millisecond
	if p.last.IsZero() || now.Sub(p.last) > timerCredit {
		p.next = now
	}
	if floor := now.Add(-timerCredit); p.next.Before(floor) {
		p.next = floor
	}
	duration := time.Duration((uint64(n)*uint64(time.Second) + p.bytesPerSecond - 1) / p.bytesPerSecond)
	p.next = p.next.Add(duration)
	if delay := p.next.Sub(now); delay > 0 {
		p.sleep(delay)
	}
	p.last = p.now()
	p.bytes += uint64(n)
}

func (fs *pacedFS) wrap(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &pacedFile{File: f, pace: fs.pace}, nil
}
func (fs *pacedFS) Create(name string) (vfs.File, error) { return fs.wrap(fs.FS.Create(name)) }
func (fs *pacedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.Open(name, opts...))
}
func (fs *pacedFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.OpenReadWrite(name, opts...))
}
func (fs *pacedFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	return fs.wrap(fs.FS.ReuseForWrite(oldname, newname))
}

type pacedFile struct {
	vfs.File
	pace *ioPacer
}

const ioQuantum = 64 << 10

func (f *pacedFile) Read(p []byte) (int, error) {
	if len(p) > ioQuantum {
		p = p[:ioQuantum]
	}
	if len(p) == 0 {
		return 0, nil
	}
	f.pace.charge(len(p))
	return f.File.Read(p)
}
func (f *pacedFile) ReadAt(p []byte, offset int64) (int, error) {
	return f.transfer(p, func(b []byte, n int) (int, error) { return f.File.ReadAt(b, offset+int64(n)) })
}
func (f *pacedFile) Write(p []byte) (int, error) {
	return f.transfer(p, func(b []byte, _ int) (int, error) { return f.File.Write(b) })
}
func (f *pacedFile) WriteAt(p []byte, offset int64) (int, error) {
	return f.transfer(p, func(b []byte, n int) (int, error) { return f.File.WriteAt(b, offset+int64(n)) })
}
func (f *pacedFile) transfer(p []byte, operation func([]byte, int) (int, error)) (int, error) {
	total := 0
	for len(p) > 0 {
		n := min(len(p), ioQuantum)
		f.pace.charge(n)
		got, err := operation(p[:n], total)
		total += got
		if err != nil {
			return total, err
		}
		if got != n {
			return total, io.ErrShortWrite
		}
		p = p[n:]
	}
	return total, nil
}

// Do not let readahead or eager allocation escape the data-byte scheduler.
func (*pacedFile) Prefetch(int64, int64) error    { return nil }
func (*pacedFile) Preallocate(int64, int64) error { return nil }
func (*pacedFile) Fd() uintptr                    { return vfs.InvalidFd }
