package chunkstore

import (
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// compactionPacer shares a small token bucket across all compaction outputs.
// A reload wakes sleepers so disabling or raising the rate takes effect even
// when the previous rate would have kept a write waiting for a long time.
type compactionPacer struct {
	mu      sync.Mutex
	rate    int64
	tokens  float64
	last    time.Time
	changed chan struct{}
	bytes   atomic.Uint64
	wait    atomic.Int64
}

func newCompactionPacer(rate int64) *compactionPacer {
	p := &compactionPacer{rate: rate, last: time.Now(), changed: make(chan struct{})}
	p.tokens = p.burst()
	return p
}

// Keep at most 10 ms of credit, capped at 64 KiB. Splitting large writes keeps
// a table buffer from consuming seconds of budget in one filesystem write.
func (p *compactionPacer) burst() float64 {
	return float64(min(64<<10, max(1, p.rate/100)))
}

func (p *compactionPacer) refill(now time.Time) {
	p.tokens = min(p.burst(), p.tokens+now.Sub(p.last).Seconds()*float64(p.rate))
	p.last = now
}

func (p *compactionPacer) setRate(rate int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refill(time.Now())
	p.rate = rate
	p.tokens = min(p.tokens, p.burst())
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *compactionPacer) rateBytes() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rate
}

func (p *compactionPacer) take(size int) (n int, paced bool) {
	for {
		p.mu.Lock()
		if p.rate == 0 {
			p.mu.Unlock()
			return size, false
		}
		p.refill(time.Now())
		n = min(size, int(p.burst()))
		if p.tokens >= float64(n) {
			p.tokens -= float64(n)
			p.mu.Unlock()
			return n, true
		}
		delay := time.Duration((float64(n) - p.tokens) / float64(p.rate) * float64(time.Second))
		changed := p.changed
		p.mu.Unlock()
		start := time.Now()
		timer := time.NewTimer(max(time.Nanosecond, delay))
		select {
		case <-timer.C:
		case <-changed:
			timer.Stop()
		}
		p.wait.Add(int64(time.Since(start)))
	}
}

// Pebble opens a table before TableCreated, but emits the event before any
// writes. Keep the open handle until Close so the event can mark it. Flush
// tables take tokens from a separate flush bucket, which is unpaced unless
// flush pacing is enabled; staged ingest tables, WALs and manifests are never
// paced.
type compactionPacingFS struct {
	vfs.FS
	pacer *compactionPacer
	// flush paces memtable flush output; see Options.FlushRate.
	flush  *compactionPacer
	output atomic.Uint64
	mu     sync.Mutex
	files  map[string]*compactionPacingFile
}

func newCompactionPacingFS(fs vfs.FS, rate int64) *compactionPacingFS {
	return &compactionPacingFS{FS: fs, pacer: newCompactionPacer(max(0, rate)), flush: newCompactionPacer(0), files: make(map[string]*compactionPacingFile)}
}

func (fs *compactionPacingFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil || !strings.HasSuffix(name, ".sst") {
		return f, err
	}
	fp := &compactionPacingFile{File: f, fs: fs, name: filepath.Clean(name)}
	fs.mu.Lock()
	fs.files[fp.name] = fp
	fs.mu.Unlock()
	return fp, nil
}

func (fs *compactionPacingFS) tableCreated(info pebble.TableCreateInfo) {
	if info.Reason != "compacting" && info.Reason != "flushing" {
		return
	}
	fs.mu.Lock()
	if f := fs.files[filepath.Clean(info.Path)]; f != nil {
		if info.Reason == "compacting" {
			f.compacting.Store(true)
		} else {
			f.flushing.Store(true)
		}
	}
	fs.mu.Unlock()
}

type compactionPacingFile struct {
	vfs.File
	fs                   *compactionPacingFS
	name                 string
	compacting, flushing atomic.Bool
}

func (f *compactionPacingFile) Write(b []byte) (int, error) {
	pacer := f.fs.pacer
	compacting := f.compacting.Load()
	if !compacting {
		if !f.flushing.Load() {
			return f.File.Write(b)
		}
		pacer = f.fs.flush
	}
	written := 0
	for len(b) > 0 {
		n, paced := pacer.take(len(b))
		count, err := f.File.Write(b[:n])
		written += count
		// The compaction controller measures compaction output only.
		if compacting {
			f.fs.output.Add(uint64(count))
		}
		if paced {
			pacer.bytes.Add(uint64(count))
		}
		if err != nil {
			return written, err
		}
		if count != n {
			return written, io.ErrShortWrite
		}
		b = b[n:]
	}
	return written, nil
}

func (f *compactionPacingFile) Close() error {
	f.fs.mu.Lock()
	if f.fs.files[f.name] == f {
		delete(f.fs.files, f.name)
	}
	f.fs.mu.Unlock()
	return f.File.Close()
}
