package v2index

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// A base file is the base run's entries, then its block offsets (u64 each),
// then a footer: entry bytes, blocks, names (u64 each) and fileMagic. It is
// written to a temporary name and renamed, so a kill at any point leaves the
// preceding base file whole. There is no fsync: the files are a projection
// that SetDir discards at startup, so they need no power-loss durability.
const (
	fileMagic  = "v2idxfc1"
	footerSize = 32
)

// fileHook lets tests fail or kill a base write after size entry bytes.
var fileHook func(size int) error

// SetDir keeps the base level in files under dir (page cache) instead of the
// Go heap. Call it before the first publication. Files of earlier processes
// are removed: the index is a projection rebuilt at startup.
func (c *Catalog) SetDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "base-") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	c.dir = dir
	return nil
}

func (c *Catalog) nextPath() string {
	if c.dir == "" {
		return ""
	}
	c.seq++
	return filepath.Join(c.dir, fmt.Sprintf("base-%06d.fc", c.seq))
}

// writeBase writes a whole new base; a file failure keeps it on the heap.
func (c *Catalog) writeBase(ctx context.Context, fill func(*levelWriter) error) (*run, error) {
	r, err := writeRun(ctx, c.nextPath(), fill)
	if err != nil {
		c.err.Store(&err)
	}
	if r == nil {
		return nil, ctx.Err()
	}
	return r, nil
}

// writeRun writes a run to path, or to the heap when path is empty or the
// file write fails; the failure is returned with the heap run. A nil run
// means fill was canceled.
func writeRun(ctx context.Context, path string, fill func(*levelWriter) error) (*run, error) {
	var ferr error
	if path != "" {
		r, err := writeFile(path, fill)
		if err == nil || ctx.Err() != nil {
			return r, nil
		}
		ferr = err
	}
	var w levelWriter
	if err := fill(&w); err != nil {
		return nil, nil
	}
	return w.finish(), ferr
}

func (w *levelWriter) flush() {
	if w.err == nil && fileHook != nil {
		w.err = fileHook(w.size)
	}
	if w.err == nil {
		_, w.err = w.out.Write(w.r.data)
	}
	w.r.data = w.r.data[:0]
}

func writeFile(path string, fill func(*levelWriter) error) (*run, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := levelWriter{out: f}
	w.r.data = make([]byte, 0, 128<<10)
	err = fill(&w)
	if err == nil {
		w.flush()
		err = w.err
	}
	if err == nil {
		tail := make([]byte, 0, 8*len(w.r.blocks)+footerSize)
		for _, b := range w.r.blocks {
			tail = binary.LittleEndian.AppendUint64(tail, uint64(b))
		}
		tail = binary.LittleEndian.AppendUint64(tail, uint64(w.size))
		tail = binary.LittleEndian.AppendUint64(tail, uint64(len(w.r.blocks)))
		tail = binary.LittleEndian.AppendUint64(tail, uint64(w.r.n))
		tail = append(tail, fileMagic...)
		_, err = f.Write(tail)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
		return nil, err
	}
	return openRun(path)
}

// openRun maps a base file read-only. Only the block offsets are copied to
// the heap. The mapping is released when no snapshot references the run.
func openRun(path string) (*run, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size < footerSize {
		return nil, fmt.Errorf("v2 index: %s: truncated base file", path)
	}
	m, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	foot := m[size-footerSize:]
	dataLen := int64(binary.LittleEndian.Uint64(foot))
	nb := int64(binary.LittleEndian.Uint64(foot[8:]))
	n := int64(binary.LittleEndian.Uint64(foot[16:]))
	if string(foot[24:]) != fileMagic || dataLen < 0 || nb < 0 || nb != (n+blockNames-1)/blockNames || dataLen+8*nb+footerSize != size {
		syscall.Munmap(m)
		return nil, fmt.Errorf("v2 index: %s: corrupt base file", path)
	}
	blocks := make([]int, nb)
	for i := range blocks {
		blocks[i] = int(binary.LittleEndian.Uint64(m[dataLen+8*int64(i):]))
		if (i == 0 && blocks[i] != 0) || (i > 0 && blocks[i] <= blocks[i-1]) || int64(blocks[i]) >= dataLen {
			syscall.Munmap(m)
			return nil, errors.New("v2 index: corrupt base file block offsets")
		}
	}
	// Map every page now (the file was just written, so it is in the page
	// cache): finds then take no first-touch faults on a new base.
	var sum byte
	for i := 0; i < len(m); i += os.Getpagesize() {
		sum += m[i]
	}
	runtime.KeepAlive(sum)
	r := &run{data: m[:dataLen:dataLen], blocks: blocks, n: int(n), path: path}
	runtime.AddCleanup(r, func(m []byte) { syscall.Munmap(m) }, m)
	return r, nil
}

// retire removes a replaced base file. Snapshots that still read it keep
// the mapping until they are released.
func retire(r *run) {
	if r != nil && r.path != "" {
		os.Remove(r.path)
	}
}
