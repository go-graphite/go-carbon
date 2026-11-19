package v2index

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
)

// A base file is the base run's entries, then its block offsets (u64 each),
// then a footer: entry bytes, blocks, names (u64 each) and fileMagic. It is
// written to a temporary name and renamed, so a kill at any point leaves the
// preceding base file whole.
//
// base-N has a name log, log-N: every membership change published after
// base-N's names were taken, like an LSM's WAL beside its newest table. A
// record holds the added, then the removed names (a count and a front-coded
// run each) behind its length and CRC-32C. A base merge starts log-N with
// the live names of the levels it does not merge. A restart serves base-B
// plus logs B, B+1, ..., so find is exact as of the last publication; a
// torn last record is dropped. Logs below an installed base are removed.
// There is no fsync: a lost or torn file only means the first publication
// after a restart waits for the rebuild.
const (
	fileMagic  = "v2idxfc1"
	footerSize = 32
)

// fileHook lets tests fail or kill a base write after size entry bytes.
var fileHook func(size int) error

// SetDir keeps the base level in files under dir (page cache) instead of the
// Go heap. Call it before the first publication. The newest whole base file
// of an earlier process and its logs are published at once as revision 1,
// like an LSM opening its parts: find answers from them while the startup
// rebuild runs, and the rebuild's publication replaces them. Older and torn
// files are removed.
func (c *Catalog) SetDir(dir string) error {
	defer c.own()()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	newest, seq := "", 0
	for _, e := range entries {
		var n int
		if _, err := fmt.Sscanf(e.Name(), "base-%06d.fc", &n); err == nil && e.Name() == fmt.Sprintf("base-%06d.fc", n) && n > seq {
			newest, seq = e.Name(), n
		}
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "base-") && e.Name() != newest {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	c.dir, c.seq = dir, seq
	if newest == "" || c.current.Load() != nil {
		c.dropLogs(math.MaxInt)
		return nil
	}
	c.dropLogs(seq)
	r, err := openRun(filepath.Join(dir, newest))
	var s *Snapshot
	if err == nil {
		s, err = c.replay(&Snapshot{levels: []*level{{run: r}}, n: r.n})
	}
	if err != nil {
		// A corrupt projection is rebuilt, never served.
		if c.log != nil {
			c.log.Close()
			c.log = nil
		}
		c.dropLogs(math.MaxInt)
		return os.Remove(filepath.Join(dir, newest))
	}
	c.current.Store(s)
	return nil
}

// replay folds logs c.seq, c.seq+1, ... into base as revision 1 and keeps
// the last log open for appends, without its torn tail.
func (c *Catalog) replay(base *Snapshot) (*Snapshot, error) {
	live := map[string]bool{}
	for ; ; c.seq++ {
		data, err := os.ReadFile(c.logPath(c.seq))
		if err != nil {
			return nil, err
		}
		good, err := readLog(data, live)
		if err != nil {
			return nil, err
		}
		c.logged += good
		if _, err := os.Stat(c.logPath(c.seq + 1)); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if c.log, err = os.OpenFile(c.logPath(c.seq), os.O_WRONLY|os.O_APPEND, 0); err == nil {
			err = c.log.Truncate(int64(good))
		}
		if err != nil {
			return nil, err
		}
		break
	}
	var add, remove []string
	for name, alive := range live {
		if alive {
			add = append(add, name)
		} else {
			remove = append(remove, name)
		}
	}
	s, _, _, err := c.apply(context.Background(), base, 1, add, remove)
	if s == nil && err == nil {
		s, base.revision = base, 1
	}
	return s, err
}

// readLog folds the whole records of a log into live (name -> listed) and
// returns the bytes they span.
func readLog(data []byte, live map[string]bool) (int, error) {
	off := 0
	for off+8 <= len(data) {
		size := int(binary.LittleEndian.Uint32(data[off:]))
		if size > len(data)-off-8 {
			break
		}
		bad := errors.New("v2 index: corrupt name log")
		if crc32.Checksum(data[off+8:off+8+size], castagnoli) != binary.LittleEndian.Uint32(data[off+4:]) {
			return 0, bad
		}
		p := data[off+8 : off+8+size]
		for _, listed := range []bool{true, false} {
			n, k := binary.Uvarint(p)
			if k <= 0 {
				return 0, bad
			}
			p = p[k:]
			m, k := binary.Uvarint(p)
			if k <= 0 || m > uint64(len(p)-k) {
				return 0, bad
			}
			data := p[k : k+int(m)]
			p = p[k+int(m):]
			var key []byte
			for i := uint64(0); i < n; i++ {
				shared, k := binary.Uvarint(data)
				if k <= 0 || shared > uint64(len(key)) {
					return 0, bad
				}
				data = data[k:]
				size, k := binary.Uvarint(data)
				if k <= 0 || size > uint64(len(data)-k) {
					return 0, bad
				}
				key = append(key[:shared], data[k:k+int(size)]...)
				data = data[k+int(size):]
				live[string(key)] = listed
			}
			if len(data) != 0 {
				return 0, bad
			}
		}
		off += 8 + size
	}
	return off, nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// appendLog records one published change in the current log. A failed
// write removes every log, so a restart rebuilds instead of serving less.
func (c *Catalog) appendLog(add, remove []string) {
	if c.log == nil || len(add)+len(remove) == 0 {
		return
	}
	rec := make([]byte, 8, 64)
	for _, names := range [][]string{add, remove} {
		r := newRun(names)
		rec = binary.AppendUvarint(rec, uint64(r.n))
		rec = binary.AppendUvarint(rec, uint64(len(r.data)))
		rec = append(rec, r.data...)
	}
	binary.LittleEndian.PutUint32(rec, uint32(len(rec)-8))
	binary.LittleEndian.PutUint32(rec[4:], crc32.Checksum(rec[8:], castagnoli))
	if _, err := c.log.Write(rec); err != nil {
		c.logFailed(err)
	}
	c.logged += len(rec)
}

func (c *Catalog) logFailed(err error) {
	c.err.Store(&err)
	if c.log != nil {
		c.log.Close()
	}
	c.log = nil
	c.dropLogs(math.MaxInt)
}

func (c *Catalog) logPath(seq int) string {
	return filepath.Join(c.dir, fmt.Sprintf("log-%06d", seq))
}

// dropLogs removes the logs below seq.
func (c *Catalog) dropLogs(seq int) {
	if c.dir == "" {
		return
	}
	entries, _ := os.ReadDir(c.dir)
	for _, e := range entries {
		var n int
		if _, err := fmt.Sscanf(e.Name(), "log-%06d", &n); err == nil && n < seq {
			os.Remove(filepath.Join(c.dir, e.Name()))
		}
	}
}

// nextPath names the next base file and starts its log with the live names
// of keep, the levels that base will not hold.
func (c *Catalog) nextPath(keep []*level) string {
	if c.dir == "" {
		return ""
	}
	c.seq++
	if c.log != nil {
		c.log.Close()
	}
	var err error
	if c.log, err = os.OpenFile(c.logPath(c.seq), os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o644); err != nil {
		c.logFailed(err)
	}
	c.appendLog(slices.Collect((&Snapshot{levels: keep}).Names()), nil)
	c.logged = 0
	return filepath.Join(c.dir, fmt.Sprintf("base-%06d.fc", c.seq))
}

// writeBase writes a whole new base; a file failure keeps it on the heap.
func (c *Catalog) writeBase(ctx context.Context, fill func(*levelWriter) error) (*run, error) {
	r, err := writeRun(ctx, c.nextPath(nil), fill)
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
