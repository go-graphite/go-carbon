package carbonserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/blevesearch/vellum"
	"github.com/go-graphite/go-whisper"
	"github.com/klauspost/compress/gzip"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// scanWalker walks the ranges handed to one worker.
//
// Directories that did not change since the previous scan are not opened.
// A directory's ctime moves to the current time whenever an entry is created,
// removed or renamed in it, and cannot be set back. So a directory whose ctime
// is older than the previous scan's start, which listed it or reused an older
// listing still valid then, has the same entries now. Its listing is rebuilt
// from the previous generation: metric files and the directories leading to
// them from the metric index, everything else the walk acts on from the
// directory catalogue (see scan_dirs.go). Every metric is still stat'ed, so
// sizes stay current; only the listing is reused.
type scanWalker struct {
	scan        *parallelScan
	r           *scanRange
	buf, name   []byte
	frames      []scanFrame
	levels      [][]scanDirent
	names       []scanNames // per depth: the listing's names
	pos         []int       // per depth: the entry being walked
	dirs        int         // directories entered, to retry a split only after descending
	splitTried  int
	path, key   []byte // trimmed path of the current entry and its encoded key
	first, last []byte
	fst         *vellum.Builder
	fstBuf      bytes.Buffer
	cat         scanCatalogueWriter
	gz          *gzip.Writer
	spool       *scanSpool
	record, row []byte
	rel, side   []byte
	st, sidecar unix.Stat_t
}

// scanFrame is one directory on the current path.
type scanFrame struct {
	fd      int // -1 when the listing comes from the previous generation
	anchor  int // depth of the nearest open directory at or above this one
	pathLen int // this directory's lengths of path and key
	keyLen  int
	// Previous generation states after this directory's key and separator.
	keys, dirs fstState
	// The directory's catalogue record, decided once its walk is known.
	pending, flushed, hasMetric bool
}

// scanDirent is a directory entry, with what the previous generation knew
// about it.
type scanDirent struct {
	name string
	typ  scanType
	// row of the metric in the previous generation, if any
	row    int64
	hasRow bool
	// previous generation states after "name\x00", for directories
	keys, dirs fstState
	recorded   bool // the catalogue has a record for this entry
}

func (w *scanWalker) walkRange(r *scanRange) {
	w.r = r
	w.path, w.key = w.path[:0], w.key[:0]
	w.splitTried = -1
	w.fstBuf.Reset()
	var err error
	if w.spool == nil {
		w.spool, err = w.scan.newSpool()
	}
	if err == nil && w.fst == nil {
		w.fst, err = vellum.New(&w.fstBuf, nil)
	} else if err == nil {
		err = w.fst.Reset(&w.fstBuf)
	}
	if err == nil {
		err = w.cat.reset()
	}
	if err != nil {
		r.outputErr = err
	} else {
		r.spool, r.segments = w.spool, w.spool.mark()
		flc := scanSpoolWriter{w.spool, scanSpoolFLC}
		if w.gz == nil {
			w.gz = gzip.NewWriter(flc)
		} else {
			w.gz.Reset(flc)
		}
	}
	fd, err := scanOpenRoot(w.scan.root)
	if err != nil {
		w.walkError(err)
	} else {
		if r.index == 0 {
			r.files++ // the data directory itself, as filepath.Walk reports it
		}
		old := w.scan.old
		w.frames = append(w.frames[:0], scanFrame{fd: fd, keys: old.keys.accept(old.keys.start(), 0)})
		if old.dirs != nil {
			w.frames[0].dirs = old.dirs.accept(old.dirs.start(), 0)
		}
		w.walkDir(0)
		_ = unix.Close(fd)
	}
	w.finish()
}

func (w *scanWalker) frame(depth int) *scanFrame { return &w.frames[depth] }

// statAt stats an entry of the directory at depth, relative to the nearest
// open directory.
func (w *scanWalker) statAt(depth int, name string, follow bool, st *unix.Stat_t) error {
	f := w.frame(depth)
	a := w.frame(f.anchor)
	w.rel = w.rel[:0]
	if f != a {
		w.rel = append(append(w.rel, w.path[a.pathLen+1:f.pathLen]...), '/')
	}
	w.rel = append(append(w.rel, name...), 0)
	return scanStatPath(a.fd, w.rel, follow, st)
}

func (w *scanWalker) openAt(depth int, name string) (int, error) {
	f := w.frame(depth)
	if f.fd >= 0 {
		return scanOpenDir(f.fd, name)
	}
	a := w.frame(f.anchor)
	return scanOpenDir(a.fd, w.relative(a, f, name))
}

func (w *scanWalker) relative(anchor, f *scanFrame, name string) string {
	w.rel = append(append(append(w.rel[:0], w.path[anchor.pathLen+1:f.pathLen]...), '/'), name...)
	return string(w.rel)
}

// listDir reads the directory at depth: from disk, or from the previous
// generation when it did not change.
func (w *scanWalker) listDir(depth int) ([]scanDirent, error) {
	f := w.frame(depth)
	entries, names := w.levels[depth][:0], &w.names[depth]
	names.reset()
	if f.fd < 0 {
		entries = w.replayDir(f, names, entries)
		w.r.replayedDirs++
		return entries, nil
	}
	w.r.listedDirs++
	entries, err := scanReadDir(f.fd, w.buf, names, entries)
	if err != nil {
		return entries, err
	}
	// Byte order of names is filepath.Walk order, and with '/' encoded as the
	// smallest byte it is also the order of snapshot keys.
	slices.SortFunc(entries, func(a, b scanDirent) int { return strings.Compare(a.name, b.name) })
	return entries, nil
}

// replayDir rebuilds a listing from the metric index and the catalogue.
func (w *scanWalker) replayDir(f *scanFrame, names *scanNames, entries []scanDirent) []scanDirent {
	old := w.scan.old
	old.keys.names(f.keys, &w.name, func(name []byte, final bool, value uint64, child fstState) {
		e := scanDirent{name: names.add(name), typ: scanTypeRegular, row: int64(value), hasRow: final, keys: child}
		if child.ok {
			e.typ = scanTypeDir
		}
		entries = append(entries, e)
	})
	metrics := len(entries)
	i := 0
	old.dirs.names(f.dirs, &w.name, func(name []byte, final bool, value uint64, child fstState) {
		for i < metrics && entries[i].name < string(name) {
			i++
		}
		var e *scanDirent
		if i < metrics && entries[i].name == string(name) {
			e = &entries[i]
		} else {
			entries = append(entries, scanDirent{name: names.add(name), typ: scanTypeRegular})
			e = &entries[len(entries)-1]
		}
		e.dirs = child
		if final {
			e.recorded = true
			if typ := scanType(value); typ == scanTypeDir || !e.hasRow {
				e.typ = typ
			}
		}
		if child.ok {
			e.typ = scanTypeDir
		}
	})
	if len(entries) > metrics {
		slices.SortFunc(entries, func(a, b scanDirent) int { return strings.Compare(a.name, b.name) })
	}
	return entries
}

// lookupOld fills in what the previous generation knew about a listed entry.
func (w *scanWalker) lookupOld(f *scanFrame, e *scanDirent, dir bool) {
	old := w.scan.old
	s := old.keys.acceptString(f.keys, e.name)
	if final, row := old.keys.final(s); final {
		e.row, e.hasRow = int64(row), true
	}
	if !dir {
		return
	}
	e.keys = old.keys.accept(s, 0)
	if old.dirs != nil {
		d := old.dirs.acceptString(f.dirs, e.name)
		e.recorded, _ = old.dirs.final(d)
		e.dirs = old.dirs.accept(d, 0)
	}
}

func (w *scanWalker) walkDir(depth int) {
	if w.scan.stopped() {
		w.r.cancelled = true
		return
	}
	w.dirs++
	for len(w.levels) <= depth {
		w.levels, w.names, w.pos = append(w.levels, nil), append(w.names, scanNames{}), append(w.pos, 0)
	}
	entries, err := w.listDir(depth)
	w.levels[depth] = entries
	if err != nil {
		w.walkError(err)
		return
	}
	f := w.frame(depth)
	pathLen, keyLen := f.pathLen, f.keyLen
	defer func() {
		w.path, w.key = w.path[:pathLen], w.key[:keyLen]
		// Keep small listings for reuse but not the rare huge directory's.
		if cap(w.levels[depth]) > 4096 {
			w.levels[depth], w.names[depth] = nil, scanNames{}
		}
	}()
	w.lockCounts(depth, entries)
	for i := range entries {
		if w.r.cancelled {
			return
		}
		e := &entries[i]
		w.path = append(append(w.path[:pathLen], '/'), e.name...)
		w.key = append(append(w.key[:keyLen], 0), e.name...)
		if bytes.Compare(w.key, w.r.end) >= 0 {
			return // later entries and their subtrees follow this one
		}
		inRange := bytes.Compare(w.key, w.r.start) >= 0
		subtree := scanCompareSuffixed(w.key, 0, w.r.end) < 0 && scanCompareSuffixed(w.key, 1, w.r.start) > 0
		if !inRange && !subtree {
			continue
		}
		w.pos[depth] = i
		if w.r.split.Load() {
			w.split(depth)
		}
		typ := e.typ
		if typ == scanTypeUnknown {
			if err := w.statAt(depth, e.name, false, &w.st); err != nil {
				w.walkError(err)
				continue
			}
			typ = scanModeType(uint32(w.st.Mode))
		}
		wsp := strings.HasSuffix(e.name, ".wsp")
		if f.fd >= 0 && (wsp || typ == scanTypeDir) {
			w.lookupOld(f, e, typ == scanTypeDir)
		}
		if typ == scanTypeDir {
			replay := false
			if subtree && w.scan.replayCutoff > 0 && (e.keys.ok || e.dirs.ok || e.recorded) {
				// The previous generation knows this directory: reuse its
				// listing if it did not change since.
				if err := w.statAt(depth, e.name, false, &w.st); err != nil {
					w.walkError(err)
					continue
				}
				if scanModeType(uint32(w.st.Mode)) == scanTypeDir {
					replay = w.st.Ctim.Nano() < w.scan.replayCutoff
				}
			}
			if inRange {
				w.r.files++
				if wsp {
					w.metric(depth, e, entries, true)
				}
			}
			if !subtree {
				if inRange {
					// Its entries belong to later ranges, which decide.
					w.catalogue(w.key, uint64(scanTypeDir))
				}
				continue
			}
			child := scanFrame{fd: -1, anchor: f.anchor, pathLen: len(w.path), keyLen: len(w.key), keys: e.keys, dirs: e.dirs, pending: inRange}
			if !replay {
				fd, err := w.openAt(depth, e.name)
				if err != nil {
					w.walkError(err)
					continue
				}
				child.fd, child.anchor = fd, depth+1
			}
			w.frames = append(w.frames[:depth+1], child)
			w.walkDir(depth + 1)
			c := w.frame(depth + 1)
			if c.fd >= 0 {
				_ = unix.Close(c.fd)
			}
			if c.pending && !c.flushed && !c.hasMetric {
				// No metric below: only the catalogue knows this directory.
				w.catalogue(w.key[:c.keyLen], uint64(scanTypeDir))
			}
			w.frames = w.frames[:depth+1]
			f = w.frame(depth)
			continue
		}
		if !inRange {
			continue
		}
		switch {
		case f.fd >= 0 && typ == scanTypeRegular && strings.HasSuffix(e.name, ".lock"):
			w.r.lockFiles++
		case f.fd < 0 && wsp && e.hasRow:
			w.r.lockFiles++ // the lock file expected beside a replayed metric file
		}
		if strings.HasSuffix(e.name, ".ooo") {
			w.catalogue(w.key, uint64(typ))
			if typ == scanTypeRegular {
				if err := w.statAt(depth, e.name, false, &w.st); err != nil {
					w.walkError(err)
					continue
				}
				w.r.oooFiles++
				w.r.oooPhysicalBytes += uint64(w.st.Blocks) * 512
			}
		}
		if wsp {
			w.metric(depth, e, entries, false)
		}
	}
}

// lockCounts keeps the lock file gauge exact without recording every lock
// file. Whisper keeps one lock file beside each metric file, so a listing
// records only how a directory's regular *.lock files outnumber (or fall
// short of) its metric files, and a replayed listing counts one lock file per
// metric file plus that difference. The difference is the catalogue key
// "directory\x00", which sorts before the directory's entries.
func (w *scanWalker) lockCounts(depth int, entries []scanDirent) {
	f := w.frame(depth)
	dir := w.key[:f.keyLen]
	inRange := scanCompareSuffixed(dir, 0, w.r.start) >= 0 && scanCompareSuffixed(dir, 0, w.r.end) < 0
	var delta int64
	if f.fd < 0 {
		// The walk counts one lock file per metric file; this is the rest.
		if final, v := w.scan.old.dirs.final(f.dirs); final {
			delta = scanUnzigzag(v)
		}
		if inRange {
			w.r.lockFiles += uint64(delta)
		}
	} else if inRange {
		for i := range entries {
			e := &entries[i]
			typ := e.typ
			if typ == scanTypeUnknown && (strings.HasSuffix(e.name, ".lock") || strings.HasSuffix(e.name, ".wsp")) {
				if err := scanStat(f.fd, e.name, false, &w.st); err != nil {
					continue // removed since listed
				}
				typ = scanModeType(uint32(w.st.Mode))
				e.typ = typ
			}
			switch {
			case typ == scanTypeRegular && strings.HasSuffix(e.name, ".lock"):
				delta++
			case typ != scanTypeDir && strings.HasSuffix(e.name, ".wsp"):
				delta--
			}
		}
	}
	if inRange && delta != 0 {
		w.catalogue(append(dir[:len(dir):len(dir)], 0), scanZigzag(delta))
	}
}

func scanZigzag(v int64) uint64   { return uint64(v<<1) ^ uint64(v>>63) }
func scanUnzigzag(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }

// catalogue records key, after the pending records of the directories above
// it, which sort first. A directory recorded before a metric appears below it
// is merely redundant.
func (w *scanWalker) catalogue(key []byte, value uint64) {
	if w.r.outputErr != nil {
		return
	}
	for d := 1; d < len(w.frames); d++ {
		f := w.frame(d)
		if f.pending && !f.flushed && !f.hasMetric {
			f.flushed = true
			if bytes.Equal(w.key[:f.keyLen], key) {
				continue // recorded below with its own value
			}
			if err := w.cat.add(w.key[:f.keyLen], uint64(scanTypeDir)); err != nil {
				w.r.outputErr = err
				return
			}
		}
	}
	if err := w.cat.add(key, value); err != nil {
		w.r.outputErr = err
	}
}

// split gives away about half of the work left in the range. The candidates
// are later entries of the directories leading to the current entry, which all
// sort after it. The previous generation's row numbers measure the work from
// each candidate to the end of the range: it shrinks along a directory's
// entries and grows with depth. So the split goes into the shallowest directory
// whose remaining entries hold at least half of the work, at the entry closest
// to the middle. Until the walk enters another directory, nothing better than
// a failed attempt appears.
func (w *scanWalker) split(depth int) {
	if w.splitTried == w.dirs {
		return
	}
	w.splitTried = w.dirs
	r := w.r
	half := r.remaining() / 2
	if half < scanMinSplitRows {
		return
	}
	var err error
	share := func(d, i int) int64 {
		row, rowErr := w.scan.old.rowAt(w.siblingKey(d, i))
		if rowErr != nil {
			err = rowErr
			return 0
		}
		return r.endRow - row
	}
	for d := 0; d <= depth && err == nil; d++ {
		entries := w.levels[d]
		lo := w.pos[d] + 1
		// Entries at or after the end of the range belong to later ranges.
		hi := lo + sort.Search(len(entries)-lo, func(i int) bool { return bytes.Compare(w.siblingKey(d, lo+i), r.end) >= 0 })
		if lo >= hi || share(d, lo) < half {
			continue
		}
		// The last entry still leaving at least half of the work to give away.
		m := lo + sort.Search(hi-lo, func(i int) bool { return share(d, lo+i) < half }) - 1
		if m+1 < hi && abs(share(d, m+1)-half) < abs(share(d, m)-half) {
			m++
		}
		key := w.siblingKey(d, m)
		row, rowErr := w.scan.old.rowAt(key)
		if err == nil && rowErr == nil && r.endRow-row >= scanMinSplitRows && bytes.Compare(key, r.start) > 0 {
			w.scan.sched.donate(r, key, row)
		}
		return
	}
}

// siblingKey returns the key of entry i of the directory at depth d on the
// current path.
func (w *scanWalker) siblingKey(d, i int) []byte {
	name := w.levels[d][i].name
	prefix := w.frame(d).keyLen
	key := make([]byte, 0, prefix+1+len(name))
	return append(append(append(key, w.key[:prefix]...), 0), name...)
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// rowAt returns the row of the first saved key at or after key: rows are
// consecutive in key order, so it is the number of saved keys before key.
func (s *indexSnapshot) rowAt(key []byte) (int64, error) {
	defer runtime.KeepAlive(s)
	it, err := s.index.Iterator(key, nil)
	if errors.Is(err, vellum.ErrIteratorDone) {
		return int64(s.index.Len()), nil
	}
	if err != nil {
		return 0, err
	}
	defer it.Close()
	_, row := it.Current()
	return int64(row), nil
}

// walkError ignores entries removed since their directory was listed, as the
// sequential scan does; anything else leaves the scan incomplete.
func (w *scanWalker) walkError(err error) {
	if errors.Is(err, unix.ENOENT) {
		return
	}
	w.scan.logWalkError(w.path, err)
	if w.r.walkErr == nil {
		w.r.walkErr = fmt.Errorf("%s: %w", w.path, err)
	}
}

func (w *scanWalker) metric(depth int, e *scanDirent, siblings []scanDirent, isDir bool) {
	if err := w.statAt(depth, e.name, false, &w.st); err != nil {
		w.walkError(err)
		return
	}
	if !isDir {
		w.r.files++
	}
	for d := 1; d < len(w.frames); d++ {
		w.frame(d).hasMetric = true
	}
	logical, physical := w.st.Size, w.st.Blocks*512
	// The listing shows whether a sidecar exists; most metrics have none.
	var sidecar string
	if len(e.name)+len(".ooo") <= 255 {
		// whisper.OutOfOrderSidecarPath, without a string per metric.
		w.side = append(append(w.side[:0], e.name...), ".ooo"...)
		sidecar = unsafe.String(unsafe.SliceData(w.side), len(w.side)) // skipcq: GSC-G103
	} else {
		sidecar = whisper.OutOfOrderSidecarPath(e.name)
	}
	if _, ok := slices.BinarySearchFunc(siblings, sidecar, func(e scanDirent, name string) int { return strings.Compare(e.name, name) }); ok {
		if err := w.statAt(depth, sidecar, true, &w.sidecar); err == nil {
			logical += w.sidecar.Size
			physical += w.sidecar.Blocks * 512
		} else if !errors.Is(err, unix.ENOENT) {
			w.scan.u.logger.Info("failed to stat out-of-order sidecar", zap.ByteString("path", w.path), zap.Error(err))
		}
	}
	var metric string
	var dataPoints int64
	if w.scan.estimate != nil {
		metric = scanMetricName(w.path)
		_, _, dataPoints = w.scan.estimate(metric)
	}
	var firstSeenAt int64
	if _, ok := w.scan.cached[string(w.path)]; ok {
		w.r.cacheHits = append(w.r.cacheHits, string(w.path))
	} else if e.hasRow {
		w.r.metricsKnown++
		w.r.progress.Store(e.row)
		firstSeenAt = w.scan.old.openedAt
		if values, err := w.scan.old.metadata.get(uint64(e.row)); err == nil && values[3] != 0 {
			firstSeenAt = values[3]
		}
	} else {
		w.r.metricsKnown++
		if metric == "" {
			metric = scanMetricName(w.path)
		}
		// Keep the time the realtime index first saw a metric awaiting its file.
		firstSeenAt = w.scan.now
		if node := w.scan.live.mutableFileNode(metric); node != nil {
			if meta, ok := node.meta.Load().(*fileMeta); ok {
				if seen := atomic.LoadInt64(&meta.firstSeenAt); seen != 0 && seen < firstSeenAt {
					firstSeenAt = seen
				}
			}
		}
		w.r.newMetrics = append(w.r.newMetrics, scanNewMetric{string(w.path), logical, physical, dataPoints, firstSeenAt})
	}
	w.emit(logical, physical, dataPoints, firstSeenAt)
}

func scanMetricName(path []byte) string {
	if len(path) < len("/.wsp") {
		return ""
	}
	var b strings.Builder
	b.Grow(len(path) - len("/.wsp"))
	for _, c := range path[1 : len(path)-len(".wsp")] {
		if c == '/' {
			c = '.'
		}
		_ = b.WriteByte(c)
	}
	return b.String()
}

func (w *scanWalker) emit(logical, physical, dataPoints, firstSeenAt int64) {
	r := w.r
	if r.outputErr != nil {
		return
	}
	if bytes.HasSuffix(w.path, []byte("/.wsp")) {
		r.outputErr = fmt.Errorf("snapshot requires a complete metric path: %q", w.path)
		return
	}
	if r.rows == 0 {
		w.first = append(w.first[:0], w.key...)
	}
	if err := w.fst.Insert(w.key, uint64(r.rows)); err != nil {
		r.outputErr = err
		return
	}
	r.rows++
	w.last = append(w.last[:0], w.key...)
	w.row = w.row[:0]
	for _, v := range [4]int64{logical, physical, dataPoints, firstSeenAt} {
		w.row = binary.LittleEndian.AppendUint64(w.row, uint64(v))
	}
	if _, err := (scanSpoolWriter{w.spool, scanSpoolMeta}).Write(w.row); err != nil {
		r.outputErr = err
		return
	}
	w.record = appendFLCv2Entry(w.record[:0], w.path, logical, physical, dataPoints, firstSeenAt)
	if _, err := w.gz.Write(w.record); err != nil {
		r.outputErr = err
	}
}

func (w *scanWalker) finish() {
	r := w.r
	w.r = nil
	if r.outputErr != nil {
		return
	}
	r.outputErr = errors.Join(w.fst.Close(), w.gz.Close())
	complete := r.outputErr == nil && r.walkErr == nil && !r.cancelled
	if complete && r.rows > 0 {
		// Only prefixes shared with a neighbouring range are re-encoded when
		// joining; they are no longer than the common prefix with the bounds.
		left, right := scanCommonPrefix(w.first, r.start), scanCommonPrefix(w.last, r.end)
		data := w.fstBuf.Bytes()
		if r.shard, r.outputErr = newFSTShard(data, bytes.Clone(w.first), bytes.Clone(w.last), uint64(r.rows), left, right); r.outputErr == nil {
			_, r.outputErr = (scanSpoolWriter{w.spool, scanSpoolFST}).Write(fstShardBody(data))
		}
	}
	if complete && r.outputErr == nil {
		r.dirsShard, r.outputErr = w.cat.finish(r, scanSpoolWriter{w.spool, scanSpoolDirs})
	}
	if err := w.spool.seal(&r.segments); r.outputErr == nil {
		r.outputErr = err
	}
}

func scanCommonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// scanCompareSuffixed compares key followed by byte c with bound.
func scanCompareSuffixed(key []byte, c byte, bound []byte) int {
	n := len(key)
	if len(bound) <= n {
		if r := bytes.Compare(key[:len(bound)], bound); r != 0 {
			return r
		}
		return 1
	}
	if r := bytes.Compare(key, bound[:n]); r != 0 {
		return r
	}
	switch {
	case c < bound[n]:
		return -1
	case c > bound[n]:
		return 1
	case len(bound) == n+1:
		return 0
	}
	return -1
}
