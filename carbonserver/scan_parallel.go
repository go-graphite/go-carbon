package carbonserver

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/vellum"
	"github.com/go-graphite/go-whisper"
	"github.com/klauspost/compress/gzip"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// Parallel full scan.
//
// A sequential filepath.Walk resolves the full path of every entry and builds
// the next snapshot FST on one core, so a tree of tens of millions of files
// takes most of an hour. When the previous snapshot is available, its row
// distribution splits the ordered key space into ranges, and running ranges are
// split again when workers run out of work. Workers walk ranges relative to open
// directory descriptors, and each builds a complete FST shard, metadata rows and
// a gzip member of the file list cache, spooled to the worker's unlinked temp
// files. One committer copies those outputs in key order, so the published
// generation is equivalent to a sequential scan of the same tree, and workers
// never wait for an earlier range or keep its outputs in memory.

const scanMaxWorkers = 16

// scanWorkers returns how many goroutines walk the tree; 1 selects the
// sequential scan, which also builds the first snapshot.
func (u *fileListUpdate) scanWorkers(dir string) int {
	l := u.listener
	if !l.trieIndex || !l.concurrentIndex || u.fileIndex == nil || u.trieIdx == nil || u.trieIdx.snapshot == nil ||
		l.fileListCache == "" || l.fileListCacheVersion != FLCVersion2 || l.internalStatsDir != "" || dir != l.whisperData {
		return 1
	}
	if l.scanWorkers > 0 {
		return l.scanWorkers
	}
	return min(scanMaxWorkers, max(2, runtime.GOMAXPROCS(0)/4))
}

type parallelScan struct {
	u        *fileListUpdate
	root     string
	old      *indexSnapshot
	live     *trieIndex
	estimate func(string) (int64, int64, int64)
	cached   map[string]struct{}
	now      int64
	stop     chan struct{}
	out      *parallelScanOutput
	sched    *scanScheduler

	spoolMu sync.Mutex
	spools  []*scanSpool
}

// scanRange is one contiguous range of encoded keys and everything a worker
// produced for it.
type scanRange struct {
	index      int
	start, end []byte
	expected   int
	// Rows of the previous generation in [start, end) are [firstRow, endRow);
	// progress is the row the walk has reached, an estimate of work left.
	firstRow, endRow int64
	progress         atomic.Int64

	shard      *fstShard
	rows       int
	spool      *scanSpool
	segments   [scanSpoolFiles]scanSegment
	newMetrics []scanNewMetric
	cacheHits  []string

	files, metricsKnown, lockFiles, oooFiles, oooPhysicalBytes uint64

	walkErr   error // the tree could not be read completely
	outputErr error // only the saved generation is unusable
	cancelled bool
	elapsed   time.Duration
	split     atomic.Bool // a waiting worker asks for half of the work left
}

type scanNewMetric struct {
	path                                       string
	logical, physical, dataPoints, firstSeenAt int64
}

func (u *fileListUpdate) scanFilesParallel(dir string, quotaAndUsageStatTicker <-chan time.Time, workers int) bool {
	out, err := newParallelScanOutput(u.listener.fileListCache, u.listener.whisperData)
	if err != nil {
		u.logger.Warn("parallel scan unavailable; scanning sequentially", zap.Error(err))
		return u.scanFilesSequential(dir, quotaAndUsageStatTicker)
	}
	u.logWhisperDataDir(dir)
	started := time.Now()
	s := &parallelScan{
		u: u, root: dir, old: u.trieIdx.snapshot, live: u.trieIdx, estimate: u.listener.estimateSize,
		cached: u.cacheMetricNames, now: started.Unix(), stop: make(chan struct{}), out: out,
	}
	// Realtime notifications received during the scan are carried into the
	// next generation, as with the sequential snapshot writer.
	u.snapshotWriter = out.snapshot
	plan := (*indexSnapshot).scanRanges
	if scanPlanOverride != nil {
		plan = scanPlanOverride
	}
	sched := newScanScheduler(plan(s.old, workers))
	s.sched = sched
	planTime := time.Since(started)

	defer s.closeSpools()
	results := make(chan *scanRange, scanMaxRanges)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			w := &scanWalker{scan: s, buf: make([]byte, 64<<10)}
			for r := sched.next(); r != nil; r = sched.next() {
				started := time.Now()
				w.walkRange(r)
				r.elapsed = time.Since(started)
				sched.complete(r)
				results <- r
			}
		})
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	completed := make(map[string]*scanRange)
	var newMetrics int
	var slowest time.Duration
	var cacheHits []string
	var broken bool
	head := []byte{0}
	receive := func(r *scanRange) {
		if _, dup := completed[string(r.start)]; dup || bytes.Compare(r.start, r.end) >= 0 {
			broken = true
			return
		}
		completed[string(r.start)] = r
		for r := completed[string(head)]; r != nil; r = completed[string(head)] {
			delete(completed, string(head))
			head = r.end
			newMetrics += len(r.newMetrics)
			slowest = max(slowest, r.elapsed)
			u.logger.Debug("scan range", zap.Int("range", r.index), zap.ByteString("start", r.start), zap.Duration("elapsed", r.elapsed),
				zap.Uint64("files", r.files), zap.Int("metrics", r.rows), zap.Int("expected", r.expected))
			cacheHits = append(cacheHits, r.cacheHits...)
			s.commit(r)
		}
	}
	for running := true; running; {
		select {
		case r := <-results:
			receive(r)
		case <-workersDone:
			for len(results) > 0 {
				receive(<-results)
			}
			running = false
		case <-ticker.C:
			// The sequential walk refreshes these between files.
			u.refreshQuotaAndRealtimeMetrics(quotaAndUsageStatTicker)
		case <-u.listener.exitChan:
			close(s.stop)
			sched.stop()
			<-workersDone
			s.abortOutput()
			u.scanCancelled = true
			return false
		}
	}
	// Ranges partition the key space, so the commit chain consumes all of them.
	if broken || len(completed) > 0 || !bytes.Equal(head, scanEnd) {
		u.logger.Error("parallel scan ranges do not partition the tree; keeping the previous index generation",
			zap.Int("uncommitted_ranges", len(completed)), zap.ByteString("committed_until", head))
		u.scanFailed = true
		s.abortOutput()
	}
	for _, name := range cacheHits {
		delete(u.cacheMetricNames, name)
	}
	walkTime := time.Since(started)
	if s.out != nil {
		records := s.out.snapshot.manifest.Records
		if err := s.out.finish(); err != nil {
			u.logger.Warn("failed to publish index snapshot", zap.Error(err))
			s.abortOutput()
		} else {
			u.snapshotReady = true
			u.logger.Info("index snapshot written", zap.Uint64("records", records))
		}
	}
	u.snapshotWriter = nil
	u.infos = append(u.infos,
		zap.Int("scan_workers", workers), zap.Int("scan_ranges", sched.ranges), zap.Int("scan_splits", sched.splits), zap.Duration("scan_idle_time", sched.idle),
		zap.Int("scan_new_metrics", newMetrics), zap.Duration("scan_plan_time", planTime),
		zap.Duration("scan_walk_time", walkTime), zap.Duration("scan_slowest_range", slowest), zap.Duration("scan_publish_time", time.Since(started)-walkTime),
	)
	return true
}

// scanPlanOverride replaces the initial plan in tests.
var scanPlanOverride func(*indexSnapshot, int) []snapshotMetricRange

// scanEnd bounds the last range: every path key starts with the encoded '/'.
var scanEnd = []byte{1}

// scanMaxRanges bounds splitting, and with it the shared-prefix nodes that
// joining the shards re-encodes.
const scanMaxRanges = 4096

// scanScheduler hands out key ranges in order and rebalances them while they
// run: a worker without work asks the running range with the most work left to
// give half of it away as a new range (see scanWalker.split).
type scanScheduler struct {
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []*scanRange // not started, by start key
	active  []*scanRange // being walked, by start key
	waiting int
	ranges  int
	splits  int
	idle    time.Duration // summed time workers waited for work
	stopped bool
}

// A range is split only when the half given away holds enough saved rows to
// outweigh starting another walk. Tests lower it for small trees.
var scanMinSplitRows int64 = 2048

func newScanScheduler(planned []snapshotMetricRange) *scanScheduler {
	s := &scanScheduler{ranges: len(planned)}
	s.cond = sync.NewCond(&s.mu)
	for i, p := range planned {
		r := &scanRange{index: i, start: p.start, end: p.end, expected: p.last - p.first, firstRow: int64(p.first), endRow: int64(p.last)}
		r.progress.Store(r.firstRow)
		s.queue = append(s.queue, r)
	}
	return s
}

func (r *scanRange) remaining() int64 {
	return r.endRow - r.progress.Load()
}

func insertScanRange(list []*scanRange, r *scanRange) []*scanRange {
	i, _ := slices.BinarySearchFunc(list, r.start, func(e *scanRange, start []byte) int { return bytes.Compare(e.start, start) })
	return slices.Insert(list, i, r)
}

// next returns the next range to walk in key order, or nil once every range
// is done. Without queued ranges it asks the running ranges with the most work
// left to give half of it away, one for each waiting worker.
func (s *scanScheduler) next() *scanRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	var started time.Time
	defer func() {
		if !started.IsZero() {
			s.idle += time.Since(started)
		}
	}()
	for !s.stopped {
		if len(s.queue) > 0 {
			r := s.queue[0]
			s.queue = s.queue[1:]
			s.active = insertScanRange(s.active, r)
			return r
		}
		if len(s.active) == 0 {
			return nil
		}
		if started.IsZero() {
			started = time.Now()
		}
		s.waiting++
		s.requestSplits()
		s.cond.Wait()
		s.waiting--
	}
	return nil
}

func (s *scanScheduler) requestSplits() {
	if s.ranges >= scanMaxRanges {
		return
	}
	// Walkers advance concurrently: sort a consistent copy of their progress.
	type candidate struct {
		r    *scanRange
		left int64
	}
	largest := make([]candidate, 0, len(s.active))
	for _, r := range s.active {
		largest = append(largest, candidate{r, r.remaining()})
	}
	slices.SortFunc(largest, func(a, b candidate) int { return cmp.Compare(b.left, a.left) })
	for _, c := range largest[:min(len(largest), s.waiting)] {
		if c.left < 2*scanMinSplitRows {
			break
		}
		c.r.split.Store(true)
	}
}

// donate is called by the worker walking r: everything from split on, which
// starts at splitRow of the previous generation, becomes a new range after it.
func (s *scanScheduler) donate(r *scanRange, split []byte, splitRow int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.split.Store(false)
	if s.ranges >= scanMaxRanges {
		return
	}
	d := &scanRange{index: s.ranges, start: split, end: r.end, firstRow: splitRow, endRow: r.endRow, expected: int(r.endRow - splitRow)}
	d.progress.Store(splitRow)
	s.queue = insertScanRange(s.queue, d)
	s.ranges++
	s.splits++
	r.end, r.endRow = split, splitRow
	s.cond.Broadcast()
}

func (s *scanScheduler) complete(r *scanRange) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := slices.Index(s.active, r)
	s.active = slices.Delete(s.active, i, i+1)
	r.split.Store(false)
	s.cond.Broadcast()
}

func (s *scanScheduler) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	s.cond.Broadcast()
}

// commit applies one range in key order: index updates first, then the
// saved generation, which a failed or incomplete range discards entirely.
func (s *parallelScan) commit(r *scanRange) {
	u := s.u
	u.filesLen += int(r.files)
	u.metricsKnown += r.metricsKnown
	u.lockFiles += r.lockFiles
	u.oooFiles += r.oooFiles
	u.oooPhysicalBytes += r.oooPhysicalBytes
	if r.walkErr != nil {
		u.scanFailed = true
	}
	for _, m := range r.newMetrics {
		if _, err := u.trieIdx.insert(m.path, m.logical, m.physical, m.dataPoints, m.firstSeenAt); err != nil {
			u.listener.logTrieInsertError(u.logger, "updateFileList.trie: failed to index path", m.path, err)
		}
	}
	if s.out != nil {
		err := r.outputErr
		if err == nil && !u.scanFailed {
			err = s.out.add(r)
		}
		if err != nil {
			u.logger.Warn("failed to build index snapshot", zap.Error(err))
		}
		if err != nil || u.scanFailed {
			s.abortOutput()
		}
	}
	*r = scanRange{index: r.index, start: r.start, end: r.end}
}

const (
	scanSpoolFST = iota
	scanSpoolMeta
	scanSpoolFLC
	scanSpoolFiles
)

// scanSpool holds one worker's completed range outputs until they are
// committed. The files are unlinked on creation and closed after the scan.
type scanSpool struct {
	files   [scanSpoolFiles]*os.File
	writers [scanSpoolFiles]*bufio.Writer
	pos     [scanSpoolFiles]int64
}

type scanSegment struct{ off, n int64 }

func (s *parallelScan) newSpool() (*scanSpool, error) {
	sp := &scanSpool{}
	s.spoolMu.Lock()
	s.spools = append(s.spools, sp)
	s.spoolMu.Unlock()
	for i := range sp.files {
		f, err := os.CreateTemp(filepath.Dir(s.u.listener.fileListCache), ".carbon-scan-*")
		if err != nil {
			return nil, err
		}
		sp.files[i] = f
		if err := os.Remove(f.Name()); err != nil {
			return nil, err
		}
		sp.writers[i] = bufio.NewWriterSize(f, 256<<10)
	}
	return sp, nil
}

func (s *parallelScan) closeSpools() {
	s.spoolMu.Lock()
	defer s.spoolMu.Unlock()
	for _, sp := range s.spools {
		for _, f := range sp.files {
			if f != nil {
				_ = f.Close()
			}
		}
	}
	s.spools = nil
}

type scanSpoolWriter struct {
	sp *scanSpool
	i  int
}

func (w scanSpoolWriter) Write(p []byte) (int, error) {
	n, err := w.sp.writers[w.i].Write(p)
	w.sp.pos[w.i] += int64(n)
	return n, err
}

func (sp *scanSpool) mark() [scanSpoolFiles]scanSegment {
	var segments [scanSpoolFiles]scanSegment
	for i := range segments {
		segments[i].off = sp.pos[i]
	}
	return segments
}

// seal ends the segments started at mark and flushes them for the committer.
func (sp *scanSpool) seal(segments *[scanSpoolFiles]scanSegment) error {
	var err error
	for i := range segments {
		segments[i].n = sp.pos[i] - segments[i].off
		err = errors.Join(err, sp.writers[i].Flush())
	}
	return err
}

func (sp *scanSpool) reader(i int, segment scanSegment) *io.SectionReader {
	return io.NewSectionReader(sp.files[i], segment.off, segment.n)
}

func (s *parallelScan) abortOutput() {
	if s.out != nil {
		s.out.abort()
		s.out = nil
	}
	s.u.snapshotWriter = nil
}

func (s *parallelScan) stopped() bool {
	select {
	case <-s.stop:
		return true
	case <-s.u.listener.exitChan:
		return true
	default:
		return false
	}
}

func (s *parallelScan) logWalkError(path []byte, err error) {
	s.u.logger.Info("error processing", zap.ByteString("path", path), zap.Error(err))
}

// scanRanges splits the key space for parallel scans by saved metrics. Split
// ranges cover
// every saved metric but may skip keys between a parent prefix and its first
// child, such as a directory's own key, so each range starts where the
// previous one ends. Every path key starts with the encoded '/'.
func (s *indexSnapshot) scanRanges(workers int) []snapshotMetricRange {
	// Small ranges keep each worker's in-memory FST shard small; running ranges
	// are split further when workers run out of work.
	return s.scanRangesInto(workers*16, scanMaxRanges/4)
}

func (s *indexSnapshot) scanRangesInto(parts, maxRanges int) []snapshotMetricRange {
	ranges := s.splitMetricRangeInto(snapshotMetricRange{[]byte{0}, scanEnd, 0, s.index.Len()}, parts, maxRanges)
	if len(ranges) == 0 {
		return []snapshotMetricRange{{[]byte{0}, scanEnd, 0, s.index.Len()}}
	}
	ranges[0].start = []byte{0}
	for i := 1; i < len(ranges); i++ {
		ranges[i].start = ranges[i-1].end
	}
	return ranges
}

type scanWalker struct {
	scan        *parallelScan
	r           *scanRange
	buf         []byte
	levels      [][]scanDirent
	pos, prefix []int // per depth: current entry and length of its directory key
	dirs        int   // directories entered, to retry a split only after descending
	splitTried  int
	path, key   []byte // trimmed path of the current entry and its encoded key
	first, last []byte
	fst         *vellum.Builder
	fstBuf      bytes.Buffer
	gz          *gzip.Writer
	spool       *scanSpool
	record, row []byte
	st, sidecar unix.Stat_t

	old     *vellum.FSTIterator
	oldKey  []byte
	oldRow  uint64
	oldDone bool
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
	w.openOld()
	fd, err := scanOpenRoot(w.scan.root)
	if err != nil {
		w.walkError(err)
	} else {
		if r.index == 0 {
			r.files++ // the data directory itself, as filepath.Walk reports it
		}
		w.walkDir(fd, 0)
		_ = unix.Close(fd)
	}
	if w.old != nil {
		_ = w.old.Close()
		w.old = nil
	}
	w.finish()
}

func (w *scanWalker) openOld() {
	it, err := w.scan.old.index.Iterator(w.r.start, w.r.end)
	w.old, w.oldDone = it, err != nil
	if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
		w.r.outputErr = err
	}
	if !w.oldDone {
		w.oldKey, w.oldRow = it.Current()
	}
}

// lookupOld finds key in the previous generation. Keys arrive in increasing
// order, so one iterator over the range replaces a lookup per file.
func (w *scanWalker) lookupOld(key []byte) (uint64, bool) {
	for !w.oldDone {
		switch c := bytes.Compare(w.oldKey, key); {
		case c == 0:
			return w.oldRow, true
		case c > 0:
			return 0, false
		}
		if err := w.old.Next(); err != nil {
			w.oldDone = true
			if !errors.Is(err, vellum.ErrIteratorDone) {
				w.r.outputErr = err
			}
			break
		}
		w.oldKey, w.oldRow = w.old.Current()
		w.r.progress.Store(int64(w.oldRow))
	}
	return 0, false
}

func (w *scanWalker) walkDir(fd, depth int) {
	if w.scan.stopped() {
		w.r.cancelled = true
		return
	}
	w.dirs++
	for len(w.levels) <= depth {
		w.levels, w.pos, w.prefix = append(w.levels, nil), append(w.pos, 0), append(w.prefix, 0)
	}
	entries, err := scanReadDir(fd, w.buf, w.levels[depth][:0])
	w.levels[depth] = entries
	if err != nil {
		w.walkError(err)
		return
	}
	// Byte order of names is filepath.Walk order, and with '/' encoded as the
	// smallest byte it is also the order of snapshot keys.
	slices.SortFunc(entries, func(a, b scanDirent) int { return strings.Compare(a.name, b.name) })
	pathLen, keyLen := len(w.path), len(w.key)
	defer func() {
		w.path, w.key = w.path[:pathLen], w.key[:keyLen]
		// Keep small listings for reuse but not the rare huge directory's.
		if cap(w.levels[depth]) > 4096 {
			w.levels[depth] = nil
		}
	}()
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
		w.pos[depth], w.prefix[depth] = i, keyLen
		if w.r.split.Load() {
			w.split(depth)
		}
		typ := e.typ
		if typ == scanTypeUnknown {
			if err := scanStat(fd, e.name, false, &w.st); err != nil {
				w.walkError(err)
				continue
			}
			typ = scanModeType(uint32(w.st.Mode))
		}
		wsp := strings.HasSuffix(e.name, ".wsp")
		if typ == scanTypeDir {
			if inRange {
				w.r.files++
				if wsp {
					w.metric(fd, e.name, entries, true)
				}
			}
			if subtree {
				if child, err := scanOpenDir(fd, e.name); err != nil {
					w.walkError(err)
				} else {
					w.walkDir(child, depth+1)
					_ = unix.Close(child)
				}
			}
			continue
		}
		if !inRange {
			continue
		}
		if typ == scanTypeRegular && strings.HasSuffix(e.name, ".lock") {
			w.r.lockFiles++
		} else if typ == scanTypeRegular && strings.HasSuffix(e.name, ".ooo") {
			if err := scanStat(fd, e.name, false, &w.st); err != nil {
				w.walkError(err)
				continue
			}
			w.r.oooFiles++
			w.r.oooPhysicalBytes += uint64(w.st.Blocks) * 512
		}
		if wsp {
			w.metric(fd, e.name, entries, false)
		}
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
	key := make([]byte, 0, w.prefix[d]+1+len(name))
	return append(append(append(key, w.key[:w.prefix[d]]...), 0), name...)
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

func (w *scanWalker) metric(fd int, name string, siblings []scanDirent, isDir bool) {
	if err := scanStat(fd, name, false, &w.st); err != nil {
		w.walkError(err)
		return
	}
	if !isDir {
		w.r.files++
	}
	logical, physical := w.st.Size, w.st.Blocks*512
	// The listing shows whether a sidecar exists; most metrics have none.
	sidecar := whisper.OutOfOrderSidecarPath(name)
	if _, ok := slices.BinarySearchFunc(siblings, sidecar, func(e scanDirent, name string) int { return strings.Compare(e.name, name) }); ok {
		if err := scanStat(fd, sidecar, true, &w.sidecar); err == nil {
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
	} else if row, ok := w.lookupOld(w.key); ok {
		w.r.metricsKnown++
		firstSeenAt = w.scan.old.openedAt
		if values, err := w.scan.old.metadata.get(row); err == nil && values[3] != 0 {
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
	if r.outputErr == nil && r.walkErr == nil && !r.cancelled && r.rows > 0 {
		// Only prefixes shared with a neighbouring range are re-encoded when
		// joining; they are no longer than the common prefix with the bounds.
		left, right := scanCommonPrefix(w.first, r.start), scanCommonPrefix(w.last, r.end)
		data := w.fstBuf.Bytes()
		if r.shard, r.outputErr = newFSTShard(data, bytes.Clone(w.first), bytes.Clone(w.last), uint64(r.rows), left, right); r.outputErr == nil {
			_, r.outputErr = (scanSpoolWriter{w.spool, scanSpoolFST}).Write(fstShardBody(data))
		}
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

// parallelScanOutput writes the next generation: the FST from joined shards,
// metadata rows in key order, and the file list cache as consecutive gzip
// members, which gzip readers decode as one stream.
type parallelScanOutput struct {
	snapshot *indexSnapshotWriter
	fst      *fstJoiner
	cache    string
	flc      *os.File
	flcBuf   *bufio.Writer
	rows     []byte
}

func newParallelScanOutput(cache, root string) (_ *parallelScanOutput, err error) {
	o := &parallelScanOutput{cache: cache}
	defer func() {
		if err != nil {
			o.abort()
		}
	}()
	if o.snapshot, err = newIndexSnapshotFiles(cache, root); err != nil {
		return nil, err
	}
	if o.fst, err = newFSTJoiner(o.snapshot.indexBuffer); err != nil {
		return nil, err
	}
	if o.flc, err = os.Create(cache + ".tmp"); err != nil {
		return nil, err
	}
	o.flcBuf = bufio.NewWriterSize(o.flc, 1<<20)
	var magic bytes.Buffer
	gz := gzip.NewWriter(&magic)
	if _, err = gz.Write([]byte(version2MagicString)); err != nil {
		return nil, err
	}
	if err = gz.Close(); err != nil {
		return nil, err
	}
	_, err = o.flcBuf.Write(magic.Bytes())
	return o, err
}

func (o *parallelScanOutput) add(r *scanRange) error {
	if r.rows == 0 {
		return nil
	}
	if err := o.fst.add(r.shard, r.spool.reader(scanSpoolFST, r.segments[scanSpoolFST])); err != nil {
		return err
	}
	if r.segments[scanSpoolMeta].n != int64(r.rows)*32 {
		return fmt.Errorf("parallel scan spooled %d metadata bytes for %d rows", r.segments[scanSpoolMeta].n, r.rows)
	}
	if o.rows == nil {
		o.rows = make([]byte, 32<<10)
	}
	meta := r.spool.reader(scanSpoolMeta, r.segments[scanSpoolMeta])
	for left := r.segments[scanSpoolMeta].n; left > 0; {
		chunk := o.rows[:min(int64(len(o.rows)), left)]
		if _, err := io.ReadFull(meta, chunk); err != nil {
			return err
		}
		left -= int64(len(chunk))
		for ; len(chunk) > 0; chunk = chunk[32:] {
			var values [4]int64
			for i := range values {
				values[i] = int64(binary.LittleEndian.Uint64(chunk[i*8:]))
			}
			if err := o.snapshot.metadata.append(values); err != nil {
				return err
			}
		}
	}
	o.snapshot.manifest.Records += uint64(r.rows)
	_, err := io.Copy(o.flcBuf, r.spool.reader(scanSpoolFLC, r.segments[scanSpoolFLC]))
	return err
}

// finish publishes the file list cache before the snapshot that names it as
// its source, like the sequential writers.
func (o *parallelScanOutput) finish() error {
	if err := o.fst.finish(); err != nil {
		return err
	}
	err := errors.Join(o.flcBuf.Flush(), o.flc.Sync())
	err = errors.Join(err, o.flc.Close())
	o.flc = nil
	if err == nil {
		err = os.Rename(o.cache+".tmp", o.cache)
	}
	if err != nil {
		_ = os.Remove(o.cache + ".tmp")
		return err
	}
	return o.snapshot.finish()
}

func (o *parallelScanOutput) abort() {
	if o.flc != nil {
		_ = o.flc.Close()
		_ = os.Remove(o.cache + ".tmp")
		o.flc = nil
	}
	if o.snapshot != nil {
		_ = o.snapshot.abort()
	}
}
