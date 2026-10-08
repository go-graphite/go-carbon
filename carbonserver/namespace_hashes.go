package carbonserver

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/blevesearch/vellum"
	"github.com/go-faster/city"
	"go.uber.org/zap"
)

// Namespace hash responses use the NamespaceHashesFile v1 layout, all integers
// little-endian:
//
//	"NSHF" | version uint8 (1) | timestamp uint64
//	then per namespace, in increasing byte order:
//	name length uint32 | name | metric count uint64 | hash count uint64 | hashes uint64...
//
// Hashes are the sorted, unique CityHash64 (v1.1) values of every metric name
// below "<prefix><namespace>." with the prefix removed. Namespaces without
// metrics are omitted.
var namespaceHashesMagic = [...]byte{'N', 'S', 'H', 'F', 1}

// Each cached prefix retains one hash per metric for the current snapshot.
// Requests for further prefixes are computed without being retained.
const maxCachedNamespaceHashPrefixes = 4

const maxNamespaceHashesRequestBody = 16 << 20

// Smaller generations are hashed by a single iterator.
var namespaceHashesSplitMin uint64 = 1_000_000

type namespaceHashes struct {
	names  []string
	counts []uint64
	hashes [][]uint64 // sorted and unique
}

// snapshotRowHashes holds the CityHash64 of every snapshot metric below a
// prefix, indexed by snapshot row. Rows are consecutive in key order, so the
// metrics of any namespace, at any depth, occupy one contiguous row range.
type snapshotRowHashes struct {
	first  uint64
	hashes []uint64
	tops   []string // namespaces directly below the prefix, in key order

	// Assembled namespaces for recent enabled lists; scheduled consumers
	// repeat the same list, so most requests only merge the overlay.
	mu        sync.Mutex
	assembled map[string]*assembledNamespaces
}

const maxAssembledNamespaceLists = 2

type assembledNamespaces struct {
	ready chan struct{}
	value *namespaceHashes
	err   error
}

func namespaceListKey(include map[string]bool) string {
	if include == nil {
		return ""
	}
	names := make([]string, 0, len(include))
	for ns := range include {
		names = append(names, ns)
	}
	slices.Sort(names)
	sum := sha256.Sum256([]byte(strings.Join(names, "\n")))
	return hex.EncodeToString(sum[:])
}

// cachedNamespaces returns namespaces for include, assembling each distinct
// enabled list once per generation. Concurrent callers wait for one assembly.
func (h *snapshotRowHashes) cachedNamespaces(s *indexSnapshot, prefix string, include map[string]bool) (*namespaceHashes, error) {
	key := namespaceListKey(include)
	h.mu.Lock()
	e, ok := h.assembled[key]
	if !ok {
		if h.assembled == nil {
			h.assembled = make(map[string]*assembledNamespaces)
		}
		for k := range h.assembled {
			if len(h.assembled) < maxAssembledNamespaceLists {
				break
			}
			delete(h.assembled, k)
		}
		e = &assembledNamespaces{ready: make(chan struct{})}
		h.assembled[key] = e
	}
	h.mu.Unlock()
	if ok {
		<-e.ready
		return e.value, e.err
	}
	defer func() {
		if r := recover(); r != nil {
			e.value, e.err = nil, fmt.Errorf("namespace hashes: %v", r)
		}
		if e.err != nil {
			h.mu.Lock()
			if h.assembled[key] == e {
				delete(h.assembled, key)
			}
			h.mu.Unlock()
		}
		close(e.ready)
	}()
	e.value, e.err = h.namespaces(s, prefix, include)
	return e.value, e.err
}

type namespaceHashesEntry struct {
	ready chan struct{}
	value *snapshotRowHashes
	err   error
}

type namespaceHashSegment struct {
	name   string
	hashes []uint64
}

func namespaceHashesPrefix(prefix string) (name string, strip int) {
	name = strings.TrimRight(prefix, ".")
	if name == "" {
		return "", 0
	}
	return name, len(name) + 1
}

// namespaceHashes returns the row hashes of every snapshot metric below prefix,
// computing them once per snapshot generation. Concurrent callers wait for the
// same computation.
func (s *indexSnapshot) namespaceHashes(prefix string) (*snapshotRowHashes, error) {
	e := &namespaceHashesEntry{ready: make(chan struct{})}
	if v, loaded := s.hashes.LoadOrStore(prefix, e); loaded {
		e = v.(*namespaceHashesEntry)
		<-e.ready
		return e.value, e.err
	}
	// Beyond the limit, concurrent callers still share the computation, but
	// the result is not retained.
	retain := s.hashesCached.Add(1) <= maxCachedNamespaceHashPrefixes
	// Waiters must be released even if the computation panics.
	defer func() {
		if r := recover(); r != nil {
			e.value, e.err = nil, fmt.Errorf("namespace hashes: %v", r)
		}
		if !retain || e.err != nil {
			s.hashes.Delete(prefix)
			s.hashesCached.Add(-1)
		}
		close(e.ready)
	}()
	e.value, e.err = s.computeNamespaceHashes(prefix)
	return e.value, e.err
}

func (s *indexSnapshot) computeNamespaceHashes(prefix string) (*snapshotRowHashes, error) {
	defer runtime.KeepAlive(s)
	name, _ := namespaceHashesPrefix(prefix)
	start := snapshotNamespacePrefix(name)
	end := bytes.Clone(start)
	end[len(end)-1] = 1
	first, last, err := s.namespaceRange(name)
	if err != nil {
		return nil, err
	}
	h := &snapshotRowHashes{first: first, hashes: make([]uint64, last-first)}
	workers := namespaceHashesWorkers()
	ranges := []snapshotMetricRange{{start, end, int(first), int(last)}}
	if last-first >= namespaceHashesSplitMin && workers > 1 {
		if split := s.splitMetricRange(ranges[0], workers); len(split) > 1 {
			ranges = split
		}
	}
	tops := make([][]string, len(ranges))
	errs := make([]error, len(ranges))
	jobs := make(chan int, len(ranges))
	for i := range ranges {
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	for range min(workers, len(ranges)) {
		wg.Go(func() {
			for i := range jobs {
				tops[i], errs[i] = s.hashRowRange(h, len(start), ranges[i].start, ranges[i].end)
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	// A namespace split between ranges continues in the next range.
	for _, part := range tops {
		for _, top := range part {
			if n := len(h.tops); n == 0 || h.tops[n-1] != top {
				h.tops = append(h.tops, top)
			}
		}
	}
	return h, nil
}

func namespaceHashesWorkers() int {
	return max(1, min(8, runtime.GOMAXPROCS(0)/2))
}

// hashRowRange hashes snapshot keys "<prefix>\x00<namespace>\x00...wsp" as
// "<namespace>.<...>". Metrics directly below the prefix belong to no namespace.
func (s *indexSnapshot) hashRowRange(h *snapshotRowHashes, strip int, start, end []byte) ([]string, error) {
	it, err := s.index.Iterator(start, end)
	if it != nil {
		defer it.Close()
	}
	var tops []string
	buf := make([]byte, 0, 256)
	for err == nil {
		key, row := it.Current()
		rest := key[strip : len(key)-len(".wsp")]
		if i := bytes.IndexByte(rest, 0); i > 0 {
			if n := len(tops); n == 0 || tops[n-1] != string(rest[:i]) {
				tops = append(tops, string(rest[:i]))
			}
			buf = append(buf[:0], rest...)
			for j, c := range buf {
				if c == 0 {
					buf[j] = '.'
				}
			}
			h.hashes[row-h.first] = city.Hash64(buf)
		}
		err = it.Next()
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		err = nil
	}
	return tops, err
}

type namespaceRows struct {
	name       string
	start, end uint64
	children   [][2]uint64 // nested enabled namespaces, by start row
}

// namespaces assigns snapshot metrics to their longest enabled namespace, as
// the hash generator does: with "a" and "a.b" enabled, "a.b.x" belongs only to
// "a.b". Without include, every namespace directly below the prefix is used.
func (h *snapshotRowHashes) namespaces(s *indexSnapshot, prefix string, include map[string]bool) (*namespaceHashes, error) {
	defer runtime.KeepAlive(s)
	name, _ := namespaceHashesPrefix(prefix)
	if include == nil {
		include = make(map[string]bool, len(h.tops))
		for _, top := range h.tops {
			include[top] = true
		}
	}
	var rows []*namespaceRows
	for ns := range include {
		full := ns
		if name != "" {
			full = name + "." + ns
		}
		start, end, err := s.namespaceRange(full)
		if err != nil {
			return nil, err
		}
		if start < end {
			rows = append(rows, &namespaceRows{name: ns, start: start, end: end})
		}
	}
	// Ranges of distinct namespaces are nested or disjoint. A namespace holding
	// only a nested enabled namespace has the same range; the shorter name is
	// the ancestor.
	slices.SortFunc(rows, func(a, b *namespaceRows) int {
		return cmp.Or(cmp.Compare(a.start, b.start), cmp.Compare(b.end, a.end), cmp.Compare(len(a.name), len(b.name)))
	})
	var stack []*namespaceRows
	for _, r := range rows {
		for len(stack) > 0 && stack[len(stack)-1].end <= r.start {
			stack = stack[:len(stack)-1]
		}
		if n := len(stack); n > 0 {
			stack[n-1].children = append(stack[n-1].children, [2]uint64{r.start, r.end})
		}
		stack = append(stack, r)
	}
	segments := make([]namespaceHashSegment, len(rows))
	for i, r := range rows {
		segments[i].name = r.name
		next := r.start
		for _, c := range append(r.children, [2]uint64{r.end, r.end}) {
			segments[i].hashes = append(segments[i].hashes, h.hashes[next-h.first:c[0]-h.first]...)
			next = c[1]
		}
	}
	return newNamespaceHashes(segments, namespaceHashesWorkers()), nil
}

func newNamespaceHashes(segments []namespaceHashSegment, workers int) *namespaceHashes {
	slices.SortFunc(segments, func(a, b namespaceHashSegment) int { return strings.Compare(a.name, b.name) })
	h := &namespaceHashes{
		names:  make([]string, len(segments)),
		counts: make([]uint64, len(segments)),
		hashes: make([][]uint64, len(segments)),
	}
	jobs := make(chan int, len(segments))
	for i, seg := range segments {
		h.names[i], h.counts[i] = seg.name, uint64(len(seg.hashes))
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	for range max(1, min(workers, len(segments))) {
		wg.Go(func() {
			for i := range jobs {
				hashes := segments[i].hashes
				slices.Sort(hashes)
				hashes = slices.Compact(hashes)
				// Retain only the unique hashes for the snapshot's lifetime.
				if cap(hashes)-len(hashes) > len(hashes)/4 {
					hashes = slices.Clone(hashes)
				}
				h.hashes[i] = hashes
			}
		})
	}
	wg.Wait()
	return h
}

// namesNamespaceHashes hashes full metric names below prefix into their longest
// enabled namespace, or their first component without include.
func namesNamespaceHashes(names []string, prefix string, include map[string]bool) *namespaceHashes {
	name, strip := namespaceHashesPrefix(prefix)
	index := make(map[string]int)
	var segments []namespaceHashSegment
	for _, metric := range names {
		if strip > 0 && (len(metric) <= strip || metric[strip-1] != '.' || metric[:strip-1] != name) {
			continue
		}
		rest, match := metric[strip:], ""
		for i := 0; i < len(rest); i++ {
			if rest[i] != '.' {
				continue
			}
			if include == nil {
				match = rest[:i]
				break
			}
			if include[rest[:i]] {
				match = rest[:i]
			}
		}
		if match == "" {
			continue
		}
		j, ok := index[match]
		if !ok {
			j = len(segments)
			index[match] = j
			segments = append(segments, namespaceHashSegment{name: match})
		}
		segments[j].hashes = append(segments[j].hashes, city.Hash64([]byte(rest)))
	}
	return newNamespaceHashes(segments, 1)
}

// parseHashNamespaces reads the requested namespaces one per line, trimmed as
// the hash generator trims its enabled namespace list.
func parseHashNamespaces(body []byte) map[string]bool {
	namespaces := make(map[string]bool)
	for line := range strings.Lines(string(body)) {
		ns := strings.TrimLeftFunc(line, unicode.IsSpace)
		ns = strings.TrimRightFunc(ns, func(r rune) bool { return r == '.' || unicode.IsSpace(r) })
		if ns == "" {
			continue
		}
		namespaces[ns] = true
	}
	return namespaces
}

// writeNamespaceHashes merges the snapshot base with the mutable overlay.
// Overlay metrics are absent from the base, so metric counts add.
func writeNamespaceHashes(w io.Writer, base, overlay *namespaceHashes, include map[string]bool, timestamp int64) (namespaces int, err error) {
	bw := bufio.NewWriterSize(w, 1<<20)
	buf := make([]byte, 0, 1<<16)
	buf = append(buf, namespaceHashesMagic[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(timestamp))
	flush := func(force bool) {
		if err == nil && (force || len(buf) >= 1<<16-8) {
			_, err = bw.Write(buf)
			buf = buf[:0]
		}
	}
	i, j := 0, 0
	for err == nil && (i < len(base.names) || j < len(overlay.names)) {
		var name string
		var count uint64
		var a, b []uint64
		switch {
		case j >= len(overlay.names) || i < len(base.names) && base.names[i] < overlay.names[j]:
			name, count, a = base.names[i], base.counts[i], base.hashes[i]
			i++
		case i >= len(base.names) || overlay.names[j] < base.names[i]:
			name, count, b = overlay.names[j], overlay.counts[j], overlay.hashes[j]
			j++
		default:
			name, count, a, b = base.names[i], base.counts[i]+overlay.counts[j], base.hashes[i], overlay.hashes[j]
			i++
			j++
		}
		if count == 0 || include != nil && !include[name] {
			continue
		}
		namespaces++
		unique := uint64(0)
		mergeUniqueHashes(a, b, func(uint64) { unique++ })
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(name)))
		buf = append(buf, name...)
		buf = binary.LittleEndian.AppendUint64(buf, count)
		buf = binary.LittleEndian.AppendUint64(buf, unique)
		mergeUniqueHashes(a, b, func(h uint64) {
			buf = binary.LittleEndian.AppendUint64(buf, h)
			flush(false)
		})
		flush(false)
	}
	flush(true)
	if err == nil {
		err = bw.Flush()
	}
	return namespaces, err
}

func mergeUniqueHashes(a, b []uint64, emit func(uint64)) {
	var last uint64
	first := true
	for len(a) > 0 || len(b) > 0 {
		var h uint64
		if len(b) == 0 || len(a) > 0 && a[0] <= b[0] {
			h, a = a[0], a[1:]
		} else {
			h, b = b[0], b[1:]
		}
		if first || h != last {
			emit(h)
		}
		last, first = h, false
	}
}

// currentNamespaceHashes returns the hashes of the current index below prefix:
// snapshot metrics from the per-generation row hashes, merged with the mutable
// overlay hashed per request (every metric when there is no snapshot).
func (listener *CarbonserverListener) currentNamespaceHashes(prefix string, include map[string]bool) (base, overlay *namespaceHashes, err error) {
	fidx := listener.CurrentFileIndex()
	if fidx == nil {
		return nil, nil, errMetricsListEmpty
	}
	ti := fidx.trieIdx
	if !listener.trieIndex || ti == nil || ti.snapshot == nil {
		names, err := listener.getMetricsList()
		if err != nil {
			return nil, nil, err
		}
		return &namespaceHashes{}, namesNamespaceHashes(names, prefix, include), nil
	}
	rows, err := ti.snapshot.namespaceHashes(prefix)
	if err != nil {
		return nil, nil, err
	}
	// Remember only retained prefixes, so arbitrary request prefixes cannot
	// grow the prewarm set beyond the per-snapshot cache limit.
	if _, cached := ti.snapshot.hashes.Load(prefix); cached {
		listener.namespaceHashPrefixes.Store(prefix, include)
	}
	if base, err = rows.cachedNamespaces(ti.snapshot, prefix, include); err != nil {
		return nil, nil, err
	}
	name, _ := namespaceHashesPrefix(prefix)
	var names []string
	if dir := ti.mutableDirectory(name); dir != nil {
		names, _, _, _, _ = ti.allMetricsNodeMutable(dir, '.', name, int(^uint(0)>>1), false)
	}
	if include == nil {
		include = make(map[string]bool, len(rows.tops))
		for _, top := range rows.tops {
			include[top] = true
		}
		// New first-level namespaces exist only in the overlay.
		for _, n := range namesNamespaceHashes(names, prefix, nil).names {
			include[n] = true
		}
	}
	return base, namesNamespaceHashes(names, prefix, include), nil
}

// prewarmNamespaceHashes computes hashes for previously requested prefixes, and
// their last enabled list, as soon as a new snapshot generation is published.
func (listener *CarbonserverListener) prewarmNamespaceHashes(s *indexSnapshot) {
	listener.namespaceHashPrefixes.Range(func(key, value any) bool {
		prefix, include := key.(string), value.(map[string]bool)
		go func() {
			t0 := time.Now()
			rows, err := s.namespaceHashes(prefix)
			if err == nil {
				_, err = rows.cachedNamespaces(s, prefix, include)
			}
			if err != nil {
				listener.logger.Warn("namespace hashes prewarm failed", zap.String("prefix", prefix), zap.Error(err))
				return
			}
			listener.logger.Info("namespace hashes prewarmed", zap.String("prefix", prefix), zap.Duration("runtime_seconds", time.Since(t0)))
		}()
		return true
	})
}

func (listener *CarbonserverListener) namespaceHashesHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/namespace-hashes/?prefix=aggregations.secondly.
	// An optional POST body restricts the response to one namespace per line.
	t0 := time.Now()
	atomic.AddUint64(&listener.metrics.NamespaceHashesRequests, 1)
	prefix := req.URL.Query().Get("prefix")
	accessLogger := TraceContextToZap(req.Context(), listener.accessLogger.With(
		zap.String("handler", "namespace-hashes"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
	))
	fail := func(code int, reason string, err error) {
		accessLogger.Error("namespace hashes failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", reason),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("%s: %v", reason, err), code)
	}

	var include map[string]bool
	if req.Method == http.MethodPost {
		body, err := io.ReadAll(http.MaxBytesReader(wr, req.Body, maxNamespaceHashesRequestBody))
		include = parseHashNamespaces(body)
		if err != nil {
			fail(http.StatusBadRequest, "invalid namespace list", err)
			return
		}
	} else if req.Method != http.MethodGet {
		fail(http.StatusMethodNotAllowed, "unsupported method", errors.New(req.Method))
		return
	}

	base, overlay, err := listener.currentNamespaceHashes(prefix, include)
	if err != nil {
		fail(http.StatusInternalServerError, "can't compute namespace hashes", err)
		return
	}
	acquired := time.Since(t0)
	wr.Header().Set("Content-Type", "application/octet-stream")
	namespaces, err := writeNamespaceHashes(wr, base, overlay, nil, time.Now().Unix())
	accessLogger.Info("namespace hashes served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Duration("acquire_seconds", acquired),
		zap.Int("namespaces", namespaces),
		zap.Error(err),
	)
}
