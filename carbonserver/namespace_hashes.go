package carbonserver

import (
	"bufio"
	"bytes"
	"encoding/binary"
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

var errDottedNamespace = errors.New("namespaces must not contain '.'")

type namespaceHashes struct {
	names  []string
	counts []uint64
	hashes [][]uint64 // sorted and unique
}

type namespaceHashesEntry struct {
	ready chan struct{}
	value *namespaceHashes
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

// namespaceHashes returns the hashes of every snapshot metric below prefix,
// computing them once per snapshot generation. Concurrent callers wait for the
// same computation.
func (s *indexSnapshot) namespaceHashes(prefix string) (*namespaceHashes, error) {
	if v, ok := s.hashes.Load(prefix); ok {
		e := v.(*namespaceHashesEntry)
		<-e.ready
		return e.value, e.err
	}
	if s.hashesCached.Add(1) > maxCachedNamespaceHashPrefixes {
		s.hashesCached.Add(-1)
		return s.computeNamespaceHashes(prefix)
	}
	e := &namespaceHashesEntry{ready: make(chan struct{})}
	if v, loaded := s.hashes.LoadOrStore(prefix, e); loaded {
		s.hashesCached.Add(-1)
		e = v.(*namespaceHashesEntry)
		<-e.ready
		return e.value, e.err
	}
	// Waiters must be released even if the computation panics.
	defer func() {
		if r := recover(); r != nil {
			e.value, e.err = nil, fmt.Errorf("namespace hashes: %v", r)
		}
		if e.err != nil {
			s.hashes.Delete(prefix)
			s.hashesCached.Add(-1)
		}
		close(e.ready)
	}()
	e.value, e.err = s.computeNamespaceHashes(prefix)
	return e.value, e.err
}

func (s *indexSnapshot) computeNamespaceHashes(prefix string) (*namespaceHashes, error) {
	defer runtime.KeepAlive(s)
	name, _ := namespaceHashesPrefix(prefix)
	start := snapshotNamespacePrefix(name)
	end := bytes.Clone(start)
	end[len(end)-1] = 1
	first, last, err := s.namespaceRange(name)
	if err != nil {
		return nil, err
	}
	workers := max(1, min(8, runtime.GOMAXPROCS(0)/2))
	ranges := []snapshotMetricRange{{start, end, int(first), int(last)}}
	if last-first >= namespaceHashesSplitMin && workers > 1 {
		if split := s.splitMetricRange(ranges[0], workers); len(split) > 1 {
			ranges = split
		}
	}
	parts := make([][]namespaceHashSegment, len(ranges))
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
				parts[i], errs[i] = s.hashNamespaceRange(len(start), ranges[i].start, ranges[i].end)
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	// Keys are ordered by namespace, so a namespace split between ranges
	// continues in the first segment of the next range.
	var segments []namespaceHashSegment
	for _, part := range parts {
		for _, seg := range part {
			if n := len(segments); n > 0 && segments[n-1].name == seg.name {
				segments[n-1].hashes = append(segments[n-1].hashes, seg.hashes...)
			} else {
				segments = append(segments, seg)
			}
		}
	}
	return newNamespaceHashes(segments, workers), nil
}

// hashNamespaceRange hashes snapshot keys "<prefix>\x00<namespace>\x00...wsp"
// as "<namespace>.<...>". Metrics directly below the prefix belong to no namespace.
func (s *indexSnapshot) hashNamespaceRange(strip int, start, end []byte) ([]namespaceHashSegment, error) {
	it, err := s.index.Iterator(start, end)
	if it != nil {
		defer it.Close()
	}
	var segments []namespaceHashSegment
	buf := make([]byte, 0, 256)
	for err == nil {
		key, _ := it.Current()
		rest := key[strip : len(key)-len(".wsp")]
		if i := bytes.IndexByte(rest, 0); i > 0 {
			if n := len(segments); n == 0 || segments[n-1].name != string(rest[:i]) {
				segments = append(segments, namespaceHashSegment{name: string(rest[:i])})
			}
			buf = append(buf[:0], rest...)
			for j, c := range buf {
				if c == 0 {
					buf[j] = '.'
				}
			}
			seg := &segments[len(segments)-1]
			seg.hashes = append(seg.hashes, city.Hash64(buf))
		}
		err = it.Next()
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		err = nil
	}
	return segments, err
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

// namesNamespaceHashes groups and hashes full metric names below prefix.
func namesNamespaceHashes(names []string, prefix string) *namespaceHashes {
	name, strip := namespaceHashesPrefix(prefix)
	index := make(map[string]int)
	var segments []namespaceHashSegment
	for _, metric := range names {
		if strip > 0 && (len(metric) <= strip || metric[strip-1] != '.' || metric[:strip-1] != name) {
			continue
		}
		rest := metric[strip:]
		i := strings.IndexByte(rest, '.')
		if i <= 0 {
			continue
		}
		j, ok := index[rest[:i]]
		if !ok {
			j = len(segments)
			index[rest[:i]] = j
			segments = append(segments, namespaceHashSegment{name: rest[:i]})
		}
		segments[j].hashes = append(segments[j].hashes, city.Hash64([]byte(rest)))
	}
	return newNamespaceHashes(segments, 1)
}

// parseHashNamespaces reads the requested namespaces one per line, trimmed as
// the hash generator trims its enabled namespace list.
func parseHashNamespaces(body []byte) (map[string]bool, error) {
	namespaces := make(map[string]bool)
	for line := range strings.Lines(string(body)) {
		ns := strings.TrimLeftFunc(line, unicode.IsSpace)
		ns = strings.TrimRightFunc(ns, func(r rune) bool { return r == '.' || unicode.IsSpace(r) })
		if ns == "" {
			continue
		}
		if strings.IndexByte(ns, '.') >= 0 {
			return nil, fmt.Errorf("%w: %q", errDottedNamespace, ns)
		}
		namespaces[ns] = true
	}
	return namespaces, nil
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

func (listener *CarbonserverListener) currentNamespaceHashes(prefix string) (base, overlay *namespaceHashes, err error) {
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
		return namesNamespaceHashes(names, prefix), &namespaceHashes{}, nil
	}
	base, err = ti.snapshot.namespaceHashes(prefix)
	if err != nil {
		return nil, nil, err
	}
	// Remember only retained prefixes, so arbitrary request prefixes cannot
	// grow the prewarm set beyond the per-snapshot cache limit.
	if _, cached := ti.snapshot.hashes.Load(prefix); cached {
		listener.namespaceHashPrefixes.Store(prefix, struct{}{})
	}
	name, _ := namespaceHashesPrefix(prefix)
	var names []string
	if dir := ti.mutableDirectory(name); dir != nil {
		names, _, _, _, _ = ti.allMetricsNodeMutable(dir, '.', name, int(^uint(0)>>1), false)
	}
	return base, namesNamespaceHashes(names, prefix), nil
}

// prewarmNamespaceHashes computes hashes for previously requested prefixes as
// soon as a new snapshot generation is published.
func (listener *CarbonserverListener) prewarmNamespaceHashes(s *indexSnapshot) {
	listener.namespaceHashPrefixes.Range(func(key, _ any) bool {
		prefix := key.(string)
		go func() {
			t0 := time.Now()
			if _, err := s.namespaceHashes(prefix); err != nil {
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
		if err == nil {
			include, err = parseHashNamespaces(body)
		}
		if err != nil {
			fail(http.StatusBadRequest, "invalid namespace list", err)
			return
		}
	} else if req.Method != http.MethodGet {
		fail(http.StatusMethodNotAllowed, "unsupported method", errors.New(req.Method))
		return
	}

	base, overlay, err := listener.currentNamespaceHashes(prefix)
	if err != nil {
		fail(http.StatusInternalServerError, "can't compute namespace hashes", err)
		return
	}
	acquired := time.Since(t0)
	wr.Header().Set("Content-Type", "application/octet-stream")
	namespaces, err := writeNamespaceHashes(wr, base, overlay, include, time.Now().Unix())
	accessLogger.Info("namespace hashes served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Duration("acquire_seconds", acquired),
		zap.Int("namespaces", namespaces),
		zap.Error(err),
	)
}
