package carbonserver

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/vellum"
	"go.uber.org/zap"
)

// The mutable trie contains only metrics absent from the immutable generation.
// Its sole writer is the existing index updater; queries retain the generation
// they started with until completion.
func (ti *trieIndex) insert(path string, logical, physical, points, firstSeen int64) (*trieNode, error) {
	if ti.snapshot != nil && strings.HasSuffix(path, ti.fileExt) {
		path = "/" + strings.TrimPrefix(filepath.Clean(path), "/")
		entry, found, err := ti.snapshot.lookup(path)
		if err != nil {
			return nil, err
		}
		if found {
			name := strings.ReplaceAll(strings.TrimSuffix(path[1:], ti.fileExt), "/", ".")
			key, _ := encodeSnapshotPath(nil, entry.Path)
			row, _, err := ti.snapshot.index.Get(key)
			if err != nil {
				return nil, err
			}
			defer runtime.KeepAlive(ti.snapshot)
			return ti.snapshot.fileNode(name, row)
		}
	}
	return ti.insertMutable(path, logical, physical, points, firstSeen)
}

func (ti *trieIndex) metricPath(metric string, dirs []*trieNode) ([]*trieNode, bool) {
	if ti.snapshot == nil {
		return ti.metricPathMutable(metric, dirs)
	}
	if found, err := ti.snapshot.containsMetric(metric, ti.fileExt); err == nil && found {
		return dirs, false
	}
	if _, isNew := ti.metricPathMutable(metric, nil); !isNew {
		return dirs, false
	}
	if dirs == nil {
		return nil, true
	}
	defer runtime.KeepAlive(ti.snapshot)
	dirs = append(dirs, ti.root)
	// Walk shared prefixes with the scanner cursor. Vellum's Accept allocates
	// a decoded state per byte; quota admission needs only prefix existence.
	cursor := ti.snapshot.keys
	root := cursor.accept(cursor.start(), 0)
	state := root
	for end := 0; end < len(metric); end++ {
		c := metric[end]
		if c == '.' {
			c = 0
		}
		if state.ok {
			state = cursor.accept(state, c)
		}
		if metric[end] != '.' {
			continue
		}
		name := metric[:end]
		exists := state.ok
		if name == "" || name == "/" {
			exists = root.ok
		}
		if !exists {
			exists = ti.mutableDirectory(name) != nil
		}
		if !exists {
			break
		}
		dirs = append(dirs, ti.snapshot.directoryNode(name))
	}
	return dirs, true
}

type snapshotResult struct {
	name string
	leaf bool
	node *trieNode
}

func (ti *trieIndex) query(expr string, limit int, expand func([]string) ([]string, error)) ([]string, []bool, []*trieNode, uint32, error) {
	if ti.snapshot == nil {
		return ti.queryMutable(expr, limit, expand)
	}
	names, leaves, nodes, lookups, err := ti.snapshot.query(expr, limit, expand)
	if err != nil {
		return nil, nil, nil, lookups, err
	}
	extra, el, en, its, err := ti.queryMutable(expr, limit, expand)
	lookups += its
	if err != nil {
		return nil, nil, nil, lookups, err
	}
	if snapshotResultsOrdered(names, leaves) && snapshotResultsOrdered(extra, el) {
		// Each source is already in result order: return the snapshot rows
		// directly, or merge, instead of sorting a copy of every match.
		if len(extra) == 0 {
			n := len(names)
			for i := 1; i < len(names); i++ {
				if i >= limit && names[i] != names[i-1] {
					n = i
					break
				}
			}
			return names[:n], leaves[:n], nodes[:n], lookups, nil
		}
		if rows, ok := ti.mergeSnapshotResults(names, leaves, nodes, extra, el, en); ok {
			names, leaves, nodes = collectSnapshotResults(rows, limit)
			return names, leaves, nodes, lookups, nil
		}
	}
	rows := make([]snapshotResult, 0, len(names)+len(extra))
	for i, name := range names {
		rows = append(rows, snapshotResult{name, leaves[i], nodes[i]})
	}
	for i, name := range extra {
		node := en[i]
		if !el[i] {
			node = ti.snapshot.directoryNode(name)
		}
		rows = append(rows, snapshotResult{name, el[i], node})
	}
	sort.Slice(rows, func(i, j int) bool {
		return snapshotResultLess(rows[i].name, rows[i].leaf, rows[j].name, rows[j].leaf)
	})
	names, leaves, nodes = collectSnapshotResults(rows, limit)
	return names, leaves, nodes, lookups, nil
}

// snapshotResultLess orders query results by name, a metric before the
// namespace of the same name.
func snapshotResultLess(a string, aLeaf bool, b string, bLeaf bool) bool {
	if a == b {
		return aLeaf && !bLeaf
	}
	return a < b
}

// snapshotResultsOrdered reports whether rows are strictly increasing. Sorting
// such rows is the identity, so callers may skip it; equal rows are rejected
// because which duplicate the sort keeps is unspecified.
func snapshotResultsOrdered(names []string, leaves []bool) bool {
	for i := 1; i < len(names); i++ {
		if !snapshotResultLess(names[i-1], leaves[i-1], names[i], leaves[i]) {
			return false
		}
	}
	return true
}

// mergeSnapshotResults merges two strictly ordered sources. A row present in
// both is kept only if both nodes are the same, which makes the sort's choice
// irrelevant; otherwise it reports false and the caller sorts.
func (ti *trieIndex) mergeSnapshotResults(names []string, leaves []bool, nodes []*trieNode, extra []string, el []bool, en []*trieNode) ([]snapshotResult, bool) {
	rows := make([]snapshotResult, 0, len(names)+len(extra))
	i, j := 0, 0
	for i < len(names) || j < len(extra) {
		if j == len(extra) || (i < len(names) && snapshotResultLess(names[i], leaves[i], extra[j], el[j])) {
			rows = append(rows, snapshotResult{names[i], leaves[i], nodes[i]})
			i++
			continue
		}
		node := en[j]
		if !el[j] {
			node = ti.snapshot.directoryNode(extra[j])
		}
		if i < len(names) && names[i] == extra[j] && leaves[i] == el[j] {
			if nodes[i] != node {
				return nil, false
			}
			i++
		}
		rows = append(rows, snapshotResult{extra[j], el[j], node})
		j++
	}
	return rows, true
}

// collectSnapshotResults drops duplicate rows of sorted results and applies
// the limit.
func collectSnapshotResults(rows []snapshotResult, limit int) (names []string, leaves []bool, nodes []*trieNode) {
	for i, row := range rows {
		if i > 0 && row.name == rows[i-1].name && row.leaf == rows[i-1].leaf {
			continue
		}
		// Preserve the trie's same-name leaf/directory pair at the limit.
		if len(names) > 0 && len(names) >= limit && row.name != names[len(names)-1] {
			break
		}
		names = append(names, row.name)
		leaves = append(leaves, row.leaf)
		nodes = append(nodes, row.node)
	}
	return names, leaves, nodes
}

func (s *indexSnapshot) walkNamespace(name string, visit func(string, uint64) bool) error {
	defer runtime.KeepAlive(s)
	prefix := snapshotNamespacePrefix(name)
	end := bytes.Clone(prefix)
	end[len(end)-1] = 1
	it, err := s.index.Iterator(prefix, end)
	if it != nil {
		defer it.Close()
	}
	for err == nil {
		key, row := it.Current()
		metric := strings.ReplaceAll(string(key[1:len(key)-4]), "\x00", ".")
		if !visit(metric, row) {
			return nil
		}
		err = it.Next()
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		return nil
	}
	return err
}

// appendMetricNamesRange owns decoded names in chunks for the lifetime of the list.
// Unlike namespace callbacks, a full list retains every name together, so it
// can share append-only storage without pinning chunks for individual lookups.
func (s *indexSnapshot) appendMetricNamesRange(files []string, sep byte, startKey, endKey []byte) []string {
	defer runtime.KeepAlive(s)
	it, err := s.index.Iterator(startKey, endKey)
	if it != nil {
		defer it.Close()
	}
	var chunk strings.Builder
	for err == nil {
		key, _ := it.Current()
		name := key[1 : len(key)-4]
		if chunk.Cap()-chunk.Len() < len(name) {
			chunk = strings.Builder{}
			// Small partitions should not each retain a mostly empty 1MiB
			// chunk. Estimate from the remaining names, with bounded growth.
			size := min(cap(files)-len(files), (1<<20)/max(len(name), 1)) * len(name)
			chunk.Grow(max(4<<10, len(name), size))
		}
		start := chunk.Len()
		for {
			i := bytes.IndexByte(name, 0)
			if i < 0 {
				_, _ = chunk.Write(name)
				break
			}
			_, _ = chunk.Write(name[:i])
			_ = chunk.WriteByte('.')
			name = name[i+1:]
		}
		metric := chunk.String()[start:]
		if sep != '.' {
			metric = strings.ReplaceAll(metric, ".", string(sep))
		}
		files = append(files, metric)
		err = it.Next()
	}
	return files
}

type snapshotMetricRange struct {
	start, end  []byte
	first, last int
}

// Snapshot values are consecutive row numbers in encoded-key order, as used by
// namespaceRange. Probe prefix boundaries to split skewed namespaces without a
// preliminary traversal of every metric or changes to the saved index format.
func (s *indexSnapshot) metricListRanges(workers int) []snapshotMetricRange {
	return s.splitMetricRange(snapshotMetricRange{[]byte{0}, []byte{1}, 0, s.index.Len()}, workers)
}

func (s *indexSnapshot) splitMetricRange(whole snapshotMetricRange, workers int) []snapshotMetricRange {
	return s.splitMetricRangeInto(whole, workers*2, 256)
}

// splitMetricRangeInto splits until every range holds at most 1/parts of the
// rows, or there are maxRanges ranges.
func (s *indexSnapshot) splitMetricRangeInto(whole snapshotMetricRange, parts, maxRanges int) []snapshotMetricRange {
	defer runtime.KeepAlive(s)
	ranges := []snapshotMetricRange{whole}
	limit := max((whole.last-whole.first)/parts, 1)
	for len(ranges) < maxRanges {
		largest := 0
		for i := range ranges {
			if ranges[i].last-ranges[i].first > ranges[largest].last-ranges[largest].first {
				largest = i
			}
		}
		r := ranges[largest]
		if r.last-r.first <= limit {
			break
		}
		state := s.index.Start()
		for _, c := range r.start {
			state = s.index.Accept(state, c)
		}
		var children []snapshotMetricRange
		if s.index.IsMatch(state) {
			children = append(children, snapshotMetricRange{r.start, nil, r.first, 0})
		}
		for c := 0; c < 256; c++ {
			if !s.index.CanMatch(s.index.Accept(state, byte(c))) {
				continue
			}
			prefix := append(bytes.Clone(r.start), byte(c))
			it, err := s.index.Iterator(prefix, r.end)
			if err != nil {
				if it != nil {
					_ = it.Close()
				}
				return nil
			}
			_, row := it.Current()
			_ = it.Close()
			if row < uint64(r.first) || row >= uint64(r.last) ||
				len(children) > 0 && row <= uint64(children[len(children)-1].first) {
				return nil
			}
			if len(children) > 0 {
				children[len(children)-1].end = prefix
				children[len(children)-1].last = int(row)
			}
			children = append(children, snapshotMetricRange{prefix, nil, int(row), 0})
		}
		if len(children) == 0 || children[0].first != r.first {
			return nil
		}
		children[len(children)-1].end = r.end
		children[len(children)-1].last = r.last
		ranges = append(ranges[:largest], append(children, ranges[largest+1:]...)...)
	}
	return ranges
}

func (s *indexSnapshot) appendMetricNamesParallel(files []string, sep byte, workers int) []string {
	defer runtime.KeepAlive(s)
	ranges := s.metricListRanges(workers)
	if len(ranges) < 2 {
		return s.appendMetricNamesRange(files, sep, []byte{0}, []byte{1})
	}
	base := len(files)
	files = files[:base+s.index.Len()]
	jobs := make(chan snapshotMetricRange, len(ranges))
	for _, r := range ranges {
		jobs <- r
	}
	close(jobs)
	var wg sync.WaitGroup
	var incomplete atomic.Bool
	for range workers {
		wg.Go(func() {
			for r := range jobs {
				part := files[base+r.first : base+r.last : base+r.last]
				if len(s.appendMetricNamesRange(part[:0], sep, r.start, r.end)) != len(part) {
					incomplete.Store(true)
				}
			}
		})
	}
	wg.Wait()
	if incomplete.Load() {
		return s.appendMetricNamesRange(files[:base], sep, []byte{0}, []byte{1})
	}
	return files
}

func (s *indexSnapshot) appendMetricNames(files []string, sep byte) []string {
	workers := min(4, runtime.GOMAXPROCS(0))
	if s.index.Len() < 1_000_000 || workers == 1 {
		return s.appendMetricNamesRange(files, sep, []byte{0}, []byte{1})
	}
	return s.appendMetricNamesParallel(files, sep, workers)
}

func (ti *trieIndex) allMetrics(sep byte) []string {
	extra := ti.allMetricsMutable(sep)
	if ti.snapshot == nil {
		return extra
	}
	// Keep the largely ordered snapshot separate from the sorted mutable trie.
	// Prepending the overlay defeats the sort's nearly sorted input fast path
	// and repeatedly grows a slice containing every metric in a large snapshot.
	files := make([]string, 0, ti.snapshot.index.Len()+len(extra))
	files = ti.snapshot.appendMetricNames(files, sep)
	// Encoded path order differs from metric order (NUL separators and .wsp),
	// so the decoded snapshot still needs sorting before the linear merge.
	sort.Strings(files)
	base := len(files)
	files = files[:base+len(extra)]
	// Merge backwards into the reserved space without overwriting unread names.
	for i, j, k := base-1, len(extra)-1, len(files)-1; j >= 0; k-- {
		if i >= 0 && files[i] > extra[j] {
			files[k] = files[i]
			i--
		} else {
			files[k] = extra[j]
			j--
		}
	}
	return files
}

func (ti *trieIndex) allMetricsNode(node *trieNode, sep byte, prefix string, limit int, statsOnly bool) (files []string, nodes []*trieNode, count int, physical, logical int64) {
	if ti.snapshot == nil {
		return ti.allMetricsNodeMutable(node, sep, prefix, limit, statsOnly)
	}
	name := strings.ReplaceAll(prefix, string(sep), ".")
	// Mutable directories with the same path are independent from snapshot nodes.
	if n := ti.mutableDirectory(name); n != nil {
		files, nodes, count, physical, logical = ti.allMetricsNodeMutable(n, sep, prefix, limit, statsOnly)
	}
	if statsOnly && limit > 0 {
		start, end, err := ti.snapshot.namespaceRange(name)
		if err == nil {
			sums, err := ti.snapshot.metadata.usage(start, end)
			if err == nil {
				count += int(end - start)
				logical += sums[0]
				physical += sums[1]
			}
		}
		runtime.KeepAlive(ti.snapshot)
		return
	}
	if len(files) > 0 && len(files) >= limit {
		return
	}
	_ = ti.snapshot.walkNamespace(name, func(metric string, row uint64) bool {
		n, err := ti.snapshot.fileNode(metric, row)
		if err != nil {
			return false
		}
		m := n.meta.Load().(*fileMeta)
		count++
		physical += m.physicalSize
		logical += m.logicalSize
		if !statsOnly {
			if sep != '.' {
				metric = strings.ReplaceAll(metric, ".", string(sep))
			}
			files = append(files, metric)
			nodes = append(nodes, n)
		}
		return len(files) < limit
	})
	return
}

func metricNamespaces(name string, visit func(string)) {
	visit("/")
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			visit(name[:i])
		}
	}
}

// Overlay usage is computed only from new metrics. A namespace contributes to
// its parent's Namespaces quota exactly once, and only if absent from the base.
func (ti *trieIndex) overlayUsage() (map[string]QuotaUsage, map[string][2]int64, int) {
	usage := make(map[string]QuotaUsage, len(ti.quotaNodes)+1)
	reads := make(map[string][2]int64, len(ti.quotaNodes)+1)
	path := make([]byte, 0, 256)
	extraDirs := 0
	fst := ti.snapshot.index
	defer runtime.KeepAlive(ti.snapshot)

	// Aggregate each subtree once. Carry the immutable automaton state along
	// the same edges instead of looking up every ancestor of every metric.
	// Only root and configured quotas consume totals; do not materialize names
	// or maps for the millions of other metrics and namespaces.
	var visit func(*trieNode, int) (QuotaUsage, [2]int64)
	visit = func(node *trieNode, state int) (QuotaUsage, [2]int64) {
		if node != ti.root && node.file() {
			m := node.meta.Load().(*fileMeta)
			return QuotaUsage{Metrics: 1, LogicalSize: m.logicalSize, PhysicalSize: m.physicalSize, DataPoints: m.dataPoints},
				[2]int64{atomic.SwapInt64(&m.readHits, 0), atomic.SwapInt64(&m.readBytes, 0)}
		}
		length := len(path)
		isDir := node.dir()
		if isDir {
			if fst.CanMatch(state) {
				state = fst.Accept(state, 0)
			}
			path = append(path, '.')
		} else {
			path = append(path, node.c...)
			for _, c := range node.c {
				if !fst.CanMatch(state) {
					break
				}
				state = fst.Accept(state, c)
			}
		}
		var total QuotaUsage
		var read [2]int64
		children := node.getChildrens()
		for i := range children {
			u, r := visit(node.getChild(children, i), state)
			total.Metrics += u.Metrics
			total.Namespaces += u.Namespaces
			total.LogicalSize += u.LogicalSize
			total.PhysicalSize += u.PhysicalSize
			total.DataPoints += u.DataPoints
			read[0] += r[0]
			read[1] += r[1]
		}
		if node == ti.root {
			usage["/"], reads["/"] = total, read
		} else if isDir {
			if _, configured := ti.quotaNodes[string(path[:length])]; configured {
				name := string(path[:length])
				usage[name], reads[name] = total, read
			}
			// A namespace contributes one immediate child to its parent only
			// when it contains metrics and is absent from the immutable base.
			total.Namespaces = 0
			if total.Metrics != 0 && !fst.CanMatch(state) {
				total.Namespaces = 1
				extraDirs++
			}
		}
		path = path[:length]
		return total, read
	}
	visit(ti.root, fst.Accept(fst.Start(), 0))
	return usage, reads, extraDirs
}

// snapshotQuotaUsage counts only the affected mutable subtree and reads saved
// totals from the packed prefix sums. It leaves throughput/read counters intact.
func (ti *trieIndex) snapshotQuotaUsage(name string, exists func(string) bool) (QuotaUsage, error) {
	usage, err := ti.snapshot.namespaceUsage(name)
	if err != nil {
		return usage, err
	}
	node := ti.mutableDirectory(name)
	if node == nil {
		return usage, nil
	}
	extra := quotaStorageUsage(node)
	usage.Metrics += extra.Metrics
	usage.LogicalSize += extra.LogicalSize
	usage.PhysicalSize += extra.PhysicalSize
	usage.DataPoints += extra.DataPoints
	prefix := name + "."
	if name == "/" || name == "" {
		prefix = ""
	}
	var countDirectories func(*trieNode, string)
	countDirectories = func(parent *trieNode, path string) {
		for _, child := range *parent.childrens {
			if child.file() {
				continue
			}
			if child.dir() {
				if !exists(path) {
					usage.Namespaces++
				}
				continue
			}
			countDirectories(child, path+string(child.c))
		}
	}
	countDirectories(node, prefix)
	return usage, nil
}

func (ti *trieIndex) refreshUsage(throughputs *throughputQuotaManager) uint64 {
	if ti.snapshot == nil {
		return ti.refreshUsageMutable(throughputs)
	}
	extra, reads, _ := ti.overlayUsage()
	ti.snapshot.nodes.files.Range(func(key, value any) bool {
		m := value.(*trieNode).meta.Load().(*fileMeta)
		hits := atomic.SwapInt64(&m.readHits, 0)
		readBytes := atomic.SwapInt64(&m.readBytes, 0)
		if hits != 0 || readBytes != 0 {
			metricNamespaces(key.(string), func(prefix string) { r := reads[prefix]; r[0] += hits; r[1] += readBytes; reads[prefix] = r })
		}
		return true
	})
	ti.qauMetrics = ti.qauMetrics[:0]
	refresh := func(name string, node *trieNode) {
		base, err := ti.snapshot.namespaceUsage(name)
		if err != nil {
			ti.logger.Error("snapshot quota usage failed")
			return
		}
		delta := extra[name]
		u := node.meta.Load().(*dirMeta).usage
		atomic.StoreInt64(&u.Metrics, base.Metrics+delta.Metrics)
		atomic.StoreInt64(&u.Namespaces, base.Namespaces+delta.Namespaces)
		atomic.StoreInt64(&u.LogicalSize, base.LogicalSize+delta.LogicalSize)
		atomic.StoreInt64(&u.PhysicalSize, base.PhysicalSize+delta.PhysicalSize)
		atomic.StoreInt64(&u.DataPoints, base.DataPoints+delta.DataPoints)
		var throughput int64
		if throughputs != nil {
			if te := throughputs.load(name); te != nil {
				throughput = te.offset().total()
			}
		}
		throttled := atomic.SwapInt64(&u.Throttled, 0)
		metric := name
		if name == "/" {
			metric = "root"
		}
		r := reads[name]
		ti.generateTrieMetrics(ti.metricName(node, metric), node, throughput, throttled, r[0], r[1])
	}
	refresh("/", ti.root)
	for name := range ti.quotaNodes {
		if name != "/" {
			refresh(name, ti.snapshot.directoryNode(name))
		}
	}
	return ti.snapshot.manifest.Records + uint64(ti.getFileCount())
}

// Keep the existing trie diagnostics return shape.
//
//nolint:unparam
func (ti *trieIndex) countNodes() (count, files, dirs, onec, onefc, onedc int, byChildren, byGen *trieCounter) {
	count, files, dirs, onec, onefc, onedc, byChildren, byGen = ti.countNodesMutable()
	if ti.snapshot != nil {
		// Node counters describe the resident mutable trie. File counts describe the
		// complete logical index, including the immutable generation.
		files += int(ti.snapshot.manifest.Records)
	}
	return
}

func (ti *trieIndex) getQuotaTree(w io.Writer) {
	if ti.snapshot == nil {
		ti.getQuotaTreeMutable(w)
		return
	}
	fmt.Fprintf(w, "snapshot: %d metrics; mutable trie: %d metrics\n", ti.snapshot.manifest.Records, ti.getFileCount())
	names := make([]string, 0, len(ti.quotaNodes))
	for name := range ti.quotaNodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		meta := ti.quotaNodes[name]
		fmt.Fprintf(w, "%s (quota:%s usage:%s)\n", name, meta.quota.Load(), meta.usage)
	}
}

// A scan replaces a mapped generation only when every file and both accelerator
// files were successfully published. The live overlay receives notifications
// throughout that scan. Copy it before the swap, excluding metrics now in the
// snapshot, and preserve the throughput window across generations.
func (u *fileListUpdate) replaceSnapshot() {
	snapshot, err := openIndexSnapshot(u.listener.fileListCache, u.listener.whisperData)
	if err != nil {
		u.logger.Warn("failed to load replacement index snapshot", zap.Error(err))
		return
	}
	old := u.trieIdx
	next := newTrie(".wsp", u.listener.maxCreatesPerSecond, u.listener.estimateSize)
	next.snapshot, next.throughputs = snapshot, old.throughputs
	pending := u.snapshotNotifications
	if pending == nil {
		pending = make(map[string]*trieNode)
	}
	if old.snapshot != nil {
		names, nodes, _, _, _ := old.allMetricsNodeMutable(old.root, '.', "", int(^uint(0)>>1), false)
		for i, name := range names {
			pending[name] = nodes[i]
		}
	}
	// On the first conversion, the generation-pruned trie contains exactly the
	// scanned files, notifications processed during that scan, and remaining
	// cache-scan names. The first group is already in the snapshot: avoid a
	// second traversal of every old heap node just to discover the small delta.
	for path := range u.cacheMetricNames {
		if strings.HasSuffix(path, ".wsp") {
			name := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(path, "/"), ".wsp"), "/", ".")
			if _, ok := pending[name]; !ok {
				pending[name] = nil
			}
		}
	}
	for name, node := range pending {
		if err := next.copyOverlayMetric(name, node); err != nil {
			_ = snapshot.close()
			u.logger.Warn("failed to transfer snapshot overlay", zap.Error(err))
			return
		}
	}
	u.listener.drainRealtimeMetrics(next)
	u.trieIdx = next
	u.listener.prewarmNamespaceHashes(snapshot)
	u.metricsKnown = snapshot.manifest.Records + uint64(next.getFileCount())
}

func (ti *trieIndex) copyOverlayMetric(name string, node *trieNode) error {
	path := "/" + strings.ReplaceAll(name, ".", "/") + ti.fileExt
	found, err := ti.snapshot.containsMetric(name, ti.fileExt)
	if err != nil || found {
		return err
	}
	var m fileMeta
	if node != nil {
		source := node.meta.Load().(*fileMeta)
		m.logicalSize = atomic.LoadInt64(&source.logicalSize)
		m.physicalSize = atomic.LoadInt64(&source.physicalSize)
		m.dataPoints = atomic.LoadInt64(&source.dataPoints)
		m.firstSeenAt = atomic.LoadInt64(&source.firstSeenAt)
	}
	_, err = ti.insertMutable(path, m.logicalSize, m.physicalSize, m.dataPoints, m.firstSeenAt)
	return err
}

// Follow literal metric bytes; namespace names returned by a query may themselves
// contain glob metacharacters and must not be recompiled as another expression.
func (ti *trieIndex) mutableDirectory(name string) *trieNode {
	if name == "" || name == "/" {
		return ti.root
	}
	dirs, _ := ti.metricPathMutable(name+".", make([]*trieNode, 0, strings.Count(name, ".")+2))
	if len(dirs) != strings.Count(name, ".")+2 {
		return nil
	}
	return dirs[len(dirs)-1]
}

func (u *fileListUpdate) drainRealtimeMetrics() {
	for remaining := len(u.listener.newMetricsChan); remaining > 0; remaining-- {
		metric := <-u.listener.newMetricsChan
		node := u.listener.insertRealtimeMetric(u.trieIdx, metric)
		if node != nil && u.snapshotWriter != nil {
			if u.snapshotNotifications == nil {
				u.snapshotNotifications = make(map[string]*trieNode)
			}
			u.snapshotNotifications[metric] = node
		}
	}
}
