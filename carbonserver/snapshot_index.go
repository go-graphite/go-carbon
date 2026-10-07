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
	if _, found, err := ti.snapshot.lookup("/" + strings.ReplaceAll(metric, ".", "/") + ti.fileExt); err == nil && found {
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
	// Walk shared prefixes once. Namespace ranges enumerate ordered FST state
	// twice per ancestor, although quota admission needs only prefix existence.
	root := ti.snapshot.index.Accept(ti.snapshot.index.Start(), 0)
	state := root
	for end := 0; end < len(metric); end++ {
		c := metric[end]
		if c == '.' {
			c = 0
		}
		if ti.snapshot.index.CanMatch(state) {
			state = ti.snapshot.index.Accept(state, c)
		}
		if metric[end] != '.' {
			continue
		}
		name := metric[:end]
		exists := ti.snapshot.index.CanMatch(state)
		if name == "" || name == "/" {
			exists = ti.snapshot.index.CanMatch(root)
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
		if rows[i].name == rows[j].name {
			return rows[i].leaf && !rows[j].leaf
		}
		return rows[i].name < rows[j].name
	})
	names, leaves, nodes = nil, nil, nil
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
	return names, leaves, nodes, lookups, nil
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

func (ti *trieIndex) allMetrics(sep byte) []string {
	files := ti.allMetricsMutable(sep)
	if ti.snapshot == nil {
		return files
	}
	_ = ti.snapshot.walkNamespace("/", func(name string, _ uint64) bool {
		if sep != '.' {
			name = strings.ReplaceAll(name, ".", string(sep))
		}
		files = append(files, name)
		return true
	})
	sort.Strings(files)
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
		m := n.meta.(*fileMeta)
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
	usage := make(map[string]QuotaUsage)
	reads := make(map[string][2]int64)
	dirs := make(map[string]bool)
	names, nodes, _, _, _ := ti.allMetricsNodeMutable(ti.root, '.', "", int(^uint(0)>>1), false)
	for i, name := range names {
		m := nodes[i].meta.(*fileMeta)
		hits := atomic.SwapInt64(&m.readHits, 0)
		readBytes := atomic.SwapInt64(&m.readBytes, 0)
		metricNamespaces(name, func(prefix string) {
			u := usage[prefix]
			u.Metrics++
			u.LogicalSize += m.logicalSize
			u.PhysicalSize += m.physicalSize
			u.DataPoints += m.dataPoints
			usage[prefix] = u
			r := reads[prefix]
			r[0] += hits
			r[1] += readBytes
			reads[prefix] = r
			if prefix != "/" {
				dirs[prefix] = true
			}
		})
	}
	extraDirs := 0
	exists := ti.snapshot.namespaceLookup()
	for name := range dirs {
		if exists(name) {
			continue
		}
		parent := "/"
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			parent = name[:i]
		}
		u := usage[parent]
		u.Namespaces++
		usage[parent] = u
		extraDirs++
	}
	return usage, reads, extraDirs
}

func (ti *trieIndex) refreshUsage(throughputs *throughputQuotaManager) uint64 {
	if ti.snapshot == nil {
		return ti.refreshUsageMutable(throughputs)
	}
	extra, reads, _ := ti.overlayUsage()
	ti.snapshot.nodes.files.Range(func(key, value any) bool {
		m := value.(*trieNode).meta.(*fileMeta)
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
		u := node.meta.(*dirMeta).usage
		atomic.StoreInt64(&u.Metrics, base.Metrics+delta.Metrics)
		atomic.StoreInt64(&u.Namespaces, base.Namespaces+delta.Namespaces)
		atomic.StoreInt64(&u.LogicalSize, base.LogicalSize+delta.LogicalSize)
		atomic.StoreInt64(&u.PhysicalSize, base.PhysicalSize+delta.PhysicalSize)
		atomic.StoreInt64(&u.DataPoints, base.DataPoints+delta.DataPoints)
		var throughput int64
		if throughputs != nil {
			if te := throughputs.load(name); te != nil {
				throughput = atomic.LoadInt64(&te.offset().dataPoints)
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
	return ti.snapshot.manifest.Records + uint64(ti.fileCount)
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
	fmt.Fprintf(w, "snapshot: %d metrics; mutable trie: %d metrics\n", ti.snapshot.manifest.Records, ti.fileCount)
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
		path := "/" + strings.ReplaceAll(name, ".", "/") + ".wsp"
		if _, found, err := snapshot.lookup(path); err == nil && found {
			continue
		}
		var m fileMeta
		if node != nil {
			source := node.meta.(*fileMeta)
			m.logicalSize = atomic.LoadInt64(&source.logicalSize)
			m.physicalSize = atomic.LoadInt64(&source.physicalSize)
			m.dataPoints = atomic.LoadInt64(&source.dataPoints)
			m.firstSeenAt = atomic.LoadInt64(&source.firstSeenAt)
		}
		if _, err := next.insertMutable(path, m.logicalSize, m.physicalSize, m.dataPoints, m.firstSeenAt); err != nil {
			_ = snapshot.close()
			u.logger.Warn("failed to transfer snapshot overlay", zap.Error(err))
			return
		}
	}
	u.listener.drainRealtimeMetrics(next)
	u.trieIdx = next
	u.metricsKnown = snapshot.manifest.Records + uint64(next.fileCount)
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
