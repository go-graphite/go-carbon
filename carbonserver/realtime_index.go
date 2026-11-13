package carbonserver

import (
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// IndexNewMetric inserts a metric synchronously. Live ingestion uses the pending
// queue instead, so producers never wait for the trie writer lock.
func (listener *CarbonserverListener) IndexNewMetric(metric string) bool {
	if !listener.trieIndex || !listener.concurrentIndex || listener.realtimeIndex <= 0 || listener.indexPaused.Load() {
		return false
	}
	listener.metricStoreIndexMu.Lock()
	defer listener.metricStoreIndexMu.Unlock()
	if listener.indexPaused.Load() {
		return false
	}
	return listener.indexNewMetricLocked(metric)
}

func (listener *CarbonserverListener) indexNewMetricLocked(metric string) bool {
	fidx := listener.CurrentFileIndex()
	if fidx == nil || fidx.trieIdx == nil {
		// Keep initial readiness tied to the complete catalogue. Once its trie is
		// stable, overflow can still extend it without waiting for the disk walk.
		u := listener.realtimeScan
		if u == nil || !u.acceptRealtime || u.trieIdx == nil {
			return false
		}
		node := listener.insertRealtimeMetric(u.trieIdx, metric)
		u.rememberRealtimeMetric(metric, node)
		if node != nil {
			listener.markDiscoveryChange(metric)
		}
		return true
	}
	before := fidx.trieIdx.getFileCount()
	node := listener.insertRealtimeMetric(fidx.trieIdx, metric)
	if fidx.trieIdx.getFileCount() != before {
		listener.markDiscoveryChange(metric)
	}
	if u := listener.realtimeScan; u != nil {
		u.rememberRealtimeMetric(metric, node)
	}
	if next := listener.realtimeNext; node != nil && next != nil && next != fidx.trieIdx {
		listener.insertRealtimeMetric(next, metric)
	}
	// Unstorable/invalid names are accounted for by insertRealtimeMetric.
	return true
}

// startRealtimeIndexer runs independently of filesystem scans, quota ticks and
// startup recovery gates. The enclosing updater joins it before stopping.
func (listener *CarbonserverListener) startRealtimeIndexer(exit <-chan struct{}) func() {
	if !listener.trieIndex || !listener.concurrentIndex || listener.realtimeIndex <= 0 {
		return func() {}
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		// Mixed source/catalog startup must publish its complete union before it
		// advertises readiness. Keep queued notifications until that publication.
		if listener.CurrentFileIndex() == nil {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for listener.CurrentFileIndex() == nil {
				select {
				case <-exit:
					return
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}
		for {
			select {
			case <-exit:
				return
			case <-stop:
				return
			case <-listener.newMetricsQueue.Ready():
			}
			for {
				select {
				case <-exit:
					return
				case <-stop:
					return
				default:
				}
				listener.metricStoreIndexMu.Lock()
				batch := listener.newMetricsQueue.Take(4096)
				slices.Sort(batch)
				for _, metric := range batch {
					listener.indexNewMetricLocked(metric)
				}
				listener.newMetricsQueue.Ack(batch)
				listener.metricStoreIndexMu.Unlock()
				if len(batch) == 0 {
					break
				}
			}
		}
	}()
	return func() { close(stop); <-done }
}

func (listener *CarbonserverListener) realtimeMetricDropped(metric string) {
	if atomic.AddUint64(&listener.metrics.RealtimeIndexDropped, 1) == 1 {
		listener.logger.Warn("realtime index metric dropped", zap.String("metric", metric))
	}
}

// walkMutableMetrics uses the same atomic links as queries. The path buffer and
// recursion stack are bounded by metric depth, independent of overlay size.
// Writers may add names during this walk; dual writes preserve those arrivals.
func (ti *trieIndex) walkMutableMetrics(visit func([]byte, *trieNode) error) error {
	var path []byte
	var walk func(*trieNode) error
	walk = func(node *trieNode) error {
		if node != ti.root && node.file() {
			return visit(path, node)
		}
		length := len(path)
		if node.dir() {
			path = append(path, '.')
		} else {
			path = append(path, node.c...)
		}
		children := node.getChildrens()
		for i := range children {
			if err := walk(node.getChild(children, i)); err != nil {
				return err
			}
		}
		path = path[:length]
		return nil
	}
	return walk(ti.root)
}

// Normal dotted names need neither filepath.Clean nor an allocated virtual
// path. The generic trie builder copies every retained label from this scratch.
// Noncanonical names retain the existing path normalization and validation.
func (l *CarbonserverListener) insertRealtimeCanonical(ti *trieIndex, metric string) (*trieNode, error, bool) {
	if metric == "" || metric[0] == '.' || metric[len(metric)-1] == '.' || strings.Contains(metric, "..") || strings.ContainsAny(metric, "/\\") {
		return nil, nil, false
	}
	if l.getMetricStore() != nil {
		if store.ValidateMetricName(metric) != nil || !metricStoreIndexableName(metric) {
			return nil, nil, false
		}
	} else if strings.IndexByte(metric, ';') < 0 {
		if len(l.whisperData)+len(metric)+5 > helper.MaxWhisperPathLength {
			return nil, nil, false
		}
		for remaining := metric; ; {
			end := strings.IndexByte(remaining, '.')
			if end < 0 {
				if len(remaining)+4 > helper.MaxWhisperFilenameLength {
					return nil, nil, false
				}
				break
			}
			if end > helper.MaxWhisperFilenameLength {
				return nil, nil, false
			}
			remaining = remaining[end+1:]
		}
		if !helper.WhisperPathFits(l.whisperData) {
			return nil, nil, false
		}
	}
	if ti.snapshot != nil {
		found, err := ti.snapshot.containsMetric(metric, ti.fileExt)
		if found || err != nil {
			return nil, nil, false
		}
	}
	path := append(l.realtimePath[:0], '/')
	for i := range len(metric) {
		c := metric[i]
		if c == '.' {
			c = '/'
		}
		path = append(path, c)
	}
	path = append(path, ti.fileExt...)
	l.realtimePath = path
	if ti.insertCursor == nil {
		ti.insertCursor = &trieBulkBuilder{}
	}
	node, err := insertMutablePath(ti, path, 0, 0, 0, 0)
	return node, err, true
}
