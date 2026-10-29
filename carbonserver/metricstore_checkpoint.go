package carbonserver

import "go.uber.org/zap"

// PrepareReadCheckpoint freezes discovery before checkpoint warmup. A catalog
// trie without a mapped generation needs only an empty immutable base: its
// existing names become the saved overlay. Mapped catalogs reuse their base.
func (l *CarbonserverListener) PrepareReadCheckpoint() bool {
	l.PauseIndexUpdates()
	l.metricStoreIndexMu.Lock()
	defer l.metricStoreIndexMu.Unlock()
	l.checkpointMu.Lock()
	defer l.checkpointMu.Unlock()
	if l.checkpointIndex != nil {
		return true
	}
	index := l.CurrentFileIndex()
	if index == nil || index.trieIdx == nil {
		return false
	}
	if index.trieIdx.snapshot != nil {
		return true
	}
	if l.getMetricIndexStore() == nil || !l.concurrentIndex || l.fileListCache == "" || l.fileListCacheVersion != FLCVersion2 {
		return false
	}
	snapshot, err := l.emptyMetricStoreSnapshot()
	if err != nil {
		l.logger.Warn("shared catalog checkpoint unavailable; using ordered restore", zap.Error(err))
		return false
	}
	old := index.trieIdx
	// The updater is stopped. Root metadata and throughput counters remain
	// shared with readers; only checkpoint helpers use the immutable base.
	trie := &trieIndex{
		snapshot: snapshot, root: old.root, fileExt: old.fileExt,
		fileCount: old.getFileCount(), depth: old.getDepth(), longestMetric: old.longestMetric,
		estimateSize: old.estimateSize, maxCreatesTicker: old.maxCreatesTicker,
		throughputs: old.throughputs, quotaNodes: old.quotaNodes,
		resetFrequency: old.getResetFrequency(), logger: old.logger,
	}
	checkpoint := *index
	checkpoint.trieIdx = trie
	l.checkpointIndex = &checkpoint
	return true
}

func (l *CarbonserverListener) emptyMetricStoreSnapshot() (*indexSnapshot, error) {
	cache, err := NewFileListCache(l.fileListCache, FLCVersion2, 'w')
	if err != nil {
		return nil, err
	}
	if err := cache.Close(); err != nil {
		return nil, err
	}
	writer, err := newIndexSnapshotWriter(l.fileListCache, l.whisperData)
	if err != nil {
		return nil, err
	}
	defer writer.abort()
	if err := writer.finish(); err != nil {
		return nil, err
	}
	return openIndexSnapshot(l.fileListCache, l.whisperData)
}

func (l *CarbonserverListener) readCheckpointIndex() *fileIndex {
	l.checkpointMu.Lock()
	index := l.checkpointIndex
	l.checkpointMu.Unlock()
	if index != nil {
		return index
	}
	return l.CurrentFileIndex()
}
