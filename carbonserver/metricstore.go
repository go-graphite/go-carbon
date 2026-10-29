package carbonserver

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgryski/go-trigram"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
)

// Incremental conversion preserves the ordinary filesystem index contract and
// exact values, so its discovery caches retain the existing TTL policy.
func (listener *CarbonserverListener) usesSharedCatalogCachePolicy() bool {
	db := listener.getMetricStore()
	return db != nil && db.IncrementalSource() == ""
}

const metricStoreCatalogPageSize = 4096

// UsesSharedCatalogIndex reports whether discovery uses only committed store metadata.
func (listener *CarbonserverListener) UsesSharedCatalogIndex() bool {
	return listener.getMetricIndexStore() != nil
}

// updateMetricStoreIndex creates the same virtual .wsp paths used by the
// filesystem index. It deliberately does not consult the file-list cache: the
// store catalog is authoritative across carbonserver restarts.
func (listener *CarbonserverListener) updateMetricStoreIndex(metricStore *store.Store) error {
	phasecpu.Set(phasecpu.Index)
	defer phasecpu.Set(phasecpu.Other)
	listener.metricStoreIndexMu.Lock()
	defer listener.metricStoreIndexMu.Unlock()
	if listener.indexPaused.Load() {
		return nil
	}

	started := time.Now()
	after := listener.metricStoreCursor
	previous := listener.CurrentFileIndex()
	if listener.metricStoreIndex != metricStore || previous == nil {
		after = 0
	}
	changes, cursor, rebuild, err := metricStore.CatalogChanges(context.Background(), after)
	if err != nil {
		return err
	}
	if listener.trieIndex && listener.fileListCache != "" && listener.fileListCacheVersion == FLCVersion2 {
		return listener.updateMappedMetricStoreIndex(metricStore, previous, changes, cursor, rebuild)
	}
	if !rebuild && len(changes) == 0 {
		listener.metricStoreCursor = cursor
		listener.refreshMetricStoreSpace(previous)
		return nil
	}
	if !rebuild && listener.trieIndex && previous.trieIdx != nil {
		// Existing policies and deletions still use a full rebuild. Ordinary
		// writes never enter the journal; only new names extend the live trie.
		listener.fileIdxMutex.Lock()
		for _, m := range changes {
			if previous.details[m.Name] != nil {
				rebuild = true
				break
			}
		}
		listener.fileIdxMutex.Unlock()
		if !rebuild {
			for _, m := range changes {
				logical, points := metricStoreLogicalSize(m)
				if _, err := previous.trieIdx.insert(metricStoreVirtualPath(m.Name), logical, 0, points, 0); err != nil {
					return err
				}
				listener.fileIdxMutex.Lock()
				previous.details[m.Name] = &protov3.MetricDetails{Size: logical}
				listener.fileIdxMutex.Unlock()
			}
			if listener.isQuotaEnabled() {
				if err := listener.applySharedStoreQuotas(previous.trieIdx); err != nil {
					return err
				}
			}
			listener.metricStoreCursor = cursor
			atomic.AddUint64(&listener.metrics.MetricsKnown, uint64(len(changes)))
			atomic.AddUint64(&listener.metrics.TrieFiles, uint64(len(changes)))
			atomic.AddUint64(&listener.metrics.FileScanTimeNS, uint64(time.Since(started)))
			atomic.AddUint64(&listener.metrics.IndexBuildTimeNS, uint64(time.Since(started)))
			listener.refreshMetricStoreSpace(previous)
			return nil
		}
	}
	files, details, trieIdx, metricsKnown, err := listener.loadMetricStoreCatalog(metricStore)
	if err != nil {
		return err
	}

	var freeSpace, totalSpace uint64
	if err := metricStoreFilesystemStats(listener.whisperData, &freeSpace, &totalSpace); err != nil {
		listener.logger.Debug("failed to read shared metric-store filesystem stats", zap.Error(err))
	}
	nfidx := &fileIndex{
		details:     details,
		accessTimes: make(map[string]int64),
		// A shared store cannot attribute physical bytes to one metric.
		freeSpace:  freeSpace,
		totalSpace: totalSpace,
	}
	var indexSize int
	if listener.trieIndex {
		if listener.isQuotaEnabled() {
			if err := listener.applySharedStoreQuotas(trieIdx); err != nil {
				return err
			}
		}
		nfidx.trieIdx = trieIdx
		count, files, dirs := trieIdx.builder.nodes, int(trieIdx.getFileCount()), trieIdx.builder.dirs
		trieIdx.builder = nil
		atomic.StoreUint64(&listener.metrics.TrieNodes, uint64(count))
		atomic.StoreUint64(&listener.metrics.TrieFiles, uint64(files))
		atomic.StoreUint64(&listener.metrics.TrieDirs, uint64(dirs))
		indexSize = count
	} else {
		nfidx.files = files
		nfidx.idx = trigram.NewIndex(files)
		nfidx.idx.Prune(0.95)
		indexSize = len(nfidx.idx)
	}

	if previous != nil && listener.internalStatsDir != "" {
		listener.fileIdxMutex.Lock()
		for metric, accessTime := range previous.accessTimes {
			if detail, ok := nfidx.details[metric]; ok {
				detail.RdTime = accessTime
				nfidx.accessTimes[metric] = accessTime
			} else if listener.db != nil {
				listener.db.Delete([]byte(metric), nil)
			}
		}
		listener.fileIdxMutex.Unlock()
	}
	listener.UpdateFileIndex(nfidx)
	listener.metricStoreIndex, listener.metricStoreCursor = metricStore, cursor
	atomic.StoreUint64(&listener.metrics.MetricsKnown, metricsKnown)
	atomic.AddUint64(&listener.metrics.FileScanTimeNS, uint64(time.Since(started)))
	atomic.AddUint64(&listener.metrics.IndexBuildTimeNS, uint64(time.Since(started)))
	listener.logger.Info("shared metric-store index updated",
		zap.Uint64("metrics_known", metricsKnown),
		zap.Int("index_size", indexSize),
		zap.Duration("runtime", time.Since(started)),
	)
	return nil
}

func (listener *CarbonserverListener) refreshMetricStoreSpace(previous *fileIndex) {
	var free, total uint64
	if err := metricStoreFilesystemStats(listener.whisperData, &free, &total); err == nil && (free != previous.freeSpace || total != previous.totalSpace) {
		// Readers retain their index; only the small space-stat wrapper changes.
		next := *previous
		next.freeSpace, next.totalSpace = free, total
		listener.UpdateFileIndex(&next)
	}
}

func (listener *CarbonserverListener) loadMetricStoreCatalog(metricStore *store.Store) ([]string, map[string]*protov3.MetricDetails, *trieIndex, uint64, error) {
	var files []string
	details := make(map[string]*protov3.MetricDetails)
	seenPaths := make(map[string]struct{})
	var trieIdx *trieIndex
	if listener.trieIndex {
		trieIdx = newTrie(".wsp", listener.maxCreatesPerSecond, listener.estimateSize)
		trieIdx.builder = &trieBulkBuilder{}
	}
	return listener.loadMetricStoreCatalogPages(metricStore, files, details, trieIdx, seenPaths)
}

func (listener *CarbonserverListener) loadMetricStoreCatalogPages(metricStore *store.Store, files []string, details map[string]*protov3.MetricDetails, trieIdx *trieIndex, seenPaths map[string]struct{}) ([]string, map[string]*protov3.MetricDetails, *trieIndex, uint64, error) {
	var metricsKnown uint64
	var indexErr error
	err := store.EachPage(context.Background(), metricStore.ListPage, "", metricStoreCatalogPageSize, func(page []store.Metadata) error {
		for _, metadata := range page {
			files, indexErr = listener.addMetricStoreCatalogEntry(metadata, files, details, trieIdx, seenPaths)
			if indexErr != nil {
				return indexErr
			}
			metricsKnown++
		}
		return nil
	})
	if indexErr != nil {
		return nil, nil, nil, 0, indexErr
	}
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("list shared metric-store catalog: %w", err)
	}
	return files, details, trieIdx, metricsKnown, nil
}

func (listener *CarbonserverListener) addMetricStoreCatalogEntry(metadata store.Metadata, files []string, details map[string]*protov3.MetricDetails, trieIdx *trieIndex, seenPaths map[string]struct{}) ([]string, error) {
	path := metricStoreVirtualPath(metadata.Name)
	logical, dataPoints := metricStoreLogicalSize(metadata)
	details[metadata.Name] = &protov3.MetricDetails{Size: logical}
	if listener.trieIndex {
		if _, err := trieIdx.insert(path, logical, 0, dataPoints, 0); err != nil {
			return nil, fmt.Errorf("index shared metric %q: %w", metadata.Name, err)
		}
		return files, nil
	}
	for _, virtualPath := range metricStoreTrigramPaths(path) {
		if _, ok := seenPaths[virtualPath]; ok {
			continue
		}
		seenPaths[virtualPath] = struct{}{}
		files = append(files, virtualPath)
	}
	return files, nil
}

func metricStoreVirtualPath(name string) string {
	return "/" + strings.ReplaceAll(name, ".", "/") + ".wsp"
}

func metricStoreTrigramPaths(metricPath string) []string {
	parts := strings.Split(strings.TrimPrefix(metricPath, "/"), "/")
	paths := make([]string, 0, len(parts))
	for i := range parts {
		paths = append(paths, "/"+strings.Join(parts[:i+1], "/"))
	}
	return paths
}

func metricStoreLogicalSize(metadata store.Metadata) (int64, int64) {
	logicalSize := int64(16 + 12*len(metadata.Retentions)) // classic header and archive descriptors
	var dataPoints int64
	for _, retention := range metadata.Retentions {
		points := int64(retention.NumberOfPoints())
		dataPoints += points
		logicalSize += points * 12 // classic Whisper point: timestamp + float64
	}
	return logicalSize, dataPoints
}

func metricStoreFilesystemStats(path string, freeSpace, totalSpace *uint64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return err
	}
	if stat.Bavail >= 0 { // nolint:staticcheck // skipcq: SCC-SA4003
		*freeSpace = uint64(stat.Bavail) * uint64(stat.Bsize)
	}
	*totalSpace = stat.Blocks * uint64(stat.Bsize)
	return nil
}

func (listener *CarbonserverListener) applySharedStoreQuotas(trieIdx *trieIndex) error {
	started := time.Now()
	throughputs, err := trieIdx.applyQuotas(listener.quotaUsageReportFrequency, listener.getQuotas()...)
	if err != nil {
		return fmt.Errorf("apply shared metric-store quotas: %w", err)
	}
	atomic.StoreUint64(&listener.metrics.QuotaApplyTimeNs, uint64(time.Since(started)))
	usageStarted := time.Now()
	files := trieIdx.refreshUsage(throughputs)
	atomic.StoreUint64(&listener.metrics.UsageRefreshTimeNs, uint64(time.Since(usageStarted)))
	atomic.StoreUint64(&listener.metrics.MetricsKnown, files)
	return nil
}
