package carbonserver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
)

// Bound the mutable catalog independently of the number of stored metrics.
// Compaction streams the authoritative catalog into the same mapped generation
// used during migration; it never consults the reclaimed source tree.
const metricStoreOverlayLimit = 100000

func (l *CarbonserverListener) updateMappedMetricStoreIndex(db *store.Store, previous *fileIndex, changes []store.Metadata, cursor uint64, rebuild bool) error {
	started := time.Now()
	if previous == nil || previous.trieIdx == nil || previous.trieIdx.snapshot == nil {
		rebuild = true
	}
	if !rebuild {
		ti := previous.trieIdx
		if int(ti.getFileCount())+len(changes) > metricStoreOverlayLimit {
			rebuild = true
		}
		for _, m := range changes {
			// Packed metadata is immutable. Mutable entries can instead be
			// updated in place, including names notified before their commit.
			found, err := ti.snapshot.containsMetric(m.Name, ti.fileExt)
			if err != nil {
				return err
			}
			if found {
				rebuild = true
				break
			}
		}
	}
	if rebuild {
		next, err := l.loadMappedMetricStoreCatalog(db)
		if err != nil {
			return err
		}
		if previous != nil && previous.trieIdx != nil {
			next.trieIdx.throughputs = previous.trieIdx.throughputs
			if l.cacheGet != nil {
				old := previous.trieIdx
				names, nodes, _, _, _ := old.allMetricsNodeMutable(old.root, '.', "", int(^uint(0)>>1), false)
				for i, name := range names {
					if len(l.cacheGet(name)) != 0 {
						if err := next.trieIdx.copyOverlayMetric(name, nodes[i]); err != nil {
							return errors.Join(err, next.trieIdx.snapshot.close())
						}
					}
				}
			}
		}
		if previous != nil && l.internalStatsDir != "" {
			l.fileIdxMutex.Lock()
			for metric, accessed := range previous.accessTimes {
				if detail := next.details[metric]; detail != nil {
					detail.RdTime = accessed
					next.accessTimes[metric] = accessed
				} else if l.db != nil {
					l.db.Delete([]byte(metric), nil)
				}
			}
			l.fileIdxMutex.Unlock()
		}
		if l.isQuotaEnabled() {
			if err := l.applySharedStoreQuotas(next.trieIdx); err != nil {
				return errors.Join(err, next.trieIdx.snapshot.close())
			}
		}
		l.UpdateFileIndex(next)
		previous = next
	} else if len(changes) != 0 {
		for _, m := range changes {
			logical, points := metricStoreLogicalSize(m)
			if _, err := previous.trieIdx.insert(metricStoreVirtualPath(m.Name), logical, 0, points, 0); err != nil {
				return err
			}
			if l.internalStatsDir != "" {
				l.fileIdxMutex.Lock()
				previous.details[strings.Clone(m.Name)] = &protov3.MetricDetails{Size: logical}
				l.fileIdxMutex.Unlock()
			}
		}
		if l.isQuotaEnabled() {
			if err := l.applySharedStoreQuotas(previous.trieIdx); err != nil {
				return err
			}
		}
	}
	l.metricStoreIndex, l.metricStoreCursor = db, cursor
	files := previous.trieIdx.snapshot.manifest.Records + uint64(previous.trieIdx.getFileCount())
	atomic.StoreUint64(&l.metrics.MetricsKnown, files)
	atomic.StoreUint64(&l.metrics.TrieFiles, files)
	if rebuild || len(changes) != 0 {
		atomic.AddUint64(&l.metrics.IndexBuildTimeNS, uint64(time.Since(started)))
		atomic.AddUint64(&l.metrics.FileScanTimeNS, uint64(time.Since(started)))
	}
	if rebuild {
		nodes, _, dirs, _, _, _, _, _ := previous.trieIdx.countNodes()
		atomic.StoreUint64(&l.metrics.TrieNodes, uint64(nodes))
		atomic.StoreUint64(&l.metrics.TrieDirs, uint64(dirs))
		l.logger.Info("shared metric-store index updated",
			zap.Uint64("metrics_known", files),
			zap.Uint64("mapped_metrics", previous.trieIdx.snapshot.manifest.Records),
			zap.Int64("overlay_metrics", previous.trieIdx.getFileCount()),
			zap.Duration("runtime", time.Since(started)),
		)
	}
	l.refreshMetricStoreSpace(previous)
	return nil
}

func (l *CarbonserverListener) loadMappedMetricStoreCatalog(db *store.Store) (_ *fileIndex, err error) {
	directory := l.incrementalIndexDir
	if directory == "" {
		directory = filepath.Dir(l.fileListCache)
	}
	if err := os.MkdirAll(directory, 0750); err != nil {
		return nil, err
	}
	temporary, err := os.MkdirTemp(directory, "index-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(temporary)
	// A private update supplies the bounded external sort and cancellation,
	// without extending the live trie or running quota callbacks under its lock.
	u := &fileListUpdate{listener: l, logger: l.logger, catalogRecovery: true}
	runs, err := u.incrementalCatalogRuns(db, temporary, incrementalIndexRunBytes, nil)
	if err != nil {
		return nil, err
	}
	runs, err = u.compactIncrementalRuns(runs, temporary)
	if err != nil {
		return nil, err
	}
	cache, err := NewFileListCache(l.fileListCache, FLCVersion2, 'w')
	if err != nil {
		return nil, err
	}
	defer cache.Abort()
	writer, err := newIndexSnapshotWriter(l.fileListCache, l.whisperData)
	if err != nil {
		return nil, err
	}
	defer writer.abort()
	index := &fileIndex{details: make(map[string]*protov3.MetricDetails), accessTimes: make(map[string]int64)}
	err = mergeIncrementalEntries(runs, func(entry *FLCEntry) error {
		if u.cancelled() {
			return context.Canceled
		}
		if err := cache.Write(entry); err != nil {
			return err
		}
		if l.internalStatsDir != "" {
			index.details[metricFromVirtualPath(entry.Path)] = &protov3.MetricDetails{Size: entry.LogicalSize}
		}
		return writer.append(entry)
	})
	if err != nil {
		return nil, err
	}
	if err := cache.Close(); err != nil {
		return nil, err
	}
	if err := writer.finish(); err != nil {
		return nil, err
	}
	snapshot, err := openIndexSnapshot(l.fileListCache, l.whisperData)
	if err != nil {
		return nil, err
	}
	index.trieIdx = newTrie(".wsp", l.maxCreatesPerSecond, l.estimateSize)
	index.trieIdx.snapshot = snapshot
	if err := metricStoreFilesystemStats(l.whisperData, &index.freeSpace, &index.totalSpace); err != nil {
		return nil, errors.Join(err, snapshot.close())
	}
	l.prewarmNamespaceHashes(snapshot)
	return index, nil
}
