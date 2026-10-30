package carbonserver

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
)

// Name sorting is bounded independently of metric cardinality. Temporary files
// contain names/stats only, not history. The final stream is in the same order
// as the filesystem walk, so the existing compact FST stays usable at scale.
const incrementalIndexRunBytes = 64 << 20
const incrementalIndexFanIn = 32

func (listener *CarbonserverListener) SetIncrementalIndexScratch(directory string) {
	listener.incrementalIndexDir = directory
}

func (u *fileListUpdate) scanIncrementalFiles(dir string, ticker <-chan time.Time) (ok bool) {
	db := u.listener.getMetricStore()
	if db == nil || db.IncrementalSource() == "" || !u.listener.trieIndex || u.listener.incrementalIndexDir == "" {
		u.logger.Error("incremental index requires shared ownership, trie and private disk scratch")
		return false
	}
	if err := os.MkdirAll(u.listener.incrementalIndexDir, 0750); err != nil {
		u.logger.Error("prepare incremental index scratch", zap.Error(err))
		return false
	}
	temporary, err := os.MkdirTemp(u.listener.incrementalIndexDir, "index-")
	if err != nil {
		u.logger.Error("prepare incremental index spool", zap.Error(err))
		return false
	}
	defer os.RemoveAll(temporary)
	defer func() {
		if !ok {
			u.scanFailed = true
			if u.fileListCache != nil {
				_ = u.fileListCache.Abort()
				u.fileListCache = nil
			}
			if u.snapshotWriter != nil {
				_ = u.snapshotWriter.abort()
				u.snapshotWriter = nil
			}
		}
	}()
	source := filepath.Join(temporary, "source.flc")
	spool, err := NewFileListCache(source, FLCVersion2, 'w')
	if err != nil {
		u.logger.Error("open incremental index spool", zap.Error(err))
		return false
	}
	u.incrementalSpool = spool
	// Complete the source walk BEFORE reading catalog pages. A file unlinked
	// before the walk reached it already has durable catalog ownership; a
	// conversion after this walk is represented by its spooled source entry.
	scanned := u.scanFiles(dir, ticker)
	u.incrementalSpool = nil
	closeErr := spool.Close()
	if !scanned || u.scanFailed || u.incrementalSpoolErr != nil || closeErr != nil {
		u.logger.Error("scan incremental source index", zap.Error(errors.Join(u.incrementalSpoolErr, closeErr)))
		return false
	}
	runs, err := u.incrementalCatalogRuns(db, temporary, incrementalIndexRunBytes, ticker)
	if err != nil {
		u.logger.Error("sort incremental catalog", zap.Error(err))
		return false
	}
	runs, err = u.compactIncrementalRuns(runs, temporary)
	if err != nil {
		u.logger.Error("merge incremental index runs", zap.Error(err))
		return false
	}
	u.fileListCache = u.newFileListCacheWriter()
	if u.listener.fileListCache != "" && u.fileListCache == nil {
		return false
	}
	u.metricsKnown = uint64(len(u.cacheMetricNames))
	u.filesLen = 0
	err = mergeIncrementalEntries(append([]string{source}, runs...), func(entry *FLCEntry) error {
		if u.cancelled() {
			return context.Canceled
		}
		u.refreshQuotaAndRealtimeMetrics(ticker)
		u.filesLen++
		firstSeen := u.indexFile(entry.Path, true, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints)
		u.cacheFile(entry.Path, true, entry.DataPoints, entry.LogicalSize, entry.PhysicalSize, firstSeen)
		if u.listener.fileListCache != "" && u.fileListCache == nil {
			return errors.New("write combined file-list cache failed")
		}
		if u.listener.internalStatsDir != "" {
			metric := metricFromVirtualPath(entry.Path)
			if u.details[metric] == nil {
				u.details[metric] = &protov3.MetricDetails{}
			}
			u.details[metric].Size, u.details[metric].RealSize = entry.LogicalSize, entry.PhysicalSize
		}
		return nil
	})
	if err != nil {
		u.logger.Error("merge source and shared indexes", zap.Error(err))
		return false
	}
	return true
}

func (u *fileListUpdate) compactIncrementalRuns(runs []string, directory string) ([]string, error) {
	for len(runs) >= incrementalIndexFanIn {
		var next []string
		for first := 0; first < len(runs); first += incrementalIndexFanIn {
			group := runs[first:min(first+incrementalIndexFanIn, len(runs))]
			name := filepath.Join(directory, fmt.Sprintf("merge-%d-%d.flc", len(runs), first))
			writer, err := NewFileListCache(name, FLCVersion2, 'w')
			if err != nil {
				return nil, err
			}
			err = mergeIncrementalEntries(group, func(entry *FLCEntry) error {
				if u.cancelled() {
					return context.Canceled
				}
				return writer.Write(entry)
			})
			err = errors.Join(err, writer.Close())
			if err != nil {
				return nil, err
			}
			for _, old := range group {
				_ = os.Remove(old)
			}
			next = append(next, name)
		}
		runs = next
	}
	return runs, nil
}

func metricFromVirtualPath(path string) string {
	result := []byte(path[1 : len(path)-4])
	for i, b := range result {
		if b == '/' {
			result[i] = '.'
		}
	}
	return string(result)
}

type incrementalSortEntry struct {
	entry FLCEntry
	key   []byte
}

func (u *fileListUpdate) incrementalCatalogRuns(db *store.Store, directory string, budget int, ticker <-chan time.Time) ([]string, error) {
	var entries []incrementalSortEntry
	var paths []string
	used := 0
	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
		path := filepath.Join(directory, fmt.Sprintf("catalog-%06d.flc", len(paths)))
		writer, err := NewFileListCache(path, FLCVersion2, 'w')
		if err != nil {
			return err
		}
		for i := range entries {
			if err := writer.Write(&entries[i].entry); err != nil {
				_ = writer.Abort()
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
		paths = append(paths, path)
		// Drop names/keys before reusing the bounded slice storage.
		clear(entries)
		entries, used = entries[:0], 0
		return nil
	}
	err := store.EachPage(context.Background(), db.ListCatalogPage, "", metricStoreCatalogPageSize, func(page []store.Metadata) error {
		if u.cancelled() {
			return context.Canceled
		}
		if !u.catalogRecovery {
			u.refreshQuotaAndRealtimeMetrics(ticker)
		}
		for _, metadata := range page {
			path := metricStoreVirtualPath(metadata.Name)
			logical, points := metricStoreLogicalSize(metadata)
			key, err := encodeSnapshotPath(nil, path)
			if err != nil {
				return err
			}
			entries = append(entries, incrementalSortEntry{entry: FLCEntry{Path: path, LogicalSize: logical, DataPoints: points}, key: key})
			used += len(path) + len(key) + 128
			if used >= budget {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	return paths, err
}

type incrementalIndexCursor struct {
	reader   FileListCache
	entry    FLCEntry
	key      []byte
	priority int
}

func (c *incrementalIndexCursor) next() error {
	if reader, ok := c.reader.(interface{ readInto(*FLCEntry) error }); ok {
		if err := reader.readInto(&c.entry); err != nil {
			return err
		}
	} else {
		entry, err := c.reader.Read()
		if err != nil {
			return err
		}
		c.entry = *entry
	}
	var err error
	c.key, err = encodeSnapshotPath(c.key, c.entry.Path)
	return err
}

type incrementalIndexHeap []*incrementalIndexCursor

func (h incrementalIndexHeap) Len() int { return len(h) }
func (h incrementalIndexHeap) Less(i, j int) bool {
	if order := bytes.Compare(h[i].key, h[j].key); order != 0 {
		return order < 0
	}
	return h[i].priority > h[j].priority
}
func (h incrementalIndexHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *incrementalIndexHeap) Push(value any) { *h = append(*h, value.(*incrementalIndexCursor)) }
func (h *incrementalIndexHeap) Pop() any {
	last := len(*h) - 1
	value := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return value
}

// Inputs are sorted by encodeSnapshotPath. Later inputs win duplicate paths,
// so a durable shared entry replaces an original pending asynchronous unlink.
func mergeIncrementalEntries(paths []string, emit func(*FLCEntry) error) (err error) {
	var readers []FileListCache
	defer func() {
		for _, reader := range readers {
			err = errors.Join(err, reader.Close())
		}
	}()
	var queue incrementalIndexHeap
	for priority, path := range paths {
		reader, err := NewFileListCache(path, FLCVersion2, 'r')
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		cursor := &incrementalIndexCursor{reader: reader, priority: priority}
		if err := cursor.next(); errors.Is(err, io.EOF) {
			continue
		} else if err != nil {
			return err
		}
		heap.Push(&queue, cursor)
	}
	var previous []byte
	for len(queue) > 0 {
		cursor := heap.Pop(&queue).(*incrementalIndexCursor)
		if !bytes.Equal(previous, cursor.key) {
			if err := emit(&cursor.entry); err != nil {
				return err
			}
			previous = append(previous[:0], cursor.key...)
		}
		if err := cursor.next(); errors.Is(err, io.EOF) {
			continue
		} else if err != nil {
			return err
		}
		heap.Push(&queue, cursor)
	}
	return nil
}
