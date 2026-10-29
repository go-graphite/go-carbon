package chunkstore

import "sync/atomic"

// Block counts are from export iterators, excluding concurrent ingestion and
// background compaction. CachedBytes/BlockBytes is a byte-weighted hit ratio.
type TransferExportStats struct {
	Requests, Metrics, MetadataHits, BlockBytes, CachedBytes, Seeks, Steps uint64
}
type transferExportCounters struct {
	requests, metrics, metadataHits, blockBytes, cachedBytes, seeks, steps atomic.Uint64
}

func (c *transferExportCounters) stats() TransferExportStats {
	return TransferExportStats{c.requests.Load(), c.metrics.Load(), c.metadataHits.Load(), c.blockBytes.Load(), c.cachedBytes.Load(), c.seeks.Load(), c.steps.Load()}
}
