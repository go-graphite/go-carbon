package inputlog

import "github.com/go-graphite/go-carbon/points"

// Batch stages accepted points in two columns. It retains one reference and
// one point per row (24 bytes), without a redundant per-row stream hint. The
// encoder reads each reference's atomic hint when it consumes the batch.
// Refs and Points must have equal lengths and remain immutable until consumed.
type Batch struct {
	Refs   []*StreamRef
	Points []points.Point
}

// BatchSource optionally avoids Entry's per-point hint in the cache's staging
// buffers. It has the same generation and spare ownership contract as Source.
// This is an in-memory interface; the log's durable codecs are unchanged.
type BatchSource interface {
	TakeLogBatch(shard int, gen uint64, spare Batch) Batch
}
