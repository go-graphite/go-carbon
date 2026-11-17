package v2core

import (
	"math"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

const rollupPartitionSeconds int64 = 86400

// Keep source samples through the last possible coarse window that can use
// them. This is deliberately conservative at the archive retention boundary.
func rollupRecordExpiry(c sealrollup.Config, r sealrollup.Record) int64 {
	a := c.Archives[r.Archive]
	extra := int64(0)
	if r.Archive+1 < len(c.Archives) {
		extra = c.Archives[r.Archive+1].Step
	}
	retention := a.Step*a.Count + extra
	if r.Sample.Time > math.MaxInt64-retention {
		return math.MaxInt64
	}
	return r.Sample.Time + retention
}

// Partition zero remains the durable metadata/legacy namespace. A positive
// partition names the UTC day in which the data become disposable, so archives
// with different retentions need not wait for the longest retention to expire.
func rollupExpiryPartition(c sealrollup.Config, r sealrollup.Record) int64 {
	return corePartition(rollupRecordExpiry(c, r), rollupPartitionSeconds) * rollupPartitionSeconds
}
