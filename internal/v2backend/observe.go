package v2backend

import (
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// Optional batch-level diagnostics. Durations are summed wall time and may
// include blocking; they are not process CPU or an additive latency budget.
type stageCounters struct {
	enabled                                                 bool
	resolve, identity, append, journal, sync, plan, publish atomic.Uint64
	dataBytes, metadataBytes, files                         atomic.Uint64
	syncGroups                                              atomic.Uint64
	flushSync                                               atomic.Uint64
}

func (d *stageCounters) start() time.Time {
	if d.enabled {
		return time.Now()
	}
	return time.Time{}
}

func (d *stageCounters) finish(counter *atomic.Uint64, start time.Time) {
	if !start.IsZero() {
		counter.Add(uint64(time.Since(start)))
	}
}

// StageStats reports optional diagnostic counters for the experimental writer.
// Call before Close, after Flush for a complete committed-point denominator.
func (s *Store) StageStats() map[string]any {
	d := &s.stages
	m := map[string]any{
		"resolve_including_identity_ns": d.resolve.Load(), "identity_miss_ns": d.identity.Load(),
		"core_append_ns": d.append.Load(), "journal_encode_append_ns": d.journal.Load(),
		"seal_dependency_sync_ns": d.sync.Load(), "seal_plan_ns": d.plan.Load(),
		"snapshot_publish_ns": d.publish.Load(), "data_payload_bytes": d.dataBytes.Load(),
		"metadata_payload_bytes": d.metadataBytes.Load(), "planned_files": d.files.Load(),
		"dependency_sync_groups":   d.syncGroups.Load(),
		"flush_dependency_sync_ns": d.flushSync.Load(),
	}
	if s.core != nil {
		m["core"] = v2core.Sample(s.core, "backend", time.Now(), s.ingested.Load())
	}
	return m
}
