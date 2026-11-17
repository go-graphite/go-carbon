package carbon

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// App's lifecycle lock keeps the engine open during collection. No producer
// lock, name scan or data read is needed, even with a large catalog.
type v2Collector struct{ app *App }

var v2GCDescs = struct{ pending, removed, unlinked *prometheus.Desc }{
	prometheus.NewDesc("v2_part_gc_pending_parts", "Retired sealed parts awaiting original-link removal.", nil, nil),
	prometheus.NewDesc("v2_part_gc_removed_parts_total", "Retired parts whose original links were removed in this process.", nil, nil),
	prometheus.NewDesc("v2_part_gc_unlinked_bytes_total", "Bytes in removed original part links; hardlinks and pinned SSTs can retain physical space.", nil, nil),
}

var v2Descs = struct {
	ingest, committed, buffered, dispatch, seals, sealTime, sealLag       *prometheus.Desc
	parts, debt, compacted, obsolete, live, dual, reads, names, rollupLag *prometheus.Desc
}{
	ingest:    prometheus.NewDesc("v2_ingest_points_total", "Points accepted by the v2 TCP receiver; use rate() for points per second.", nil, nil),
	committed: prometheus.NewDesc("v2_committed_points_total", "Points durably sealed by this v2 process, including journal replay.", nil, nil),
	buffered:  prometheus.NewDesc("v2_buffered_points", "Points in owner heads awaiting a durable seal; excludes receiver and dispatch queues.", nil, nil),
	dispatch:  prometheus.NewDesc("v2_dispatch_bytes", "Bytes held by bounded producer dispatch leases.", nil, nil),
	seals:     prometheus.NewDesc("v2_seals_total", "Owner seal attempts, including failed attempts.", nil, nil),
	sealTime:  prometheus.NewDesc("v2_seal_seconds_total", "Time spent sorting, reducing and durably writing owner seals, summed across owners.", nil, nil),
	sealLag:   prometheus.NewDesc("v2_seal_lag_seconds", "Maximum age of an unsealed owner head; excludes time in receiver and dispatch queues. Rollups share this seal boundary.", nil, nil),
	rollupLag: prometheus.NewDesc("v2_rollup_lag_seconds", "Age of the oldest unsealed owner head. Rollups are committed in that same seal; excludes upstream queues.", nil, nil),
	parts:     prometheus.NewDesc("v2_parts", "Current physical SST files by LSM level; not logical archive parts.", []string{"level"}, nil),
	debt:      prometheus.NewDesc("v2_compaction_debt_bytes", "Estimated LSM compaction work remaining in bytes.", nil, nil),
	compacted: prometheus.NewDesc("v2_compacted_bytes_total", "Bytes written by LSM compactions in this process.", nil, nil),
	obsolete:  prometheus.NewDesc("v2_obsolete_bytes", "Obsolete SST bytes eligible for deletion; excludes reader-pinned zombie tables.", nil, nil),
	live:      prometheus.NewDesc("v2_live_sst_bytes", "Bytes in live SST files across all levels.", nil, nil),
	dual:      prometheus.NewDesc("v2_dual_write_enabled", "One for dual writes. The current v2 runtime is a sole writer and reports zero.", nil, nil),
	reads:     prometheus.NewDesc("v2_read_duration_seconds", "FetchMany wall time including errors, cancellation and consumer callbacks.", nil, nil),
	names:     prometheus.NewDesc("v2_catalog_names", "Names in the latest immutable discovery view, including classic union names.", nil, nil),
}

func (*v2Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- v2MoveDesc
	ch <- v2MoveOwnerDesc
	ch <- v2MoveRegressionDesc
	for _, d := range []*prometheus.Desc{v2GCDescs.pending, v2GCDescs.removed, v2GCDescs.unlinked} {
		ch <- d
	}
	for _, d := range []*prometheus.Desc{v2Descs.ingest, v2Descs.committed, v2Descs.buffered, v2Descs.dispatch, v2Descs.seals, v2Descs.sealTime, v2Descs.sealLag, v2Descs.parts, v2Descs.debt, v2Descs.compacted, v2Descs.obsolete, v2Descs.live, v2Descs.dual, v2Descs.reads, v2Descs.names, v2Descs.rollupLag} {
		ch <- d
	}
}

func (c *v2Collector) Collect(ch chan<- prometheus.Metric) {
	c.app.Lock()
	defer c.app.Unlock()
	v := c.app.v2
	if v == nil || v.store == nil || v.server == nil {
		return
	}
	collectV2Moves(v.moves, ch)
	s := v.store.Core.Observe()
	emit := func(d *prometheus.Desc, kind prometheus.ValueType, value float64) {
		ch <- prometheus.MustNewConstMetric(d, kind, value)
	}
	var pendingGC, removedGC, unlinkedGC uint64
	for _, gc := range v.store.Core.PartGCStats() {
		pendingGC += gc.PendingParts
		removedGC += gc.RemovedParts
		unlinkedGC += gc.UnlinkedFileBytes
	}
	emit(v2GCDescs.pending, prometheus.GaugeValue, float64(pendingGC))
	emit(v2GCDescs.removed, prometheus.CounterValue, float64(removedGC))
	emit(v2GCDescs.unlinked, prometheus.CounterValue, float64(unlinkedGC))
	emit(v2Descs.ingest, prometheus.CounterValue, float64(v.server.Stats().Accepted))
	emit(v2Descs.committed, prometheus.CounterValue, float64(s.CommittedPoints))
	emit(v2Descs.buffered, prometheus.GaugeValue, float64(s.BufferedPoints))
	emit(v2Descs.dispatch, prometheus.GaugeValue, float64(s.DispatchBytes))
	emit(v2Descs.seals, prometheus.CounterValue, float64(s.Seals))
	emit(v2Descs.sealTime, prometheus.CounterValue, float64(s.SealNanoseconds)/1e9)
	emit(v2Descs.sealLag, prometheus.GaugeValue, s.SealLagSeconds)
	emit(v2Descs.rollupLag, prometheus.GaugeValue, s.SealLagSeconds)
	emit(v2Descs.debt, prometheus.GaugeValue, float64(s.CompactionDebtBytes))
	emit(v2Descs.compacted, prometheus.CounterValue, float64(s.CompactedBytes))
	emit(v2Descs.obsolete, prometheus.GaugeValue, float64(s.ObsoleteBytes))
	emit(v2Descs.live, prometheus.GaugeValue, float64(s.LiveBytes))
	emit(v2Descs.dual, prometheus.GaugeValue, 0)
	emit(v2Descs.names, prometheus.GaugeValue, float64(v.catalog.Snapshot().Len()))
	for level, count := range s.Parts {
		ch <- prometheus.MustNewConstMetric(v2Descs.parts, prometheus.GaugeValue, float64(count), strconv.Itoa(level))
	}
	count, sum, buckets := v.reader.ReadObservation()
	ch <- prometheus.MustNewConstHistogram(v2Descs.reads, count, sum, buckets)
}
