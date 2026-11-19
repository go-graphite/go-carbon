package carbon

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-graphite/go-carbon/helper"
)

// App's lifecycle lock keeps the engine open during collection. No producer
// lock, name scan or data read is needed, even with a large catalog.
type v2Collector struct{ app *App }

var v2MergeDescs = struct{ live, pressure, runs *prometheus.Desc }{
	prometheus.NewDesc("v2_sealed_parts", "Catalog-live sealed parts, excluding retired originals waiting for GC.", nil, nil),
	prometheus.NewDesc("v2_merge_over_budget_seals_total", "Seals admitted while owner inventory exceeds the soft target of 32 parts; intake continues.", nil, nil),
	prometheus.NewDesc("v2_merge_runs_total", "Completed atomic sealed-part replacements.", nil, nil),
}

var v2GCDescs = struct{ pending, removed, unlinked *prometheus.Desc }{
	prometheus.NewDesc("v2_part_gc_pending_parts", "Retired sealed parts awaiting original-link removal.", nil, nil),
	prometheus.NewDesc("v2_part_gc_removed_parts_total", "Retired parts whose original links were removed in this process.", nil, nil),
	prometheus.NewDesc("v2_part_gc_unlinked_bytes_total", "Bytes in removed original part links; hardlinks and pinned SSTs can retain physical space.", nil, nil),
}

var v2Descs = struct {
	ingest, committed, buffered, dispatch, seals, sealTime, sealLag                   *prometheus.Desc
	durableSeals, sealNames, sealPoints                                               *prometheus.Desc
	parts, debt, compacted, obsolete, live, dual, reads, names, rollupLag, readStages *prometheus.Desc
}{
	ingest:       prometheus.NewDesc("v2_ingest_points_total", "Points accepted by the v2 TCP receiver; use rate() for points per second.", nil, nil),
	committed:    prometheus.NewDesc("v2_committed_points_total", "Points durably sealed by this v2 process, including journal replay.", nil, nil),
	buffered:     prometheus.NewDesc("v2_buffered_points", "Points in owner heads awaiting a durable seal; excludes receiver and dispatch queues.", nil, nil),
	dispatch:     prometheus.NewDesc("v2_dispatch_bytes", "Bytes held by bounded producer dispatch leases.", nil, nil),
	seals:        prometheus.NewDesc("v2_seals_total", "Owner seal attempts, including failed attempts.", nil, nil),
	durableSeals: prometheus.NewDesc("v2_durable_seals_total", "Owner seals that became durable; the denominator for names and points per seal.", nil, nil),
	sealNames:    prometheus.NewDesc("v2_seal_names_total", "Names with points in durable owner seals, excluding idle rollup closures; divide by v2_durable_seals_total for names per seal.", nil, nil),
	sealPoints:   prometheus.NewDesc("v2_seal_points_total", "Points in durable owner seals; divide by v2_durable_seals_total for points per seal, or by v2_seal_names_total for points per name.", nil, nil),
	sealTime:     prometheus.NewDesc("v2_seal_seconds_total", "Time spent sorting, reducing and durably writing owner seals, summed across owners.", nil, nil),
	sealLag:      prometheus.NewDesc("v2_seal_lag_seconds", "Maximum age of an unsealed owner head; excludes time in receiver and dispatch queues. Rollups share this seal boundary.", nil, nil),
	rollupLag:    prometheus.NewDesc("v2_rollup_lag_seconds", "Age of the oldest unsealed owner head. Rollups are committed in that same seal; excludes upstream queues.", nil, nil),
	parts:        prometheus.NewDesc("v2_parts", "Current physical SST files by LSM level; not logical archive parts.", []string{"level"}, nil),
	debt:         prometheus.NewDesc("v2_compaction_debt_bytes", "Estimated LSM compaction work remaining in bytes.", nil, nil),
	compacted:    prometheus.NewDesc("v2_compacted_bytes_total", "Bytes written by LSM compactions in this process.", nil, nil),
	obsolete:     prometheus.NewDesc("v2_obsolete_bytes", "Obsolete SST bytes eligible for deletion; excludes reader-pinned zombie tables.", nil, nil),
	live:         prometheus.NewDesc("v2_live_sst_bytes", "Bytes in live SST files across all levels.", nil, nil),
	dual:         prometheus.NewDesc("v2_dual_write_enabled", "One for dual writes. The current v2 runtime is a sole writer and reports zero.", nil, nil),
	readStages:   prometheus.NewDesc("v2_read_stage_duration_seconds", "Render request stage wall time; per-owner stages are observed once per owner call.", []string{"stage"}, nil),
	reads:        prometheus.NewDesc("v2_read_duration_seconds", "FetchMany wall time including errors, cancellation and consumer callbacks.", nil, nil),
	names:        prometheus.NewDesc("v2_catalog_names", "Names in the latest immutable discovery view, including classic union names.", nil, nil),
}

var v2CatalogDescs = struct{ baseErrors, baseHeap *prometheus.Desc }{
	prometheus.NewDesc("v2_catalog_base_file_errors_total", "Failed writes of the discovery index base file; the base stays on the heap until a write succeeds.", nil, nil),
	prometheus.NewDesc("v2_catalog_base_heap_bytes", "Bytes of the discovery index base held on the heap because its file write failed.", nil, nil),
}

// Stat reports base file failures to the stats collector.
func (c *v2PublishedCatalog) Stat(send helper.StatCallback) {
	failures, heap := c.BaseFileStats()
	send("baseFileErrors", float64(failures))
	send("baseHeapBytes", float64(heap))
}

func (*v2Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- v2MoveDesc
	ch <- v2MoveOwnerDesc
	ch <- v2MoveRegressionDesc
	for _, d := range []*prometheus.Desc{v2GCDescs.pending, v2GCDescs.removed, v2GCDescs.unlinked, v2MergeDescs.live, v2MergeDescs.pressure, v2MergeDescs.runs, v2CatalogDescs.baseErrors, v2CatalogDescs.baseHeap} {
		ch <- d
	}
	for _, d := range []*prometheus.Desc{v2Descs.ingest, v2Descs.committed, v2Descs.buffered, v2Descs.dispatch, v2Descs.seals, v2Descs.durableSeals, v2Descs.sealNames, v2Descs.sealPoints, v2Descs.sealTime, v2Descs.sealLag, v2Descs.parts, v2Descs.debt, v2Descs.compacted, v2Descs.obsolete, v2Descs.live, v2Descs.dual, v2Descs.reads, v2Descs.readStages, v2Descs.names, v2Descs.rollupLag} {
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
	var pendingGC, removedGC, unlinkedGC, liveParts, pressure, runs uint64
	for _, gc := range v.store.Core.PartGCStats() {
		liveParts += gc.LiveParts
		pressure += gc.MergeOverBudgetSeals
		runs += gc.MergeRuns
		pendingGC += gc.PendingParts
		removedGC += gc.RemovedParts
		unlinkedGC += gc.UnlinkedFileBytes
	}
	emit(v2MergeDescs.live, prometheus.GaugeValue, float64(liveParts))
	emit(v2MergeDescs.pressure, prometheus.CounterValue, float64(pressure))
	emit(v2MergeDescs.runs, prometheus.CounterValue, float64(runs))
	emit(v2GCDescs.pending, prometheus.GaugeValue, float64(pendingGC))
	emit(v2GCDescs.removed, prometheus.CounterValue, float64(removedGC))
	emit(v2GCDescs.unlinked, prometheus.CounterValue, float64(unlinkedGC))
	emit(v2Descs.ingest, prometheus.CounterValue, float64(v.server.Stats().Accepted))
	emit(v2Descs.committed, prometheus.CounterValue, float64(s.CommittedPoints))
	emit(v2Descs.buffered, prometheus.GaugeValue, float64(s.BufferedPoints))
	emit(v2Descs.dispatch, prometheus.GaugeValue, float64(s.DispatchBytes))
	emit(v2Descs.seals, prometheus.CounterValue, float64(s.Seals))
	emit(v2Descs.durableSeals, prometheus.CounterValue, float64(s.DurableSeals))
	emit(v2Descs.sealNames, prometheus.CounterValue, float64(s.SealNames))
	emit(v2Descs.sealPoints, prometheus.CounterValue, float64(s.SealPoints))
	emit(v2Descs.sealTime, prometheus.CounterValue, float64(s.SealNanoseconds)/1e9)
	emit(v2Descs.sealLag, prometheus.GaugeValue, s.SealLagSeconds)
	emit(v2Descs.rollupLag, prometheus.GaugeValue, s.SealLagSeconds)
	emit(v2Descs.debt, prometheus.GaugeValue, float64(s.CompactionDebtBytes))
	emit(v2Descs.compacted, prometheus.CounterValue, float64(s.CompactedBytes))
	emit(v2Descs.obsolete, prometheus.GaugeValue, float64(s.ObsoleteBytes))
	emit(v2Descs.live, prometheus.GaugeValue, float64(s.LiveBytes))
	emit(v2Descs.dual, prometheus.GaugeValue, 0)
	emit(v2Descs.names, prometheus.GaugeValue, float64(v.catalog.Catalog.Snapshot().Len()))
	failures, heap := v.catalog.BaseFileStats()
	emit(v2CatalogDescs.baseErrors, prometheus.CounterValue, float64(failures))
	emit(v2CatalogDescs.baseHeap, prometheus.GaugeValue, float64(heap))
	for level, count := range s.Parts {
		ch <- prometheus.MustNewConstMetric(v2Descs.parts, prometheus.GaugeValue, float64(count), strconv.Itoa(level))
	}
	count, sum, buckets := v.reader.ReadObservation()
	ch <- prometheus.MustNewConstHistogram(v2Descs.reads, count, sum, buckets)
	for _, o := range v.reader.StageObservations() {
		ch <- prometheus.MustNewConstHistogram(v2Descs.readStages, o.Count, o.Sum, o.Buckets, o.Name)
	}
}
