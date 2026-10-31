package carbon

import (
	"strconv"
	"strings"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/persister"
	whisper "github.com/go-graphite/go-whisper"
	"github.com/prometheus/client_golang/prometheus"
)

// migrationCollector exports file conversion progress and shared-store health,
// read from the live migration, store and persister at scrape time. Migration
// counters restart with each migration run; rate() and increase() handle that.
type migrationCollector struct {
	app *App
}

type metricSpec struct {
	desc      *prometheus.Desc
	kind      prometheus.ValueType
	store     bool // reported only when a shared store is open
	persister bool // reported only when a persister runs
	migration bool // reported only while a migration run exists, including after it finished
	cache     bool // reported only when the write cache exists
	value     func(*migrationSnapshot) float64
}

type migrationSnapshot struct {
	sidecars                              store.SidecarInventory
	migration, running                    bool
	converted, points, reclaimed, resumed uint64
	skipped                               uint64
	inFlight, workers, maxWorkers         int
	failed, waitingForSpace               bool
	ioUsed, ioSlots                       int
	reclaimQueued                         int64
	reclaim                               persister.DeferredReclaimStats
	persister                             bool
	pebble, whisper                       persister.BackendWrites
	store                                 bool
	stats                                 storeStatsView
	workingReserved                       int64
	cache                                 bool
	cachePoints                           int64
	cacheMetrics                          int32
	cacheBytes                            int64
}

type storeStatsView struct {
	coldPack                                                                store.ColdPackStats
	ingestTables, ingestEntries, ingestBytes, ingestL0Bytes, ingestOverlaps uint64
	flushBytes, compactionWriteBytes, compactionReadBytes                   uint64
	diskBytes, compactionDebt, memTableBytes, walBytes                      uint64
	writeStalls                                                             uint64
	compactionRate, compactionPacerWaitNanos                                int64
	compactionPacerBytes                                                    uint64
	compactionPacerTargetRate                                               int64
	compactionPacerAuto, compactionPacerBypass                              bool
	compactionPacerBypasses                                                 map[string]uint64
	writeStallSeconds                                                       float64
	l0Files, l0Sublevels, compactionsInProgress                             int64
	cacheHits, cacheMisses                                                  uint64
	levelCompacted, levelRead                                               [7]uint64
	levelBytes                                                              [7]int64
	levelTargets                                                            [7]int64
	levelScores                                                             [7]float64
	relayout                                                                store.RelayoutStats
	rangeDeletes                                                            uint64
	invalidPoints                                                           uint64
	skippedBlockDeletes                                                     uint64
	unallocatedGroupSkips                                                   uint64
	compactOnceRunning                                                      bool
	purgeFound, purgeRemoved                                                uint64
	purgeRunning                                                            bool
	blockCacheBytes                                                         uint64
	tableCacheEntries                                                       int64
	stateCache                                                              store.StateCacheStats
	compactionKinds                                                         store.CompactionKinds
	fs                                                                      store.FSStats
	importFolds                                                             []importFoldCount
	fetch                                                                   store.FetchTimings
}

// importFoldCount is one result of whisperio's import sidecar folds.
type importFoldCount struct {
	result string
	count  uint64
}

// importSidecarFoldCounts snapshots the conversions' sidecar folds by result.
func importSidecarFoldCounts() []importFoldCount {
	counts := make([]importFoldCount, 0, len(whisper.InPlaceResults())+1)
	for _, result := range whisper.InPlaceResults() {
		counts = append(counts, importFoldCount{result.String(), whisperio.ImportSidecarFolds(result)})
	}
	return append(counts, importFoldCount{"error", whisperio.ImportSidecarFoldErrors()})
}

var migrationMetricSpecs = func() []metricSpec {
	spec := func(kind prometheus.ValueType, name, help string, v func(*migrationSnapshot) float64) metricSpec {
		return metricSpec{desc: prometheus.NewDesc(name, help, nil, nil), kind: kind, store: strings.HasPrefix(name, "pebble_") || strings.HasPrefix(name, "store_") && !strings.HasPrefix(name, "store_migration_"), value: v}
	}
	counter := func(name, help string, v func(*migrationSnapshot) float64) metricSpec {
		return spec(prometheus.CounterValue, name, help, v)
	}
	gauge := func(name, help string, v func(*migrationSnapshot) float64) metricSpec {
		return spec(prometheus.GaugeValue, name, help, v)
	}
	b := func(v bool) float64 {
		if v {
			return 1
		}
		return 0
	}
	specs := []metricSpec{
		gauge("store_ooo_sidecars", "Sidecars currently present; complete when store_ooo_inventory_ready is 1.", func(s *migrationSnapshot) float64 { return float64(s.sidecars.Count) }),
		gauge("store_ooo_sidecar_points", "Physical sidecar records, including expired slots.", func(s *migrationSnapshot) float64 { return float64(s.sidecars.Points) }),
		gauge("store_ooo_sidecar_oldest_timestamp_seconds", "Oldest physical sidecar sample timestamp, or 0 when empty.", func(s *migrationSnapshot) float64 { return float64(s.sidecars.Oldest) }),
		gauge("store_ooo_inventory_ready", "1 after the existing sidecar catalog has been fully indexed.", func(s *migrationSnapshot) float64 { return b(s.sidecars.Ready) }),
		counter("store_migration_converted_total", "Metrics converted or resumed by this migration run.", func(s *migrationSnapshot) float64 { return float64(s.converted) }),
		counter("store_migration_points_total", "Points read from converted originals.", func(s *migrationSnapshot) float64 { return float64(s.points) }),
		counter("store_migration_reclaimed_bytes_total", "Bytes of originals unlinked or archived.", func(s *migrationSnapshot) float64 { return float64(s.reclaimed) }),
		counter("store_migration_skipped_total", "Conversions skipped with the original kept, such as sources the shared format rejects.", func(s *migrationSnapshot) float64 { return float64(s.skipped) }),
		counter("store_migration_resumed_total", "Conversions resumed from a pending cleanup.", func(s *migrationSnapshot) float64 { return float64(s.resumed) }),
		gauge("store_migration_running", "1 while an incremental migration run is active.", func(s *migrationSnapshot) float64 { return b(s.running) }),
		gauge("store_migration_in_flight", "Conversions admitted and not finished.", func(s *migrationSnapshot) float64 { return float64(s.inFlight) }),
		gauge("store_migration_workers", "Current conversion worker limit from the control file.", func(s *migrationSnapshot) float64 { return float64(s.workers) }),
		gauge("store_migration_workers_max", "Configured conversion worker maximum.", func(s *migrationSnapshot) float64 { return float64(s.maxWorkers) }),
		gauge("store_migration_failed", "1 when the migration run stopped with an error.", func(s *migrationSnapshot) float64 { return b(s.failed) }),
		gauge("store_migration_waiting_for_disk_space", "1 while conversions wait for free-space headroom.", func(s *migrationSnapshot) float64 { return b(s.waitingForSpace) }),
		gauge("store_migration_io_slots_used", "Conversions inside file I/O.", func(s *migrationSnapshot) float64 { return float64(s.ioUsed) }),
		gauge("store_migration_io_slots", "Conversion file I/O slots.", func(s *migrationSnapshot) float64 { return float64(s.ioSlots) }),
		gauge("store_migration_reclaim_queued", "Converted originals queued or undergoing deferred cleanup.", func(s *migrationSnapshot) float64 { return float64(s.reclaimQueued) }),
		gauge("store_migration_reclaim_workers", "Deferred reclaim directory workers.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.Workers) }),
		counter("store_migration_reclaim_completed_total", "Deferred cleanups completed after directory sync; rate gives metrics reclaimed per second.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.Completed) }),
		counter("store_migration_reclaim_inline_fallback_total", "Conversions reclaiming inline because the deferred queue was full.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.InlineFallback) }),
		counter("store_migration_reclaim_batches_total", "Directory batches processed by deferred reclaim.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.Batches) }),
		counter("store_migration_reclaim_batch_jobs_total", "Jobs processed across directory batches; divide its rate by the batch rate for mean size.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.Jobs) }),
		counter("store_migration_reclaim_batch_seconds_total", "Seconds spent in directory batches, summed across workers; divide its rate by the batch rate for mean latency.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.BatchNanos) / 1e9 }),
		counter("store_migration_reclaim_unlink_seconds_total", "Seconds spent unlinking originals, summed across workers.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.UnlinkNanos) / 1e9 }),
		counter("store_migration_reclaim_sync_seconds_total", "Seconds spent syncing reclaimed directories, summed across workers.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.SyncNanos) / 1e9 }),
		counter("store_migration_reclaim_cleanup_seconds_total", "Seconds spent recording cleanup completion, summed across workers.", func(s *migrationSnapshot) float64 { return float64(s.reclaim.CleanupNanos) / 1e9 }),
		counter("pebble_ingest_tables_total", "Tables ingested for converted files.", func(s *migrationSnapshot) float64 { return float64(s.stats.ingestTables) }),
		counter("pebble_ingest_entries_total", "Converted metrics ingested as tables.", func(s *migrationSnapshot) float64 { return float64(s.stats.ingestEntries) }),
		counter("pebble_ingest_bytes_total", "Bytes of ingested conversion tables.", func(s *migrationSnapshot) float64 { return float64(s.stats.ingestBytes) }),
		counter("pebble_ingest_l0_bytes_total", "Ingested bytes placed in L0.", func(s *migrationSnapshot) float64 { return float64(s.stats.ingestL0Bytes) }),
		counter("pebble_ingest_memtable_overlaps_total", "Ingested tables overlapping a memtable.", func(s *migrationSnapshot) float64 { return float64(s.stats.ingestOverlaps) }),
		counter("pebble_flush_bytes_total", "Bytes written by memtable flushes.", func(s *migrationSnapshot) float64 { return float64(s.stats.flushBytes) }),
		counter("pebble_compaction_write_bytes_total", "Bytes written by compactions.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionWriteBytes) }),
		gauge("pebble_compaction_rate_bytes", "Compaction output budget in bytes per second; 0 disables pacing.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionRate) }),
		gauge("pebble_compaction_pacer_target_rate_bytes", "Compaction output target in bytes per second, including while bypassed.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionPacerTargetRate) }),
		gauge("pebble_compaction_pacer_auto", "1 when automatic compaction pacing is configured.", func(s *migrationSnapshot) float64 { return b(s.stats.compactionPacerAuto) }),
		gauge("pebble_compaction_pacer_bypass", "1 while compaction pacing is bypassed.", func(s *migrationSnapshot) float64 { return b(s.stats.compactionPacerBypass) }),
		counter("pebble_compaction_pacer_bytes_total", "Compaction output bytes written while pacing was enabled.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionPacerBytes) }),
		counter("pebble_compaction_pacer_wait_seconds_total", "Seconds compaction writers waited for tokens, summed across workers.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionPacerWaitNanos) / 1e9 }),
		counter("pebble_compaction_read_bytes_total", "Bytes read by compactions.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionReadBytes) }),
		counter("pebble_write_stalls_total", "Write stalls.", func(s *migrationSnapshot) float64 { return float64(s.stats.writeStalls) }),
		counter("pebble_write_stall_seconds_total", "Seconds writes were stalled.", func(s *migrationSnapshot) float64 { return s.stats.writeStallSeconds }),
		counter("pebble_block_cache_hits_total", "Block cache hits.", func(s *migrationSnapshot) float64 { return float64(s.stats.cacheHits) }),
		counter("pebble_block_cache_misses_total", "Block cache misses.", func(s *migrationSnapshot) float64 { return float64(s.stats.cacheMisses) }),
		gauge("pebble_wal_bytes", "Live WAL bytes, replayed on restart.", func(s *migrationSnapshot) float64 { return float64(s.stats.walBytes) }),
		gauge("pebble_disk_bytes", "Shared store disk usage.", func(s *migrationSnapshot) float64 { return float64(s.stats.diskBytes) }),
		gauge("pebble_compaction_debt_bytes", "Estimated compaction debt.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionDebt) }),
		gauge("pebble_memtable_bytes", "Memtable bytes, including queued memtables.", func(s *migrationSnapshot) float64 { return float64(s.stats.memTableBytes) }),
		gauge("pebble_l0_files", "L0 tables.", func(s *migrationSnapshot) float64 { return float64(s.stats.l0Files) }),
		gauge("pebble_l0_sublevels", "L0 sublevels.", func(s *migrationSnapshot) float64 { return float64(s.stats.l0Sublevels) }),
		gauge("pebble_compactions_in_progress", "Running compactions.", func(s *migrationSnapshot) float64 { return float64(s.stats.compactionsInProgress) }),
		counter("pebble_continuation_range_deletes_total", "Continuation prefix deletes written as range tombstones; each one makes the next memtable reader re-fragment every range tombstone in it.", func(s *migrationSnapshot) float64 { return float64(s.stats.rangeDeletes) }),
		counter("pebble_continuation_skipped_block_deletes_total", "Block rewrites that overwrote their old groups in place instead of deleting them (store-reuse-block-keys).", func(s *migrationSnapshot) float64 { return float64(s.stats.skippedBlockDeletes) }),
		counter("pebble_continuation_unallocated_group_skips_total", "Classic (OOO sidecar) group reads answered from the page allocation without a storage read.", func(s *migrationSnapshot) float64 { return float64(s.stats.unallocatedGroupSkips) }),
		gauge("pebble_compact_once_running", "1 while a store-compact-once full compaction runs.", func(s *migrationSnapshot) float64 { return b(s.stats.compactOnceRunning) }),
		counter("pebble_continuation_invalid_points_total", "Input points dropped because their timestamps do not fit a 32-bit Whisper timestamp, such as milliseconds sent as seconds.", func(s *migrationSnapshot) float64 { return float64(s.stats.invalidPoints) }),
		counter("store_relayout_metrics_total", "Metrics whose pre-v7 blocks the relayout moved to the history bucket.", func(s *migrationSnapshot) float64 { return float64(s.stats.relayout.Metrics) }),
		counter("store_relayout_blocks_total", "Pre-v7 blocks the relayout moved.", func(s *migrationSnapshot) float64 { return float64(s.stats.relayout.Blocks) }),
		counter("store_relayout_skipped_total", "Metrics the relayout skipped because they changed while it planned them.", func(s *migrationSnapshot) float64 { return float64(s.stats.relayout.Skipped) }),
		counter("store_relayout_bytes_total", "Bytes the relayout copied into the history bucket.", func(s *migrationSnapshot) float64 { return float64(s.stats.relayout.Bytes) }),
		gauge("store_relayout_rate_bytes", "Current relayout pacing in bytes per second; 0 when no relayout runs.", func(s *migrationSnapshot) float64 { return float64(s.stats.relayout.Rate) }),
		gauge("store_relayout_progress_ratio", "Relayout progress through the ID-ordered catalog (0-1).", func(s *migrationSnapshot) float64 { return relayoutProgress(s.stats.relayout) }),
		gauge("store_relayout_done", "1 once the relayout moved every pre-v7 block.", func(s *migrationSnapshot) float64 { return b(s.stats.relayout.Done) }),
		gauge("store_purge_invalid_names_found", "Catalog records under names too long for the store that the purge's scan found (store-purge-invalid-names).", func(s *migrationSnapshot) float64 { return float64(s.stats.purgeFound) }),
		counter("store_purge_invalid_names_removed_total", "Catalog records under names too long for the store that the purge removed.", func(s *migrationSnapshot) float64 { return float64(s.stats.purgeRemoved) }),
		gauge("store_purge_invalid_names_running", "1 while the purge of names too long for the store runs, including its final flush and compaction.", func(s *migrationSnapshot) float64 { return b(s.stats.purgeRunning) }),
		gauge("store_relayout_failed", "1 when the background relayout stopped with an error.", func(s *migrationSnapshot) float64 { return b(s.stats.relayout.Error != "") }),
	}
	for _, backend := range []struct {
		name string
		get  func(*migrationSnapshot) *persister.BackendWrites
	}{
		{"pebble", func(s *migrationSnapshot) *persister.BackendWrites { return &s.pebble }},
		{"whisper", func(s *migrationSnapshot) *persister.BackendWrites { return &s.whisper }},
	} {
		get := backend.get
		specs = append(specs,
			persisterSpec("persister_"+backend.name+"_updates_total", "Committed persister updates to the "+backend.name+" backend (Pebble store or original whisper files).", func(s *migrationSnapshot) float64 { return float64(get(s).Updates) }),
			persisterSpec("persister_"+backend.name+"_points_total", "Points committed to the "+backend.name+" backend.", func(s *migrationSnapshot) float64 { return float64(get(s).Points) }),
			persisterSpec("persister_"+backend.name+"_write_failures_total", "Failed persister updates to the "+backend.name+" backend.", func(s *migrationSnapshot) float64 { return float64(get(s).Failures) }),
			persisterSpec("persister_"+backend.name+"_write_seconds_total", "Seconds spent in successful "+backend.name+" backend updates.", func(s *migrationSnapshot) float64 { return get(s).Seconds }),
		)
	}
	specs = append(specs,
		counter("store_cold_pack_blocks_total", "Native ranges packed or unpacked.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.Blocks) }),
		counter("store_cold_pack_points_total", "Points rewritten by cold maintenance.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.Points) }),
		counter("store_cold_pack_input_bytes_total", "Logical source bytes replaced by sealing.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.InputBytes) }),
		counter("store_cold_pack_output_bytes_total", "Logical bytes after sealing.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.OutputBytes) }),
		counter("store_cold_pack_read_bytes_total", "Snapshot bytes read by cold maintenance.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.ReadBytes) }),
		counter("store_cold_pack_write_bytes_total", "WAL batch bytes published by cold maintenance.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.WriteBytes) }),
		counter("store_cold_pack_errors_total", "Cold maintenance errors; inspect server logs.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.Errors) }),
		counter("store_cold_pack_skipped_total", "Oversized ranges skipped by bounded maintenance.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.Skipped) }),
		counter("store_cold_pack_conflicts_total", "Publication conflicts retried on later sweeps.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.Conflicts) }),
		counter("store_cold_pack_pages_total", "Bounded catalog pages examined.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.Pages) }),
		counter("store_cold_pack_passes_total", "Completed catalog sweeps.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.Passes) }),
		counter("store_cold_pack_pressure_pauses_total", "Steps paused for storage pressure.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.PressurePauses) }),
		counter("store_cold_pack_worker_seconds_total", "Conservative worker time charged to the CPU budget.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.WorkerNanos) / 1e9 }),
		gauge("store_cold_pack_max_lock_seconds", "Maximum stripe publication lock duration in this process.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.MaxLockNanos) / 1e9 }),
		gauge("store_cold_pack_failed", "Latched background failure; inspect logs.", func(s *migrationSnapshot) float64 { return b(s.stats.coldPack.Error != "") }),
		gauge("store_cold_pack_pending_metrics", "Discovered catalog entries queued in the current bounded page; not a full-store backlog estimate.", func(s *migrationSnapshot) float64 { return float64(s.stats.coldPack.PendingMetrics) }),
		gauge("store_cold_pack_active", "Worker currently executing a bounded step.", func(s *migrationSnapshot) float64 { return b(s.stats.coldPack.Active) }),

		gauge("pebble_block_cache_bytes", "Bytes held by the Pebble block cache.", func(s *migrationSnapshot) float64 { return float64(s.stats.blockCacheBytes) }),
		gauge("pebble_table_cache_entries", "Open table readers in the Pebble table cache.", func(s *migrationSnapshot) float64 { return float64(s.stats.tableCacheEntries) }),
		gauge("store_metadata_cache_bytes", "Bytes of committed metric metadata held by the store's metadata cache, records and slots, which store-metadata-cache-size bounds.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Bytes) }),
		gauge("store_metadata_cache_entries", "Metrics with an entry in the store's metadata cache, including ones pending a mutation.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Entries) }),
		gauge("store_metadata_cache_arena_bytes", "Memory the store's metadata cache holds: record chunks, including dead records awaiting compaction and unwritten chunk space, plus slot arrays and indexes.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.ArenaBytes) }),
		counter("store_metadata_cache_compacted_bytes_total", "Record bytes the metadata cache's compaction moved to reclaim dead records.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.CompactedBytes) }),
		counter("store_metadata_cache_hits_total", "Metadata, update and fetch lookups that found cached metric metadata.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Hits) }),
		counter("store_metadata_cache_misses_total", "Cache lookups without usable metric metadata, including missing metrics and reader races.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Misses) }),
		counter("store_metadata_cache_reader_misses_total", "Fetch lookups without usable cached metadata.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.ReaderMisses) }),
		counter("store_metadata_cache_locked_misses_total", "Update and Metadata lookups without cached metadata under the metric lock.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.LockedMisses) }),
		counter("store_metadata_cache_pending_misses_total", "Reader misses while the metric has a pending mutation.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.PendingMisses) }),
		counter("store_metadata_cache_stale_misses_total", "Reader misses because cached metadata is newer than the read view.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.StaleMisses) }),
		counter("store_metadata_cache_invalidations_total", "Existing states dropped by mutations without a write-through install.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Invalidations) }),
		counter("store_metadata_cache_oversized_total", "Write-through states refused because one state exceeds its shard budget.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Oversized) }),
		counter("store_metadata_cache_tail_refusals_total", "Updates and fills that dropped optional cached tails to preserve room for metric metadata.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.SlotRefusals) }),
		counter("store_metadata_cache_installs_total", "Metric states an update committed and wrote through to the metadata cache.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Installs) }),
		counter("store_metadata_cache_fills_total", "Metric states a fetch read and added to the metadata cache.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Fills) }),
		counter("store_metadata_cache_evictions_total", "Metric states the metadata cache evicted to stay within its size.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Evictions) }),
		counter("store_metadata_cache_prewarmed_total", "Metric states the start's prewarm loaded into the metadata cache (store-metadata-cache-prewarm).", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.Prewarmed) }),
		counter("store_metadata_cache_prewarm_bytes_total", "Committed state bytes installed by metadata prewarms.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.PrewarmBytes) }),
		counter("store_metadata_cache_prewarm_read_bytes_total", "Logical key/value bytes read by metadata prewarms, including refused fills.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.PrewarmReadBytes) }),
		counter("store_metadata_cache_prewarm_duration_seconds_total", "Wall time of metadata prewarms including pacing.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.PrewarmNanos) / 1e9 }),
		counter("store_metadata_cache_prewarm_skipped_total", "Metrics the start's prewarm passed over: cached or pending already, not in the catalog, changed while read, or no room left.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.PrewarmSkipped) }),
		counter("store_metadata_cache_prewarm_failed_total", "Metrics the start's prewarm could not read.", func(s *migrationSnapshot) float64 { return float64(s.stats.stateCache.PrewarmFailed) }),
		metricSpec{desc: prometheus.NewDesc("store_migration_working_bytes_reserved", "Conversion working bytes currently reserved.", nil, nil), kind: prometheus.GaugeValue, migration: true,
			value: func(s *migrationSnapshot) float64 { return float64(s.workingReserved) }},
		metricSpec{desc: prometheus.NewDesc("cache_points", "Points held in the write cache.", nil, nil), kind: prometheus.GaugeValue, cache: true,
			value: func(s *migrationSnapshot) float64 { return float64(s.cachePoints) }},
		metricSpec{desc: prometheus.NewDesc("cache_metrics", "Metrics with points in the write cache.", nil, nil), kind: prometheus.GaugeValue, cache: true,
			value: func(s *migrationSnapshot) float64 { return float64(s.cacheMetrics) }},
		metricSpec{desc: prometheus.NewDesc("cache_estimated_bytes", "Estimated owned write-cache memory: names, items, buffer capacities, indexes, in-flight points, input-log tracking and queue scratch. Excludes mapped recovery files and the input-log encoder.", nil, nil), kind: prometheus.GaugeValue, cache: true,
			value: func(s *migrationSnapshot) float64 { return float64(s.cacheBytes) }},
	)
	return specs
}()

// Pebble compactions by kind and filesystem operations by file kind.
var (
	pebbleCompactionsDesc        = prometheus.NewDesc("pebble_compactions_total", "Completed Pebble compactions by kind; multi_level is a subset of default.", []string{"kind"}, nil)
	pebbleCompactionBypassesDesc = prometheus.NewDesc("pebble_compaction_pacer_bypasses_total", "Compaction pacing bypass entries by reason, including changes between reasons.", []string{"reason"}, nil)
	pebbleFSOpsDesc              = prometheus.NewDesc("pebble_fs_ops_total", "Store filesystem operations by operation and file kind. Table writes cover flushes and compactions; table reads cover compactions and point reads, which Pebble does not tell apart.", []string{"op", "kind"}, nil)
	pebbleFSBytesDesc            = prometheus.NewDesc("pebble_fs_bytes_total", "Store filesystem bytes read and written by file kind.", []string{"op", "kind"}, nil)
	pebbleFetchStageSecondsDesc  = prometheus.NewDesc("pebble_fetch_stage_seconds_total", "Storage fetch wall time by stage; iterator_close covers the final shared continuation iterator and is included in body. Fetches without a read view are excluded.", []string{"stage"}, nil)
	pebbleFetchStageCallsDesc    = prometheus.NewDesc("pebble_fetch_stage_calls_total", "Measured storage fetch stage calls; iterator_close counts only final shared continuation iterators.", []string{"stage"}, nil)
)

// storeMigrationSidecarFoldsDesc counts converted files with an out-of-order
// sidecar by how the import folded it.
var storeMigrationSidecarFoldsDesc = prometheus.NewDesc("store_migration_sidecar_folds_total",
	"Converted files whose sidecar was folded, by in-place decision or full-rewrite reason; error means conversion failed.",
	[]string{"result"}, nil)

// Persister stage timing: summed work per stage (stagetime), the update
// duration histogram and the slowest update per scrape.
var (
	persisterStageSecondsDesc = prometheus.NewDesc("persister_stage_seconds_total", "Seconds persister updates spent in each stage, summed across workers (stages other than total are sampled and scaled).", []string{"stage"}, nil)
	persisterStageCallsDesc   = prometheus.NewDesc("persister_stage_calls_total", "Calls of each persister update stage (stages other than total are sampled and scaled).", []string{"stage"}, nil)
	persisterUpdateDesc       = prometheus.NewDesc("persister_update_duration_seconds", "Wall time of persister updates (metric writeouts).", nil, nil)
	persisterUpdateMaxDesc    = prometheus.NewDesc("persister_update_seconds_max", "Slowest persister update since the previous scrape.", nil, nil)
)

// Write amplification differs by LSM level; these locate it.
var (
	storeLevelCompactionDesc = prometheus.NewDesc("pebble_level_compaction_write_bytes_total", "Bytes compactions wrote into each LSM level.", []string{"level"}, nil)
	pebbleLevelReadDesc      = prometheus.NewDesc("pebble_level_compaction_read_bytes_total", "Bytes read by compactions into each LSM level: inputs from the level above plus the level's own overlapping tables.", []string{"level"}, nil)
	storeLevelBytesDesc      = prometheus.NewDesc("pebble_level_bytes", "Size of each LSM level.", []string{"level"}, nil)
	pebbleLevelTargetDesc    = prometheus.NewDesc("pebble_level_target_bytes", "Size target of each LSM level from Pebble's dynamic level sizing; 0 for L0 and levels above the base level.", []string{"level"}, nil)
	pebbleLevelScoreDesc     = prometheus.NewDesc("pebble_level_score", "Pebble compaction score of each LSM level: size over target, divided by the next level's score; compactions pick the highest score at or above 1.", []string{"level"}, nil)
)

func relayoutProgress(r store.RelayoutStats) float64 {
	switch {
	case r.Done:
		return 1
	case r.Ceiling == 0:
		return 0
	}
	return min(1, float64(r.Cursor)/float64(r.Ceiling))
}

func persisterSpec(name, help string, v func(*migrationSnapshot) float64) metricSpec {
	return metricSpec{desc: prometheus.NewDesc(name, help, nil, nil), kind: prometheus.CounterValue, persister: true, value: v}
}

func (c *migrationCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, spec := range migrationMetricSpecs {
		ch <- spec.desc
	}
	ch <- storeLevelCompactionDesc
	ch <- pebbleLevelReadDesc
	ch <- storeLevelBytesDesc
	ch <- pebbleLevelTargetDesc
	ch <- pebbleLevelScoreDesc
	ch <- pebbleCompactionsDesc
	ch <- pebbleCompactionBypassesDesc
	ch <- pebbleFSOpsDesc
	ch <- pebbleFSBytesDesc
	ch <- pebbleFetchStageSecondsDesc
	ch <- pebbleFetchStageCallsDesc
	ch <- storeMigrationSidecarFoldsDesc
	ch <- persisterStageSecondsDesc
	ch <- persisterStageCallsDesc
	ch <- persisterUpdateDesc
	ch <- persisterUpdateMaxDesc
}

func (c *migrationCollector) Collect(ch chan<- prometheus.Metric) {
	var s migrationSnapshot
	c.app.RLock()
	db, p, cache := c.app.MetricStore, c.app.Persister, c.app.Cache
	c.app.RUnlock()
	if m := c.app.incrementalMigration.Load(); m != nil {
		s.migration = true
		s.converted, s.points, s.resumed = m.metrics.Load(), m.points.Load(), m.resumed.Load()
		s.skipped = m.skipped.Load()
		s.reclaimed = m.unlinked.Load() + m.archived.Load()
		s.maxWorkers, s.workers, s.inFlight = m.maximum, m.maximum, int(m.active.Load())
		if m.budget != nil {
			s.workers, s.inFlight, _ = m.budget.status()
		}
		m.mu.Lock()
		s.failed = m.failure != ""
		// The run object outlives the run: after it ends its counters stay
		// reported, but nothing is converting any more.
		s.running = m.phase != "shared" && m.phase != "cancelled" && m.phase != "failed"
		m.mu.Unlock()
		s.waitingForSpace = m.spaceWaiting.Load() > 0
		s.workingReserved = m.working.Load()
	}
	if p != nil {
		s.persister = true
		s.ioUsed, s.ioSlots = p.ConversionIOSlots()
		s.reclaimQueued = p.DeferredReclaimQueued()
		s.reclaim = p.DeferredReclaimStats()
		s.pebble, s.whisper = p.WriteStats()
	}
	if cache != nil {
		s.cache, s.cachePoints, s.cacheMetrics = true, cache.Size(), cache.Len()
		s.cacheBytes = cache.EstimatedBytes()
	}
	if db != nil {
		st := db.Stats()
		s.store = true
		s.sidecars = db.Sidecars()
		s.stats = storeStatsView{coldPack: st.ColdPack,
			ingestTables: st.IngestTables, ingestEntries: st.IngestEntries, ingestBytes: st.IngestBytes,
			ingestL0Bytes: st.IngestL0Bytes, ingestOverlaps: st.IngestMemtableOverlaps,
			flushBytes: st.FlushWriteBytes, compactionWriteBytes: st.CompactionWriteBytes, compactionReadBytes: st.CompactionReadBytes,
			diskBytes: st.DiskBytes, compactionDebt: st.CompactionDebt, memTableBytes: st.MemTableBytes, walBytes: st.WALBytes,
			compactionRate: st.CompactionRate, compactionPacerBytes: st.CompactionPacerBytes, compactionPacerWaitNanos: st.CompactionPacerWaitNanos,
			compactionPacerTargetRate: st.CompactionPacerTargetRate, compactionPacerAuto: st.CompactionPacerAuto, compactionPacerBypass: st.CompactionPacerBypass != "", compactionPacerBypasses: st.CompactionPacerBypasses,
			writeStalls: st.WriteStalls, writeStallSeconds: float64(st.WriteStallNanos) / 1e9,
			l0Files: st.L0Files, l0Sublevels: st.L0Sublevels, compactionsInProgress: st.CompactionsInProgress,
			cacheHits: st.CacheHits, cacheMisses: st.CacheMisses,
			levelCompacted: st.LevelCompactedBytes, levelRead: st.LevelReadBytes, levelBytes: st.LevelBytes, levelTargets: st.LevelTargetBytes, levelScores: st.LevelScores,
			relayout: db.RelayoutStats(), rangeDeletes: st.ContinuationRangeDeletes, invalidPoints: whisperio.InvalidContinuationPoints(), skippedBlockDeletes: whisperio.SkippedBlockDeletes(), unallocatedGroupSkips: whisperio.UnallocatedGroupSkips(), compactOnceRunning: st.CompactOnceRunning,
			purgeFound: st.PurgeFound, purgeRemoved: st.PurgeRemoved, purgeRunning: st.PurgeRunning,
			blockCacheBytes: st.CacheBytes, tableCacheEntries: st.TableCacheEntries, stateCache: st.StateCache,
			compactionKinds: st.CompactionKinds, fs: st.FS, importFolds: importSidecarFoldCounts(), fetch: db.FetchTimings(),
		}
	}
	for _, spec := range migrationMetricSpecs {
		if spec.store && !s.store || spec.persister && !s.persister || spec.migration && !s.migration || spec.cache && !s.cache {
			continue
		}
		ch <- prometheus.MustNewConstMetric(spec.desc, spec.kind, spec.value(&s))
	}
	if s.persister {
		for _, stage := range stagetime.Snapshot() {
			name := stage.Stage.String()
			ch <- prometheus.MustNewConstMetric(persisterStageSecondsDesc, prometheus.CounterValue, stage.Seconds, name)
			ch <- prometheus.MustNewConstMetric(persisterStageCallsDesc, prometheus.CounterValue, float64(stage.Calls), name)
		}
		buckets, count, sum := stagetime.UpdateHistogram()
		ch <- prometheus.MustNewConstHistogram(persisterUpdateDesc, count, sum, buckets)
		ch <- prometheus.MustNewConstMetric(persisterUpdateMaxDesc, prometheus.GaugeValue, stagetime.TakeUpdateMax().Seconds())
	}
	if s.store {
		for _, stage := range []struct {
			name   string
			timing store.FetchTiming
		}{{"body", s.stats.fetch.Body}, {"read_view_close", s.stats.fetch.ReadViewClose}, {"iterator_close", s.stats.fetch.IteratorClose}} {
			ch <- prometheus.MustNewConstMetric(pebbleFetchStageSecondsDesc, prometheus.CounterValue, float64(stage.timing.Nanoseconds)/1e9, stage.name)
			ch <- prometheus.MustNewConstMetric(pebbleFetchStageCallsDesc, prometheus.CounterValue, float64(stage.timing.Calls), stage.name)
		}
		for reason, count := range s.stats.compactionPacerBypasses {
			ch <- prometheus.MustNewConstMetric(pebbleCompactionBypassesDesc, prometheus.CounterValue, float64(count), reason)
		}
		k := s.stats.compactionKinds
		for _, kind := range []struct {
			name  string
			count uint64
		}{{"default", k.Default}, {"delete_only", k.DeleteOnly}, {"elision_only", k.ElisionOnly}, {"move", k.Move}, {"read", k.Read}, {"rewrite", k.Rewrite}, {"multi_level", k.MultiLevel}} {
			ch <- prometheus.MustNewConstMetric(pebbleCompactionsDesc, prometheus.CounterValue, float64(kind.count), kind.name)
		}
		for _, fold := range s.stats.importFolds {
			ch <- prometheus.MustNewConstMetric(storeMigrationSidecarFoldsDesc, prometheus.CounterValue, float64(fold.count), fold.result)
		}
		for op, opName := range store.FSOpNames {
			for kind, kindName := range store.FSKindNames {
				ch <- prometheus.MustNewConstMetric(pebbleFSOpsDesc, prometheus.CounterValue, float64(s.stats.fs.Ops[op][kind]), opName, kindName)
				if op == store.FSRead || op == store.FSWrite {
					ch <- prometheus.MustNewConstMetric(pebbleFSBytesDesc, prometheus.CounterValue, float64(s.stats.fs.Bytes[op][kind]), opName, kindName)
				}
			}
		}
		for level := range s.stats.levelCompacted {
			l := strconv.Itoa(level)
			ch <- prometheus.MustNewConstMetric(storeLevelCompactionDesc, prometheus.CounterValue, float64(s.stats.levelCompacted[level]), l)
			ch <- prometheus.MustNewConstMetric(pebbleLevelReadDesc, prometheus.CounterValue, float64(s.stats.levelRead[level]), l)
			ch <- prometheus.MustNewConstMetric(storeLevelBytesDesc, prometheus.GaugeValue, float64(s.stats.levelBytes[level]), l)
			ch <- prometheus.MustNewConstMetric(pebbleLevelTargetDesc, prometheus.GaugeValue, float64(s.stats.levelTargets[level]), l)
			ch <- prometheus.MustNewConstMetric(pebbleLevelScoreDesc, prometheus.GaugeValue, s.stats.levelScores[level], l)
		}
	}
}
