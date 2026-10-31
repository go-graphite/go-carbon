package carbon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/storecopy"
)

const maxStoreMigrationWorkers = 65536

func (app *App) configureStoreCopy() error {
	cfg := app.Config
	ready := func() error {
		if !app.TryLock() {
			return errors.New("application lifecycle is busy; retry checkpoint")
		}
		defer app.Unlock()
		current := app.Config
		if app.copyInputSealed {
			return errors.New("input was sealed; restart before creating another checkpoint")
		}
		if current.Tags.Enabled || current.Whisper.StoreMirrorFiles || current.Whisper.StoreLiveMigration ||
			current.Whisper.StoreExpiration.Value() != 0 || current.Whisper.StoreExpirationFilename != "" || len(current.Whisper.ExpirationRules) != 0 {
			return errors.New("store copy requires completed shared storage, disabled tags, expiration and rollback mirroring")
		}
		if current.Whisper.SchemasFilename != cfg.Whisper.SchemasFilename || current.Whisper.AggregationFilename != cfg.Whisper.AggregationFilename || current.Whisper.QuotasFilename != cfg.Whisper.QuotasFilename {
			return errors.New("policy paths changed since startup; restart before checkpointing")
		}
		if app.storeRestoreDone != nil {
			select {
			case <-app.storeRestoreDone:
			default:
				return errors.New("store recovery has not finished")
			}
		}
		if m := app.incrementalMigration.Load(); m != nil && m.receipt()["phase"] != "shared" {
			return errors.New("incremental conversion has not finished")
		}
		return checkStoreConversion(sharedStorePath(cfg))
	}
	seal := func() error {
		// Stop may be waiting for this admin handler while holding App's lock.
		// Refuse a concurrent lifecycle operation instead of waiting on it.
		if !app.TryLock() {
			return errors.New("application lifecycle is busy; retry sealing")
		}
		defer app.Unlock()
		app.stopInputListeners()
		app.copyInputSealed = true
		if app.Collector != nil {
			app.Collector.Stop()
			app.Collector = nil
		}
		return nil
	}
	manager := storecopy.New(app.MetricStore, app.inputLog.Load(), cfg.InputLog.Path, sharedStorePath(cfg), ready, seal)
	manager.SetMetadata(map[string]any{"node": cfg.Buckyd.Node, "hash": cfg.Buckyd.Hash, "replicas": cfg.Buckyd.Replicas, "nodes": cfg.Buckyd.Nodes,
		"store_dir": sharedStorePath(cfg), "data_dir": cfg.Whisper.DataDir, "encoding": cfg.Whisper.StoreEncoding,
		"incremental": cfg.Whisper.StoreIncrementalMigration, "time_buckets": cfg.Whisper.StoreTimeBuckets},
		map[string]string{"storage-schemas.conf": cfg.Whisper.SchemasFilename, "storage-aggregation.conf": cfg.Whisper.AggregationFilename, "storage-quotas.conf": cfg.Whisper.QuotasFilename})
	app.Buckyd.SetStoreCopy(manager)
	return manager.CleanupOrphans()
}

func checkStoreConversion(dir string) error {
	for _, marker := range []string{"CONVERSION-IN-PROGRESS", "NEEDS-CATCHUP", "MIRROR-RECONCILE-REQUIRED"} {
		_, err := os.Stat(filepath.Join(dir, marker))
		if err == nil {
			return fmt.Errorf("shared store is not ready for serving: %s", marker)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// Mirrored stores are deliberately single-session canaries. A failed or partial
// write can leave the original ahead of the shared copy; even a clean stop does
// not authorize a later shared-store restart after stock has accepted writes.
// Only a reconciler may retire this guard after checking the current original.
func armMirrorGuard(dir string) error {
	f, err := os.OpenFile(filepath.Join(dir, "MIRROR-RECONCILE-REQUIRED"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("arm rollback mirror guard: %w", err)
	}
	_, writeErr := f.WriteString("Reconcile against the current original Whisper tree before serving this shared store again.\n")
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func validateMirrorPaths(source, destination string) error {
	resolve := func(path string) (string, error) {
		if !filepath.IsAbs(path) {
			return "", errors.New("rollback mirroring requires absolute data-dir and store-dir")
		}
		ancestor := filepath.Clean(path)
		for {
			_, err := os.Lstat(ancestor)
			if err == nil {
				break
			}
			if !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
			ancestor = filepath.Dir(ancestor)
		}
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err != nil {
			return "", err
		}
		rel, err := filepath.Rel(ancestor, path)
		if err != nil {
			return "", err
		}
		return filepath.Join(resolved, rel), nil
	}
	a, err := resolve(source)
	if err != nil {
		return err
	}
	b, err := resolve(destination)
	if err != nil {
		return err
	}
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return err
		}
		if rel == "." || filepath.IsLocal(rel) {
			return errors.New("rollback mirror data-dir and store-dir must be disjoint physical trees")
		}
	}
	return nil
}

func sharedStorePath(cfg *Config) string {
	if cfg.Whisper.StoreDir != "" {
		return cfg.Whisper.StoreDir
	}
	if cfg.Whisper.StoreEncoding == "exact" {
		return filepath.Join(cfg.Whisper.DataDir, ".store-exact-blocks")
	}
	return filepath.Join(cfg.Whisper.DataDir, ".store-chunks")
}

func validateStorageConfig(cfg *Config) error {
	cold, err := store.NormalizeColdPack(coldPackOptions(cfg))
	if err != nil {
		return err
	}
	if cold.Mode != "off" && (cfg.Whisper.StorageBackend != "pebble-chunk" || cfg.Whisper.StoreEncoding != "exact" || !cfg.Whisper.StoreIncrementalMigration || !cfg.Whisper.StoreTimeBuckets) {
		return errors.New("cold pack requires exact incremental time-bucketed storage")
	}

	if archive := cfg.Whisper.StoreMigrationArchiveDir; archive != "" {
		if !cfg.Whisper.StoreIncrementalMigration {
			return errors.New("store-migration-archive-dir requires incremental migration")
		}
		for _, root := range []string{cfg.Whisper.DataDir, sharedStorePath(cfg)} {
			if err := validateMirrorPaths(root, archive); err != nil {
				return fmt.Errorf("original archive must be disjoint from active source and shared storage: %w", err)
			}
		}
	}
	if cfg.Whisper.StoreIncrementalMigration {
		if cfg.Whisper.StorageBackend != "pebble-chunk" || cfg.Whisper.StoreEncoding != "exact" || !cfg.Whisper.Enabled || !cfg.Whisper.FLock || cfg.Whisper.StoreDir == "" || cfg.Whisper.StoreMirrorFiles || cfg.Whisper.StoreLiveMigration || cfg.Tags.Enabled || !cfg.Carbonserver.Enabled || !cfg.Carbonserver.TrieIndex || cfg.Carbonserver.FileListCache == "" || cfg.Carbonserver.FileListCacheVersion != 2 {
			return errors.New("incremental migration requires exact storage, enabled flocked persister, trie carbonserver with file-list cache, explicit disjoint store-dir, and no rollback mirror, whole-store migration or tags")
		}
		if cfg.Whisper.StoreMigrationWorkers < 0 || cfg.Whisper.StoreMigrationWorkers > maxStoreMigrationWorkers || cfg.Whisper.StoreMigrationBytesInFlight <= 0 || cfg.Whisper.StoreMigrationMinFreeBytes < 0 {
			return fmt.Errorf("incremental migration requires 0..%d workers, positive bytes-in-flight and nonnegative free-space reserve", maxStoreMigrationWorkers)
		}
		if err := validateMirrorPaths(cfg.Whisper.DataDir, cfg.Whisper.StoreDir); err != nil {
			return err
		}
	}

	if path := cfg.Whisper.StoreMigrationControlFile; path != "" && ((!cfg.Whisper.StoreLiveMigration && !cfg.Whisper.StoreIncrementalMigration) || !filepath.IsAbs(path)) {
		return errors.New("store-migration-control-file requires live migration and an absolute local path")
	}
	if cfg.Whisper.StoreMigrationBaselineCopy != "" && !cfg.Whisper.StoreLiveMigration {
		return errors.New("a verified migration baseline requires live storage migration")
	}

	if cfg.Whisper.StoreLiveMigration {
		if cfg.Whisper.StorageBackend != "pebble-chunk" || !cfg.Whisper.StoreMirrorFiles || !cfg.Carbonserver.Enabled || cfg.Tags.Enabled {
			return errors.New("live storage migration requires mirrored exact storage, carbonserver and untagged original paths")
		}
		if cfg.Whisper.StoreMigrationIOSlots < 0 {
			return errors.New("store-migration-io-slots must be nonnegative")
		}
		if b := cfg.Whisper.StoreMigrationHintBurst; b != nil && *b < 0 {
			return errors.New("store-migration-hint-burst must be nonnegative")
		}
		if cfg.Whisper.StoreMigrationWorkers < 0 || cfg.Whisper.StoreMigrationWorkers > maxStoreMigrationWorkers {
			return fmt.Errorf("store-migration-workers must be 0..%d", maxStoreMigrationWorkers)
		}
	}
	if cfg.Whisper.StoreEncoding != "" && cfg.Whisper.StoreEncoding != "legacy" && cfg.Whisper.StoreEncoding != "exact" {
		return errors.New("whisper.store-encoding must be legacy or exact")
	}
	if cfg.Whisper.StorageBackend == "" {
		cfg.Whisper.StorageBackend = "files"
	}
	if cfg.Whisper.StorageBackend == "pebble" {
		return errors.New("whisper.storage-backend = pebble is no longer supported; migrate legacy data through buckyd, then use pebble-chunk")
	}
	if cfg.Whisper.StorageBackend != "files" && cfg.Whisper.StorageBackend != "pebble-chunk" {
		return errors.New("whisper.storage-backend must be files or pebble-chunk")
	}
	if cfg.Buckyd.Enabled && cfg.Whisper.StorageBackend != "pebble-chunk" {
		return errors.New("embedded buckyd requires whisper.storage-backend = pebble-chunk")
	}
	if cfg.Whisper.StorageBackend != "pebble-chunk" {
		if cfg.Whisper.StoreMirrorFiles {
			return errors.New("whisper.store-mirror-files requires storage-backend = pebble-chunk")
		}
		if cfg.Whisper.StoreEncoding == "exact" {
			return errors.New("whisper.store-encoding = exact requires storage-backend = pebble-chunk")
		}
		return nil
	}
	if cfg.Whisper.StoreCacheSize <= 0 || cfg.Whisper.StoreMemTableSize < 64<<10 {
		return errors.New("shared storage requires positive cache size and memtable size >= 65536")
	}
	if cfg.Whisper.StoreCompactionWorkers < 0 || cfg.Whisper.StoreCompactionWorkers > 64 || cfg.Whisper.StoreL0CompactionConcurrency < 0 || cfg.Whisper.StoreL0CompactionConcurrency > 64 {
		return errors.New("shared storage compaction workers and L0 concurrency must be 0..64")
	}
	if cfg.Whisper.StoreCompactionRate < -1 {
		return errors.New("shared storage compaction rate must be -1 (auto), 0 (off), or positive")
	}
	if h := cfg.Whisper.StoreCompactionHeadroom; !(h >= 1 && h <= 4) {
		return errors.New("whisper.store-compaction-rate-headroom must be 1..4")
	}
	if w := cfg.Whisper.StoreCompactionWarmup; w == nil || w.Value() < 0 || w.Value() > 24*time.Hour {
		return errors.New("whisper.store-compaction-pacing-warmup must be a duration of 0..24h")
	}
	if cfg.Whisper.StoreFlushRate < -1 {
		return errors.New("shared storage flush rate must be -1 (auto), 0 (off), or positive")
	}
	if budget := cfg.Whisper.StoreMaxOpenFiles; budget < 0 || budget > 1<<20 || budget > 0 && budget < 64 {
		return errors.New("shared storage table-reader budget must be zero or 64..1048576")
	}
	if cfg.Whisper.StoreSyncInterval == nil || cfg.Whisper.StoreSyncInterval.Value() < 0 {
		return errors.New("whisper.store-sync-interval must be a non-negative duration")
	}
	if cores := cfg.Whisper.StoreMetadataCachePrewarmCPUCores; cores < 0 || math.IsNaN(cores) || math.IsInf(cores, 0) {
		return errors.New("store-metadata-cache-prewarm-cpu-cores must be finite and nonnegative")
	}
	if h := cfg.Whisper.StoreRestartCPUHeadroom; !(h >= 1 && h <= 4) {
		return errors.New("whisper.store-restart-cpu-headroom must be 1..4")
	}
	if cfg.Whisper.StoreMetadataCachePrewarmWorkers < 0 || cfg.Whisper.StoreMetadataCachePrewarmWorkers > 256 || cfg.Whisper.StoreMetadataCachePrewarmRate < 0 || cfg.Whisper.StoreMetadataCachePrewarmByteRate < 0 {
		return errors.New("whisper.store-metadata-cache-prewarm-workers must be 0..256 and prewarm rate/byte-rate nonnegative")
	}
	if cfg.Whisper.StoreMetadataCachePrewarmWait != nil && cfg.Whisper.StoreMetadataCachePrewarmWait.Value() < 0 {
		return errors.New("metadata prewarm wait must be nonnegative")
	}
	if cap := cfg.Whisper.StoreMetadataCachePrewarmWaitCap; cap != nil && cap.Value() <= 0 {
		return errors.New("metadata prewarm wait cap must be positive")
	}
	if err := validateExpirationConfig(cfg); err != nil {
		return err
	}
	if cfg.Whisper.StoreMirrorFiles {
		if cfg.Buckyd.Enabled || cfg.Whisper.StoreExpiration.Value() != 0 || cfg.Whisper.StoreExpirationFilename != "" || len(cfg.Whisper.ExpirationRules) != 0 {
			return errors.New("rollback mirroring requires buckyd and shared expiration to be disabled; administrative deletes are not mirrored")
		}
		if cfg.Whisper.StoreEncoding != "exact" || !cfg.Whisper.Enabled || cfg.Whisper.StoreDir == "" {
			return errors.New("rollback mirroring requires exact encoding, an enabled persister and an explicit store-dir")
		}
		if err := validateMirrorPaths(cfg.Whisper.DataDir, cfg.Whisper.StoreDir); err != nil {
			return err
		}
	}
	if cfg.Whisper.OnlineMigration {
		return errors.New("online-migration is not supported by shared storage")
	}
	return validateSharedStoragePolicies(cfg)
}

func validateSharedStoragePolicies(cfg *Config) error {
	for _, schema := range cfg.Whisper.Schemas {
		if schema.Migration != nil && *schema.Migration {
			return errors.New("schema migration is not supported by shared storage")
		}
	}
	for _, quota := range cfg.Whisper.Quotas {
		if quota.PhysicalSize > 0 && quota.PhysicalSize != math.MaxInt64 && !cfg.Whisper.PebbleChunkIgnorePhysicalQuotas && !cfg.Whisper.StoreMirrorFiles {
			return errors.New("namespace physical-size quotas are unavailable with shared storage; use logical-size quotas or set whisper.pebble-chunk-ignore-physical-quotas = true")
		}
	}
	return nil
}

func storageSettingsChanged(old, next *Config) bool {
	if old.Common.MemoryBudget != next.Common.MemoryBudget || old.memoryLimit != next.memoryLimit {
		return true
	}
	if old.Whisper.StorageBackend != next.Whisper.StorageBackend || !reflect.DeepEqual(old.Buckyd, next.Buckyd) {
		return true
	}
	if next.Whisper.StorageBackend != "pebble-chunk" {
		// The store path derives from data-dir, which file backends may still hot-reload.
		return false
	}
	return sharedStorePath(old) != sharedStorePath(next) ||
		old.Whisper.StoreMigrationArchiveDir != next.Whisper.StoreMigrationArchiveDir ||
		old.Whisper.StoreIncrementalMigration != next.Whisper.StoreIncrementalMigration ||
		((old.Whisper.StoreIncrementalMigration || next.Whisper.StoreIncrementalMigration) && old.Whisper.DataDir != next.Whisper.DataDir) ||
		old.Whisper.StoreMigrationBytesInFlight != next.Whisper.StoreMigrationBytesInFlight ||
		old.Whisper.StoreMigrationMinFreeBytes != next.Whisper.StoreMigrationMinFreeBytes ||
		old.Whisper.StoreMirrorFiles != next.Whisper.StoreMirrorFiles ||
		old.Whisper.StoreLiveMigration != next.Whisper.StoreLiveMigration ||
		old.Whisper.StoreMigrationIngest != next.Whisper.StoreMigrationIngest ||
		old.Whisper.StoreTimeBuckets != next.Whisper.StoreTimeBuckets ||
		(old.Whisper.StoreTimeBucketsRelayoutRate > 0) != (next.Whisper.StoreTimeBucketsRelayoutRate > 0) ||
		old.Whisper.StoreMigrationIngestLinger.Value() != next.Whisper.StoreMigrationIngestLinger.Value() ||
		old.Whisper.StoreMigrationIngestGroup != next.Whisper.StoreMigrationIngestGroup ||
		old.Whisper.StoreMigrationIngestParallel != next.Whisper.StoreMigrationIngestParallel ||
		old.Whisper.StoreMigrationWorkers != next.Whisper.StoreMigrationWorkers ||
		old.Whisper.StoreMigrationControlFile != next.Whisper.StoreMigrationControlFile ||
		old.Whisper.StoreMigrationBaselineCopy != next.Whisper.StoreMigrationBaselineCopy ||
		old.Whisper.StoreEncoding != next.Whisper.StoreEncoding ||
		old.Whisper.StoreCacheSize != next.Whisper.StoreCacheSize ||
		old.Whisper.StoreMemTableSize != next.Whisper.StoreMemTableSize ||
		old.Whisper.StoreL0CompactionConcurrency != next.Whisper.StoreL0CompactionConcurrency ||
		old.Whisper.StoreMaxOpenFiles != next.Whisper.StoreMaxOpenFiles ||
		old.Whisper.StoreTargetFileSize != next.Whisper.StoreTargetFileSize ||
		old.Whisper.StoreL0TargetFileSize != next.Whisper.StoreL0TargetFileSize ||
		old.Whisper.StoreParallelTableSyncs != next.Whisper.StoreParallelTableSyncs ||
		old.Whisper.StoreBloomFilterBits != next.Whisper.StoreBloomFilterBits ||
		old.Whisper.StoreWALDir != next.Whisper.StoreWALDir ||
		old.Whisper.StoreCacheShards != next.Whisper.StoreCacheShards ||
		old.Whisper.StoreTableCacheShards != next.Whisper.StoreTableCacheShards ||
		old.Whisper.StoreCompactOnce != next.Whisper.StoreCompactOnce ||
		old.Whisper.StoreReuseBlockKeys != next.Whisper.StoreReuseBlockKeys ||
		old.Whisper.StoreOOODirectWrites != next.Whisper.StoreOOODirectWrites ||
		old.Whisper.StoreFetchSharedIterator != next.Whisper.StoreFetchSharedIterator ||
		old.Whisper.StoreMetadataCacheSize != next.Whisper.StoreMetadataCacheSize ||
		old.Whisper.StoreBytesPerSync != next.Whisper.StoreBytesPerSync ||
		old.Whisper.StoreSyncInterval.Value() != next.Whisper.StoreSyncInterval.Value()
}

// storeStats reports counters as deltas since the previous flush, like every
// other module's Stat. Store.Stats itself exposes cumulative values.
type storeStats struct {
	db   *store.Store
	prev store.Stats
}

func (s *storeStats) Stat(send helper.StatCallback) {
	stats := s.db.Stats()
	send("diskBytes", float64(stats.DiskBytes))
	send("walBytes", float64(stats.WALBytes))
	send("memTableBytes", float64(stats.MemTableBytes))
	send("cacheBytes", float64(stats.CacheBytes))
	send("cacheHits", float64(stats.CacheHits-s.prev.CacheHits))
	send("cacheMisses", float64(stats.CacheMisses-s.prev.CacheMisses))
	send("chunkMaterializations", float64(stats.Materializations-s.prev.Materializations))
	send("chunkOperands", float64(stats.Operands-s.prev.Operands))
	send("reconcileComparedChunks", float64(stats.ReconcileComparedChunks-s.prev.ReconcileComparedChunks))
	send("reconcileWrittenChunks", float64(stats.ReconcileWrittenChunks-s.prev.ReconcileWrittenChunks))
	send("reconcileDeletedChunks", float64(stats.ReconcileDeletedChunks-s.prev.ReconcileDeletedChunks))
	send("reconcileUnchangedMetrics", float64(stats.ReconcileUnchangedMetrics-s.prev.ReconcileUnchangedMetrics))
	send("flushes", float64(stats.Flushes-s.prev.Flushes))
	send("compactions", float64(stats.Compactions-s.prev.Compactions))
	send("compactionDebt", float64(stats.CompactionDebt))
	send("compactionsInProgress", float64(stats.CompactionsInProgress))
	send("compactionWorkers", float64(stats.CompactionWorkers))
	send("compactionRate", float64(stats.CompactionRate))
	send("compactionPacerTargetRate", float64(stats.CompactionPacerTargetRate))
	var auto, bypass float64
	if stats.CompactionPacerAuto {
		auto = 1
	}
	if stats.CompactionPacerBypass != "" {
		bypass = 1
	}
	send("compactionPacerAuto", auto)
	send("compactionPacerBypass", bypass)
	for reason, count := range stats.CompactionPacerBypasses {
		send("compactionPacerBypass_"+reason, float64(count-s.prev.CompactionPacerBypasses[reason]))
	}
	send("compactionPacerBytes", float64(stats.CompactionPacerBytes-s.prev.CompactionPacerBytes))
	send("compactionPacerWaitSeconds", float64(stats.CompactionPacerWaitNanos-s.prev.CompactionPacerWaitNanos)/1e9)
	send("flushRate", float64(stats.FlushRate))
	send("flushPacerBytes", float64(stats.FlushPacerBytes-s.prev.FlushPacerBytes))
	send("flushPacerWaitSeconds", float64(stats.FlushPacerWaitNanos-s.prev.FlushPacerWaitNanos)/1e9)
	send("l0Sublevels", float64(stats.L0Sublevels))
	send("l0Files", float64(stats.L0Files))
	send("l0Bytes", float64(stats.L0Bytes))
	send("compactionReadBytes", float64(stats.CompactionReadBytes-s.prev.CompactionReadBytes))
	send("compactionWriteBytes", float64(stats.CompactionWriteBytes-s.prev.CompactionWriteBytes))
	send("flushWriteBytes", float64(stats.FlushWriteBytes-s.prev.FlushWriteBytes))
	send("writeStalls", float64(stats.WriteStalls-s.prev.WriteStalls))
	send("writeStallSeconds", float64(stats.WriteStallNanos-s.prev.WriteStallNanos)/1e9)
	send("tableCacheEntries", float64(stats.TableCacheEntries))
	send("tableCacheHits", float64(stats.TableCacheHits-s.prev.TableCacheHits))
	send("tableCacheMisses", float64(stats.TableCacheMisses-s.prev.TableCacheMisses))
	s.prev = stats
}

func (app *App) configureTransferCache() {
	core := app.Cache
	app.MetricStore.SetTransferPending(core.Has, func() bool { return !core.IsEmpty() })
	app.Buckyd.SetTransferFlush(func(ctx context.Context, names []string) error {
		// Stop can hold App's lock while waiting for Buckyd handlers.
		if !app.TryRLock() {
			return fmt.Errorf("application lifecycle is busy: %w", store.ErrConflict)
		}
		defer app.RUnlock()
		var pending []string
		for _, name := range names {
			if core.Has(name) {
				pending = append(pending, name)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		if app.Persister == nil {
			return errors.New("pending points have no persister")
		}
		return app.Persister.FlushForTransfer(ctx, pending)
	})
}
