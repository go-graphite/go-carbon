package carbon

import (
	"errors"
	"path/filepath"
	"reflect"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func sharedStorePath(cfg *Config) string {
	if cfg.Whisper.StoreDir != "" {
		return cfg.Whisper.StoreDir
	}
	return filepath.Join(cfg.Whisper.DataDir, ".store-chunks")
}

func validateStorageConfig(cfg *Config) error {
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
		return nil
	}
	if cfg.Whisper.StoreCacheSize <= 0 || cfg.Whisper.StoreMemTableSize < 64<<10 {
		return errors.New("shared storage requires positive cache size and memtable size >= 65536")
	}
	if cfg.Whisper.StoreSyncInterval == nil || cfg.Whisper.StoreSyncInterval.Value() < 0 {
		return errors.New("whisper.store-sync-interval must be a non-negative duration")
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
		if quota.PhysicalSize > 0 {
			return errors.New("namespace physical-size quotas are unavailable with shared storage; use logical-size quotas")
		}
	}
	return nil
}

func storageSettingsChanged(old, next *Config) bool {
	if old.Whisper.StorageBackend != next.Whisper.StorageBackend || !reflect.DeepEqual(old.Buckyd, next.Buckyd) {
		return true
	}
	if next.Whisper.StorageBackend != "pebble-chunk" {
		// The store path derives from data-dir, which file backends may still hot-reload.
		return false
	}
	return sharedStorePath(old) != sharedStorePath(next) ||
		old.Whisper.StoreCacheSize != next.Whisper.StoreCacheSize ||
		old.Whisper.StoreMemTableSize != next.Whisper.StoreMemTableSize ||
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
	s.prev = stats
}
