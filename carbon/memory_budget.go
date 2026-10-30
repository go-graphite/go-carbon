package carbon

import (
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

// The Go limit includes metadata and write caches. The remaining 30% is for
// the native block cache (20%) and mappings/native/runtime headroom (10%).
// These are startup defaults, not an RSS limit or a cache-sizing recommendation.
func (cfg *Config) resolveMemoryBudget(defined toml.MetaData, available func() (uint64, error), gomemlimit string) error {
	text := strings.TrimSpace(cfg.Common.MemoryBudget)
	if text == "" || text == "0" {
		return nil
	}
	value := strings.TrimSuffix(text, "%")
	n, err := strconv.ParseUint(value, 10, 63)
	if err != nil || n == 0 {
		return fmt.Errorf("memory-budget must be positive bytes or a percentage from 1%% to 100%%")
	}
	if strings.HasSuffix(text, "%") {
		if n > 100 {
			return fmt.Errorf("memory-budget percentage must be from 1%% to 100%%")
		}
		limit, err := available()
		if err != nil {
			return fmt.Errorf("memory-budget: %w", err)
		}
		// Divide first to avoid overflowing for large host limits.
		n = limit/100*n + limit%100*n/100
	}
	if n < 64<<20 || n > math.MaxInt64 {
		return fmt.Errorf("memory-budget must resolve to between 64 MiB and %d bytes", int64(math.MaxInt64))
	}
	share := func(percent uint64) int64 { return int64(n/100*percent + n%100*percent/100) }
	if !defined.IsDefined("whisper", "store-cache-size") {
		cfg.Whisper.StoreCacheSize = share(20)
	}
	if !defined.IsDefined("whisper", "store-metadata-cache-size") {
		cfg.Whisper.StoreMetadataCacheSize = share(20)
	}
	if !defined.IsDefined("cache", "max-bytes") {
		cfg.Cache.MaxBytes = uint64(share(10))
	}
	if gomemlimit == "" {
		cfg.memoryLimit = share(70)
	}
	return nil
}

func (cfg *Config) applyMemoryBudget() {
	if cfg.memoryLimit > 0 {
		debug.SetMemoryLimit(cfg.memoryLimit)
	}
}
