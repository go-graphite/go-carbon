package carbon

import (
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Shares of the budget. They follow a fleet of shared-store hosts tuned by
// hand: the native block cache at about 6% and the Go limit at about 33% of
// RAM, the rest left to the page cache. A 48% budget reproduces both. The Go
// limit includes the metadata and write caches; the remaining 30% is the
// native block cache and room for memtables, mappings and the runtime.
// These are startup defaults, not an RSS limit.
const (
	budgetGoLimitPercent       = 70
	budgetBlockCachePercent    = 13
	budgetMetadataCachePercent = 13
)

// budgetUnits are GOMEMLIMIT's suffixes, so both settings read alike.
var budgetUnits = []struct {
	suffix string
	shift  uint
}{{"TiB", 40}, {"GiB", 30}, {"MiB", 20}, {"KiB", 10}, {"B", 0}}

func parseBudgetBytes(text string) (uint64, bool) {
	shift := uint(0)
	for _, u := range budgetUnits {
		if strings.HasSuffix(text, u.suffix) {
			text, shift = strings.TrimSuffix(text, u.suffix), u.shift
			break
		}
	}
	n, err := strconv.ParseUint(text, 10, 63)
	if err != nil || n == 0 || n > math.MaxInt64>>shift {
		return 0, false
	}
	return n << shift, true
}

func (cfg *Config) resolveMemoryBudget(defined toml.MetaData, available func() (uint64, error), gomemlimit string) error {
	text := strings.TrimSpace(cfg.Common.MemoryBudget)
	if text == "" || text == "0" {
		return nil
	}
	var n uint64
	if value, ok := strings.CutSuffix(text, "%"); ok {
		p, err := strconv.ParseUint(value, 10, 63)
		if err != nil || p == 0 || p > 100 {
			return fmt.Errorf("memory-budget percentage must be from 1%% to 100%%")
		}
		limit, err := available()
		if err != nil {
			return fmt.Errorf("memory-budget: %w", err)
		}
		// Divide first to avoid overflowing for large host limits.
		n = limit/100*p + limit%100*p/100
	} else {
		var ok bool
		if n, ok = parseBudgetBytes(text); !ok {
			return fmt.Errorf("memory-budget must be positive bytes with an optional B, KiB, MiB, GiB or TiB suffix, or a percentage from 1%% to 100%%")
		}
	}
	if n < 64<<20 || n > math.MaxInt64 {
		return fmt.Errorf("memory-budget must resolve to between 64 MiB and %d bytes", int64(math.MaxInt64))
	}
	cfg.memoryBudget = int64(n)
	share := func(percent uint64) int64 { return int64(n/100*percent + n%100*percent/100) }
	if !defined.IsDefined("whisper", "store-cache-size") {
		cfg.Whisper.StoreCacheSize = share(budgetBlockCachePercent)
	}
	if !defined.IsDefined("whisper", "store-metadata-cache-size") {
		cfg.Whisper.StoreMetadataCacheSize = share(budgetMetadataCachePercent)
	}
	if gomemlimit == "" {
		cfg.memoryLimit = share(budgetGoLimitPercent)
	}
	return nil
}

func (cfg *Config) applyMemoryBudget() {
	if cfg.memoryBudget == 0 {
		return
	}
	source := "GOMEMLIMIT"
	if cfg.memoryLimit > 0 {
		debug.SetMemoryLimit(cfg.memoryLimit)
		source = "memory-budget"
	}
	zapwriter.Logger("config").Info("memory budget resolved",
		zap.String("memory_budget", cfg.Common.MemoryBudget),
		zap.Int64("budget_bytes", cfg.memoryBudget),
		zap.Int64("store_cache_size", cfg.Whisper.StoreCacheSize),
		zap.Int64("store_metadata_cache_size", cfg.Whisper.StoreMetadataCacheSize),
		zap.Int64("go_memory_limit", debug.SetMemoryLimit(-1)),
		zap.String("go_memory_limit_source", source),
	)
}
