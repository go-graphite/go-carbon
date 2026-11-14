package carbon

import (
	"fmt"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func (cfg *Config) validateNativeCombineBudget(available func() (uint64, error)) error {
	w := cfg.Whisper
	if err := store.ValidateNativeExportConcurrency(w.StoreNativeExportConcurrency); err != nil {
		return fmt.Errorf("whisper.store-native-export-concurrency: %w", err)
	}
	if err := store.ValidateNativeImportBudget(w.StoreNativeImportConcurrency, w.StoreNativeCombineWorkers, w.StoreNativeCombineMemory); err != nil {
		return fmt.Errorf("whisper.store-native-combine: %w", err)
	}
	// Preserve the existing budget on small hosts and unsupported platforms.
	// Only an increase needs detection, and detection failure rejects it.
	if w.StoreNativeCombineMemory <= store.DefaultNativeCombineMemory && w.StoreNativeExportConcurrency == store.DefaultNativeExportConcurrency {
		return nil
	}
	ram, err := available()
	if err != nil {
		return fmt.Errorf("whisper.store-native-combine-memory: detect available RAM: %w", err)
	}
	if w.StoreNativeExportConcurrency > store.DefaultNativeExportConcurrency {
		// Existing serial admission is preserved on small/unknown hosts. An
		// increase must budget all admitted exports plus the import scratch pool.
		required := uint64(w.StoreNativeExportConcurrency)*uint64(store.NativeExportMemoryPerStream) + uint64(w.StoreNativeCombineMemory)
		if required > ram/4 {
			return fmt.Errorf("whisper.store-native-export-concurrency: export scratch plus import memory (%d bytes) must not exceed 25%% of available RAM (%d bytes)", required, ram/4)
		}
	}
	if uint64(w.StoreNativeCombineMemory) > ram/4 {
		return fmt.Errorf("whisper.store-native-combine-memory must not exceed 25%% of available RAM (%d bytes)", ram/4)
	}
	return nil
}
