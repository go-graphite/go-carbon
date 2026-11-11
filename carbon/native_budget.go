package carbon

import (
	"fmt"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func (cfg *Config) validateNativeCombineBudget(available func() (uint64, error)) error {
	w := cfg.Whisper
	if err := store.ValidateNativeCombineBudget(w.StoreNativeCombineWorkers, w.StoreNativeCombineMemory); err != nil {
		return fmt.Errorf("whisper.store-native-combine: %w", err)
	}
	// Preserve the existing budget on small hosts and unsupported platforms.
	// Only an increase needs detection, and detection failure rejects it.
	if w.StoreNativeCombineMemory <= store.DefaultNativeCombineMemory {
		return nil
	}
	ram, err := available()
	if err != nil {
		return fmt.Errorf("whisper.store-native-combine-memory: detect available RAM: %w", err)
	}
	if uint64(w.StoreNativeCombineMemory) > ram/4 {
		return fmt.Errorf("whisper.store-native-combine-memory must not exceed 25%% of available RAM (%d bytes)", ram/4)
	}
	return nil
}
