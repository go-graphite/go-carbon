package carbon

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"time"
)

// The entire section is restart-only, including when currently disabled.
type v2Config struct {
	Enabled      bool     `toml:"enabled"`
	DataDir      string   `toml:"data-dir"`
	Shards       int      `toml:"shards"`
	Owners       int      `toml:"owners"`
	PartInterval Duration `toml:"part-interval"`
	WriteEngine  string   `toml:"write-engine"`
	JournalCodec string   `toml:"journal-codec"`
}

func newV2Config() v2Config {
	return v2Config{DataDir: "/var/lib/go-carbon/parts", Shards: 16, Owners: runtime.NumCPU(), PartInterval: Duration{time.Second}}
}

func validateV2Config(c *Config) error {
	v := c.V2
	if !v.Enabled {
		return nil
	}
	switch v.WriteEngine {
	case "", "standalone":
	case "core":
		if v.Owners < v.Shards {
			return fmt.Errorf("core write engine requires at least one journal owner per shard")
		}
	default:
		return fmt.Errorf("unknown v2 write-engine %q", v.WriteEngine)
	}
	switch v.JournalCodec {
	case "", "pdx1", "pdc3", "pdc4":
	default:
		return fmt.Errorf("unknown v2 journal-codec %q", v.JournalCodec)
	}
	if !filepath.IsAbs(v.DataDir) || filepath.Clean(v.DataDir) == string(filepath.Separator) {
		return fmt.Errorf("v2.data-dir must be an absolute non-root directory")
	}
	if v.Shards <= 0 || v.Owners <= 0 || v.PartInterval.Value() <= 0 {
		return fmt.Errorf("v2 shards, owners and part-interval must be positive")
	}
	if filepath.Clean(v.DataDir) == filepath.Clean(c.Whisper.DataDir) || filepath.Clean(v.DataDir) == filepath.Clean(sharedStorePath(c)) {
		return fmt.Errorf("v2.data-dir must differ from legacy storage directories")
	}
	if !c.Whisper.Enabled {
		return fmt.Errorf("v2 requires whisper.enabled for schema and aggregation policy")
	}
	// These paths need explicit v2 adapters; none may run a legacy writer or
	// checkpoint restorer concurrently with the immutable-parts backend.
	if c.InputLog.Enabled || c.Dump.Enabled || c.Dump.HandoffSupervisor || c.Tags.Enabled || c.Buckyd.Enabled || c.Whisper.StoreLiveMigration || c.Whisper.StoreIncrementalMigration {
		return fmt.Errorf("v2 requires legacy input-log, dump, tags, buckyd and storage migrations disabled")
	}
	return nil
}

func v2ReloadError(before, after *Config) error {
	if !reflect.DeepEqual(before.V2, after.V2) {
		return fmt.Errorf("v2 settings require a restart")
	}
	if before.V2.Enabled && (!reflect.DeepEqual(before.Whisper, after.Whisper) || !reflect.DeepEqual(before.Tags, after.Tags) || !reflect.DeepEqual(before.Dump, after.Dump) || !reflect.DeepEqual(before.InputLog, after.InputLog) || !reflect.DeepEqual(before.Buckyd, after.Buckyd)) {
		return fmt.Errorf("v2 storage and policy settings require a restart")
	}
	return nil
}
