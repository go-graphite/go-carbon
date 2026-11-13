package carbon

// Decoding views preserve the existing numeric configuration and runtime APIs.
type readableCacheConfig struct {
	*cacheConfig
	MaxSize           readableSize `toml:"max-size"`
	MaxBytes          readableSize `toml:"max-bytes"`
	BloomSize         readableSize `toml:"bloom-size"`
	WriteoutMinPoints readableSize `toml:"writeout-min-points"`
}

type readableWhisperConfig struct {
	*whisperConfig
	StoreCacheSize                       readableSize `toml:"store-cache-size"`
	StoreMemTableSize                    readableSize `toml:"store-memtable-size"`
	StoreCompactionRate                  readableSize `toml:"store-compaction-rate"`
	StoreFlushRate                       readableSize `toml:"store-flush-rate"`
	StoreLBaseMaxBytes                   readableSize `toml:"store-lbase-max-bytes"`
	StoreMemTableMaxUnflushed            readableSize `toml:"store-memtable-max-unflushed-bytes"`
	StoreColdPackMinFree                 readableSize `toml:"store-cold-pack-min-free-bytes"`
	StoreColdPackRate                    readableSize `toml:"store-cold-pack-bytes-per-second"`
	StoreColdPackMaxDebt                 readableSize `toml:"store-cold-pack-max-compaction-debt"`
	StoreNativeCombineMemory             readableSize `toml:"store-native-combine-memory"`
	StoreTimeBucketsRelayoutRate         readableSize `toml:"store-time-buckets-relayout-rate"`
	StoreTimeBucketsRelayoutMinFree      readableSize `toml:"store-time-buckets-relayout-min-free-bytes"`
	StoreTimeBucketsRelayoutMaxDebt      readableSize `toml:"store-time-buckets-relayout-max-compaction-debt"`
	StoreTimeBucketsRelayoutReclaimBytes readableSize `toml:"store-time-buckets-relayout-reclaim-bytes"`
	StoreMigrationIngestGroup            readableSize `toml:"store-migration-ingest-group-bytes"`
	StoreTargetFileSize                  readableSize `toml:"store-target-file-size"`
	StoreL0TargetFileSize                readableSize `toml:"store-l0-target-file-size"`
	StoreMetadataCacheSize               readableSize `toml:"store-metadata-cache-size"`
	StoreBytesPerSync                    readableSize `toml:"store-bytes-per-sync"`
	StoreMigrationBytesInFlight          readableSize `toml:"store-migration-bytes-in-flight"`
	StoreMigrationMinFreeBytes           readableSize `toml:"store-migration-min-free-bytes"`
	StoreExpirationMaxDebt               readableSize `toml:"store-expiration-max-compaction-debt"`
	OutOfOrderCompactThreshold           readableSize `toml:"out-of-order-compact-threshold"`
	StoreMetadataCachePrewarmByteRate    readableSize `toml:"store-metadata-cache-prewarm-byte-rate"`
	StoreMaxOpenFiles                    readableSize `toml:"store-max-open-files"`
	StoreUpdateBatchSize                 readableSize `toml:"store-update-batch-size"`
	MaxUpdatesPerSecond                  readableSize `toml:"max-updates-per-second"`
	MaxUpdatesPerSecondAutoMin           readableSize `toml:"max-updates-per-second-auto-min"`
	MaxUpdatesPerSecondAutoMax           readableSize `toml:"max-updates-per-second-auto-max"`
	StoreExpirationScanRate              readableSize `toml:"store-expiration-scan-rate"`
	StoreExpirationDeleteRate            readableSize `toml:"store-expiration-delete-rate"`
	StoreExpirationMaxDeletes            readableSize `toml:"store-expiration-max-deletes"`
	OnlineMigrationRate                  readableSize `toml:"online-migration-rate"`
	OutOfOrderCompactRate                readableSize `toml:"out-of-order-compact-rate"`
	OutOfOrderCompactMinPoints           readableSize `toml:"out-of-order-compact-min-points"`
	StoreMetadataCachePrewarmRate        readableSize `toml:"store-metadata-cache-prewarm-rate"`
}

type readableCarbonserverConfig struct {
	*carbonserverConfig
	MaxGlobs            readableSize `toml:"max-globs"`
	MaxMetricsGlobbed   readableSize `toml:"max-metrics-globbed"`
	MaxMetricsRendered  readableSize `toml:"max-metrics-rendered"`
	MaxCreatesPerSecond readableSize `toml:"max-creates-per-second"`
}

type readableDumpConfig struct {
	*dumpConfig
	RestorePerSecond readableSize `toml:"restore-per-second"`
}

func readableConfig(cfg *Config) any {
	type plain Config
	return &struct {
		*plain
		Cache        readableCacheConfig        `toml:"cache"`
		Whisper      readableWhisperConfig      `toml:"whisper"`
		Carbonserver readableCarbonserverConfig `toml:"carbonserver"`
		Dump         readableDumpConfig         `toml:"dump"`
	}{
		plain: (*plain)(cfg),
		Cache: readableCacheConfig{
			cacheConfig:       &cfg.Cache,
			MaxSize:           readableSize{target: &cfg.Cache.MaxSize, bytes: false},
			MaxBytes:          readableSize{target: &cfg.Cache.MaxBytes, bytes: true},
			BloomSize:         readableSize{target: &cfg.Cache.BloomSize, bytes: false},
			WriteoutMinPoints: readableSize{target: &cfg.Cache.WriteoutMinPoints, bytes: false},
		},
		Whisper: readableWhisperConfig{
			whisperConfig:                        &cfg.Whisper,
			StoreCacheSize:                       readableSize{target: &cfg.Whisper.StoreCacheSize, bytes: true},
			StoreMemTableSize:                    readableSize{target: &cfg.Whisper.StoreMemTableSize, bytes: true},
			StoreCompactionRate:                  readableSize{target: &cfg.Whisper.StoreCompactionRate, bytes: true},
			StoreFlushRate:                       readableSize{target: &cfg.Whisper.StoreFlushRate, bytes: true},
			StoreLBaseMaxBytes:                   readableSize{target: &cfg.Whisper.StoreLBaseMaxBytes, bytes: true},
			StoreMemTableMaxUnflushed:            readableSize{target: &cfg.Whisper.StoreMemTableMaxUnflushed, bytes: true},
			StoreColdPackMinFree:                 readableSize{target: &cfg.Whisper.StoreColdPackMinFree, bytes: true},
			StoreColdPackRate:                    readableSize{target: &cfg.Whisper.StoreColdPackRate, bytes: true},
			StoreColdPackMaxDebt:                 readableSize{target: &cfg.Whisper.StoreColdPackMaxDebt, bytes: true},
			StoreNativeCombineMemory:             readableSize{target: &cfg.Whisper.StoreNativeCombineMemory, bytes: true},
			StoreTimeBucketsRelayoutRate:         readableSize{target: &cfg.Whisper.StoreTimeBucketsRelayoutRate, bytes: true},
			StoreTimeBucketsRelayoutMinFree:      readableSize{target: &cfg.Whisper.StoreTimeBucketsRelayoutMinFree, bytes: true},
			StoreTimeBucketsRelayoutMaxDebt:      readableSize{target: &cfg.Whisper.StoreTimeBucketsRelayoutMaxDebt, bytes: true},
			StoreTimeBucketsRelayoutReclaimBytes: readableSize{target: &cfg.Whisper.StoreTimeBucketsRelayoutReclaimBytes, bytes: true},
			StoreMigrationIngestGroup:            readableSize{target: &cfg.Whisper.StoreMigrationIngestGroup, bytes: true},
			StoreTargetFileSize:                  readableSize{target: &cfg.Whisper.StoreTargetFileSize, bytes: true},
			StoreL0TargetFileSize:                readableSize{target: &cfg.Whisper.StoreL0TargetFileSize, bytes: true},
			StoreMetadataCacheSize:               readableSize{target: &cfg.Whisper.StoreMetadataCacheSize, bytes: true},
			StoreBytesPerSync:                    readableSize{target: &cfg.Whisper.StoreBytesPerSync, bytes: true},
			StoreMigrationBytesInFlight:          readableSize{target: &cfg.Whisper.StoreMigrationBytesInFlight, bytes: true},
			StoreMigrationMinFreeBytes:           readableSize{target: &cfg.Whisper.StoreMigrationMinFreeBytes, bytes: true},
			StoreExpirationMaxDebt:               readableSize{target: &cfg.Whisper.StoreExpirationMaxDebt, bytes: true},
			OutOfOrderCompactThreshold:           readableSize{target: &cfg.Whisper.OutOfOrderCompactThreshold, bytes: true},
			StoreMetadataCachePrewarmByteRate:    readableSize{target: &cfg.Whisper.StoreMetadataCachePrewarmByteRate, bytes: true},
			StoreMaxOpenFiles:                    readableSize{target: &cfg.Whisper.StoreMaxOpenFiles, bytes: false},
			StoreUpdateBatchSize:                 readableSize{target: &cfg.Whisper.StoreUpdateBatchSize, bytes: false},
			MaxUpdatesPerSecond:                  readableSize{target: &cfg.Whisper.MaxUpdatesPerSecond, bytes: false},
			MaxUpdatesPerSecondAutoMin:           readableSize{target: &cfg.Whisper.MaxUpdatesPerSecondAutoMin, bytes: false},
			MaxUpdatesPerSecondAutoMax:           readableSize{target: &cfg.Whisper.MaxUpdatesPerSecondAutoMax, bytes: false},
			StoreExpirationScanRate:              readableSize{target: &cfg.Whisper.StoreExpirationScanRate, bytes: false},
			StoreExpirationDeleteRate:            readableSize{target: &cfg.Whisper.StoreExpirationDeleteRate, bytes: false},
			StoreExpirationMaxDeletes:            readableSize{target: &cfg.Whisper.StoreExpirationMaxDeletes, bytes: false},
			OnlineMigrationRate:                  readableSize{target: &cfg.Whisper.OnlineMigrationRate, bytes: false},
			OutOfOrderCompactRate:                readableSize{target: &cfg.Whisper.OutOfOrderCompactRate, bytes: false},
			OutOfOrderCompactMinPoints:           readableSize{target: &cfg.Whisper.OutOfOrderCompactMinPoints, bytes: false},
			StoreMetadataCachePrewarmRate:        readableSize{target: &cfg.Whisper.StoreMetadataCachePrewarmRate, bytes: false},
		},
		Carbonserver: readableCarbonserverConfig{
			carbonserverConfig:  &cfg.Carbonserver,
			MaxGlobs:            readableSize{target: &cfg.Carbonserver.MaxGlobs, bytes: false},
			MaxMetricsGlobbed:   readableSize{target: &cfg.Carbonserver.MaxMetricsGlobbed, bytes: false},
			MaxMetricsRendered:  readableSize{target: &cfg.Carbonserver.MaxMetricsRendered, bytes: false},
			MaxCreatesPerSecond: readableSize{target: &cfg.Carbonserver.MaxCreatesPerSecond, bytes: false},
		},
		Dump: readableDumpConfig{
			dumpConfig:       &cfg.Dump,
			RestorePerSecond: readableSize{target: &cfg.Dump.RestorePerSecond, bytes: false},
		},
	}
}
