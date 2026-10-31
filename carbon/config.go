package carbon

import (
	"bytes"
	"encoding"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/lomik/zapwriter"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/go-graphite/go-carbon/receiver/udp"
)

const MetricEndpointLocal = "local"

// Duration wrapper time.Duration for TOML
type Duration struct {
	time.Duration
}

var _ encoding.TextMarshaler = &Duration{}

// UnmarshalText from TOML
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalText encode text with TOML format
func (d *Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// Value return time.Duration value
func (d *Duration) Value() time.Duration {
	return d.Duration
}

type commonConfig struct {
	MemoryBudget            string    `toml:"memory-budget"`
	PhaseCPUProfileInterval *Duration `toml:"phase-cpu-profile-interval"`
	PhaseCPUProfileDuration *Duration `toml:"phase-cpu-profile-duration"`
	User                    string    `toml:"user"`
	Logfile                 *string   `toml:"logfile"`
	LogLevel                *string   `toml:"log-level"`
	GraphPrefix             string    `toml:"graph-prefix"`
	MetricInterval          *Duration `toml:"metric-interval"`
	MetricEndpoint          string    `toml:"metric-endpoint"`
	MaxCPU                  int       `toml:"max-cpu"`
	// HeartbeatFile, when set, is rewritten every HeartbeatInterval so a start
	// after an unclean stop can report the window of possibly lost points.
	HeartbeatFile     string    `toml:"heartbeat-file"`
	HeartbeatInterval *Duration `toml:"heartbeat-interval"`
	// MallocTrimInterval, when positive, returns freed C heap memory to the
	// system that often (cgo builds on Linux; see helper/malloc).
	MallocTrimInterval *Duration `toml:"malloc-trim-interval"`
	// MallocStats exports the C heap's size, read after each trim.
	MallocStats bool `toml:"malloc-stats"`
}

type whisperConfig struct {
	StorageBackend               string    `toml:"storage-backend"`
	StoreDir                     string    `toml:"store-dir"`
	StoreCacheSize               int64     `toml:"store-cache-size"`
	StoreMemTableSize            uint64    `toml:"store-memtable-size"`
	StoreCompactionRate          int64     `toml:"store-compaction-rate"`
	StoreCompactionWorkers       int       `toml:"store-compaction-workers"`
	StoreL0CompactionConcurrency int       `toml:"store-l0-compaction-concurrency"`
	StoreL0CompactionThreshold   int       `toml:"store-l0-compaction-threshold"`
	StoreL0StopWritesThreshold   int       `toml:"store-l0-stop-writes-threshold"`
	StoreLBaseMaxBytes           int64     `toml:"store-lbase-max-bytes"`
	StoreMemTableMaxUnflushed    uint64    `toml:"store-memtable-max-unflushed-bytes"`
	StoreMaxOpenFiles            int       `toml:"store-max-open-files"`
	StoreSyncInterval            *Duration `toml:"store-sync-interval"`
	StoreUpdateBatchSize         int       `toml:"store-update-batch-size"`
	StoreEncoding                string    `toml:"store-encoding"`
	StoreMirrorFiles             bool      `toml:"store-mirror-files"`
	StoreIncrementalMigration    bool      `toml:"store-incremental-migration"`
	StoreMigrationIngest         bool      `toml:"store-migration-ingest"`
	StoreColdPack                string    `toml:"store-cold-pack"`
	StoreColdPackMinFree         uint64    `toml:"store-cold-pack-min-free-bytes"`
	StoreColdPackRate            int64     `toml:"store-cold-pack-bytes-per-second"`
	StoreColdPackCPU             float64   `toml:"store-cold-pack-cpu-fraction"`
	StoreColdPackWindow          *Duration `toml:"store-cold-pack-write-window"`
	StoreColdPackInterval        *Duration `toml:"store-cold-pack-interval"`
	StoreColdPackMaxDebt         uint64    `toml:"store-cold-pack-max-compaction-debt"`
	StoreTimeBuckets             bool      `toml:"store-time-buckets"`
	StoreTimeBucketsRelayoutRate int64     `toml:"store-time-buckets-relayout-rate"`
	StoreMigrationIOSlots        int       `toml:"store-migration-io-slots"`
	StoreMigrationReclaimDelay   *Duration `toml:"store-migration-reclaim-delay"`
	StoreMigrationIngestLinger   *Duration `toml:"store-migration-ingest-linger"`
	StoreMigrationIngestGroup    int       `toml:"store-migration-ingest-group-bytes"`
	StoreMigrationIngestParallel int       `toml:"store-migration-ingest-parallel"`
	StoreTargetFileSize          int64     `toml:"store-target-file-size"`
	StoreL0TargetFileSize        int64     `toml:"store-l0-target-file-size"`
	StoreParallelTableSyncs      int       `toml:"store-parallel-table-syncs"`
	StoreBloomFilterBits         int       `toml:"store-bloom-filter-bits"`
	StoreWALDir                  string    `toml:"store-wal-dir"`
	StoreCacheShards             int       `toml:"store-cache-shards"`
	StoreTableCacheShards        int       `toml:"store-table-cache-shards"`
	StoreCompactOnce             string    `toml:"store-compact-once"`
	StoreReuseBlockKeys          bool      `toml:"store-reuse-block-keys"`
	StoreOOODirectWrites         bool      `toml:"store-ooo-direct-writes"`
	StoreFetchSharedIterator     bool      `toml:"store-fetch-shared-iterator"`
	StoreMetadataCacheSize       int64     `toml:"store-metadata-cache-size"`
	StoreMetadataCachePack       bool      `toml:"store-metadata-cache-pack"`
	StoreUpdatePointLookups      bool      `toml:"store-update-point-lookups"`
	StorePurgeInvalidNames       bool      `toml:"store-purge-invalid-names"`
	StoreBytesPerSync            int       `toml:"store-bytes-per-sync"`
	StoreMigrationHintBurst      *int      `toml:"store-migration-hint-burst"`
	StoreMigrationArchiveDir     string    `toml:"store-migration-archive-dir"`
	StoreMigrationBytesInFlight  int64     `toml:"store-migration-bytes-in-flight"`
	StoreMigrationMinFreeBytes   int64     `toml:"store-migration-min-free-bytes"`
	StoreLiveMigration           bool      `toml:"store-live-migration"`
	StoreMigrationWorkers        int       `toml:"store-migration-workers"`
	StoreMigrationControlFile    string    `toml:"store-migration-control-file"`
	StoreMigrationBaselineCopy   string    `toml:"store-migration-baseline-copy"`
	DataDir                      string    `toml:"data-dir"`
	SchemasFilename              string    `toml:"schemas-file"`
	AggregationFilename          string    `toml:"aggregation-file"`
	QuotasFilename               string    `toml:"quotas-file"`
	Workers                      int       `toml:"workers"`
	MaxUpdatesPerSecond          int       `toml:"max-updates-per-second"`
	Sparse                       bool      `toml:"sparse-create"`
	PhysicalSizeFactor           float32   `toml:"physical-size-factor"`
	FLock                        bool      `toml:"flock"`
	Compressed                   bool      `toml:"compressed"`
	Enabled                      bool      `toml:"enabled"`
	HashFilenames                bool      `toml:"hash-filenames"`
	Schemas                      persister.WhisperSchemas
	Aggregation                  *persister.WhisperAggregation
	Quotas                       persister.WhisperQuotas
	RemoveEmptyFile              bool `toml:"remove-empty-file"`

	QuotasReloadInterval            Duration `toml:"quotas-reload-interval"`
	PebbleChunkIgnorePhysicalQuotas bool     `toml:"pebble-chunk-ignore-physical-quotas"`

	StoreExpiration              *Duration                        `toml:"store-expiration"`
	StoreExpirationFilename      string                           `toml:"store-expiration-file"`
	StoreExpirationCheckInterval *Duration                        `toml:"store-expiration-check-interval"`
	StoreExpirationScanRate      int                              `toml:"store-expiration-scan-rate"`
	StoreExpirationDryRun        bool                             `toml:"store-expiration-dry-run"`
	StoreExpirationDeleteWorkers int                              `toml:"store-expiration-delete-workers"`
	StoreExpirationDeleteRate    int                              `toml:"store-expiration-delete-rate"`
	StoreExpirationMaxDeletes    int                              `toml:"store-expiration-max-deletes"`
	StoreExpirationMaxL0         int                              `toml:"store-expiration-max-l0-sublevels"`
	StoreExpirationMaxDebt       int64                            `toml:"store-expiration-max-compaction-debt"`
	ExpirationRules              persister.WhisperExpirationRules `toml:"-"`

	OnlineMigration            bool   `toml:"online-migration"`
	OnlineMigrationRate        int    `toml:"online-migration-rate"` // metrics per second
	OnlineMigrationGlobalScope string `toml:"online-migration-global-scope"`

	// Compressed files cannot write a point that is older than the newest one
	// already in the block, so late arrivals and backfill are normally
	// discarded (see persister.oooDiscardedPoints). With out-of-order enabled
	// they are diverted to a classic whisper sidecar next to the file
	// and merged back in on read.
	OutOfOrder bool `toml:"out-of-order"`
	// How many metrics per second may fold their sidecar back into the
	// compressed file. With the point policy, this limits scans too.
	OutOfOrderCompactRate int `toml:"out-of-order-compact-rate"`
	// Physical size a sidecar must reach before it is compacted, in bytes.
	OutOfOrderCompactThreshold int64 `toml:"out-of-order-compact-threshold"`
	// Positive MinPoints replaces the physical-size trigger. Age is measured
	// from sample timestamps, not sidecar mtime or arrival time.
	OutOfOrderCompactMinPoints       int      `toml:"out-of-order-compact-min-points"`
	OutOfOrderCompactMaxPointAge     Duration `toml:"out-of-order-compact-max-point-age"`
	OutOfOrderCompactRetentionMargin Duration `toml:"out-of-order-compact-retention-margin"`
	// StoreMetadataCachePrewarm loads the store's metadata cache at start
	// with restored metrics first, then the catalog, on that many
	// workers (0: GOMAXPROCS/12, 2..16) and at up to that many metrics per
	// second (0: unlimited). ByteRate also caps logical key/value bytes read per
	// second (0: unlimited). See App.startMetadataPrewarm.
	// CPUCores paces optional warming against whole-process CPU (0: 1.05x saved
	// steady median). Writeout has priority within it while warming is unfinished.
	// The first rollout needs an explicit allowance until history exists.
	StoreMetadataCachePrewarmCPUCores float64 `toml:"store-metadata-cache-prewarm-cpu-cores"`
	// Checkpoint and warm-first default on with prewarm. Lazy is opt-in.
	StoreMetadataCacheCheckpoint      *bool `toml:"store-metadata-cache-checkpoint"`
	StoreMetadataCacheWarmFirst       *bool `toml:"store-metadata-cache-warm-first"`
	StoreMetadataCacheCheckpointLazy  *bool `toml:"store-metadata-cache-checkpoint-lazy"`
	StoreMetadataCachePrewarm         bool  `toml:"store-metadata-cache-prewarm"`
	StoreMetadataCachePrewarmWorkers  int   `toml:"store-metadata-cache-prewarm-workers"`
	StoreMetadataCachePrewarmRate     int   `toml:"store-metadata-cache-prewarm-rate"`
	StoreMetadataCachePrewarmByteRate int   `toml:"store-metadata-cache-prewarm-byte-rate"`
	// Wait defers persister workers after intake opens until prewarm ends,
	// cache capacity is reached, or this fixed deadline. Nil adapts to scan
	// progress within WaitCap (default 600s); explicit zero disables the gate.
	StoreMetadataCachePrewarmWait    *Duration `toml:"store-metadata-cache-prewarm-wait"`
	StoreMetadataCachePrewarmWaitCap *Duration `toml:"store-metadata-cache-prewarm-wait-cap"`
}

type cacheConfig struct {
	MaxSize           uint64   `toml:"max-size"`
	MaxBytes          uint64   `toml:"max-bytes"`
	WriteStrategy     string   `toml:"write-strategy"`
	BloomSize         uint64   `toml:"bloom-size"`
	WriteoutMinPoints int      `toml:"writeout-min-points"`
	WriteoutMaxDelay  Duration `toml:"writeout-max-delay"`
	// WriteoutSpread shortens each metric's min points and max delay by a
	// stable fraction up to this value; see Cache.SetWriteoutSpread.
	WriteoutSpread float64 `toml:"writeout-max-delay-spread"`
	// WriteoutRestoreSpread spreads restored points' writes over the first
	// max delay instead of writing them at once; see
	// Cache.SetWriteoutRestoreSpread.
	WriteoutRestoreSpread bool `toml:"writeout-restore-spread"`
	// WriteoutSparseSpread spreads writes of metrics that hold one point per
	// writeout window over the whole window; see
	// Cache.SetWriteoutSparseSpread.
	WriteoutSparseSpread bool `toml:"writeout-sparse-spread"`
	// WriteoutStartupSpread holds initial batches to independent phases;
	// see Cache.SetWriteoutStartupSpread.
	WriteoutStartupSpread bool `toml:"writeout-startup-spread"`
}

type carbonlinkConfig struct {
	Listen      string    `toml:"listen"`
	Enabled     bool      `toml:"enabled"`
	ReadTimeout *Duration `toml:"read-timeout"`
}

type grpcConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type tagsConfig struct {
	Enabled             bool      `toml:"enabled"`
	TagDB               string    `toml:"tagdb-url"`
	TagDBTimeout        *Duration `toml:"tagdb-timeout"`
	TagDBChunkSize      int       `toml:"tagdb-chunk-size"`
	TagDBUpdateInterval uint64    `toml:"tagdb-update-interval"`
	LocalDir            string    `toml:"local-dir"`
}

type carbonserverConfig struct {
	Listen                     string     `toml:"listen"`
	Enabled                    bool       `toml:"enabled"`
	Grpc                       grpcConfig `toml:"grpc"`
	ReadTimeout                *Duration  `toml:"read-timeout"`
	IdleTimeout                *Duration  `toml:"idle-timeout"`
	WriteTimeout               *Duration  `toml:"write-timeout"`
	RequestTimeout             *Duration  `toml:"request-timeout"`
	ScanFrequency              *Duration  `toml:"scan-frequency"`
	QueryCacheEnabled          bool       `toml:"query-cache-enabled"`
	StreamingQueryCacheEnabled bool       `toml:"streaming-query-cache-enabled"`
	QueryCacheSizeMB           int        `toml:"query-cache-size-mb"`
	QueryCacheCompression      string     `toml:"query-cache-compression"`
	FindCacheEnabled           bool       `toml:"find-cache-enabled"`
	FindCacheSizeMB            int        `toml:"find-cache-size-mb"`
	GlobCacheEnabled           bool       `toml:"glob-cache-enabled"`
	GlobCacheSizeMB            int        `toml:"glob-cache-size-mb"`
	Buckets                    int        `toml:"buckets"`
	MaxGlobs                   int        `toml:"max-globs"`
	FailOnMaxGlobs             bool       `toml:"fail-on-max-globs"`
	EmptyResultOk              bool       `toml:"empty-result-ok"`
	DoNotLog404s               bool       `toml:"do-not-log-404s"`
	MetricsAsCounters          bool       `toml:"metrics-as-counters"`
	TrigramIndex               bool       `toml:"trigram-index"`
	InternalStatsDir           string     `toml:"internal-stats-dir"`
	Percentiles                []int      `toml:"stats-percentiles"`
	CacheScan                  bool       `toml:"cache-scan"`

	DiscoveryCacheTTL map[string]*Duration `toml:"discovery-cache-ttl"`

	MaxMetricsGlobbed      int `toml:"max-metrics-globbed"`
	MaxMetricsRendered     int `toml:"max-metrics-rendered"`
	MaxFetchDataGoroutines int `toml:"max-fetch-data-goroutines"`

	TrieIndex       bool `toml:"trie-index"`
	ConcurrentIndex bool `toml:"concurrent-index"`
	RealtimeIndex   int  `toml:"realtime-index"`

	FileListCache        string `toml:"file-list-cache"`
	FileListCacheVersion int    `toml:"file-list-cache-version"`
	ScanWorkers          int    `toml:"scan-workers"`

	QuotaUsageReportFrequency *Duration `toml:"quota-usage-report-frequency"`
	MaxCreatesPerSecond       int       `toml:"max-creates-per-second"`

	NoServiceWhenIndexIsNotReady bool `toml:"no-service-when-index-is-not-ready"`

	// TODO: depcreate, replaced by APIPerPathRateLimiters
	MaxInflightRequests uint64 `toml:"max-inflight-requests"`

	// Only applied on filtering phase where it might potentially causes
	// high cpu and memory consumption due to matching over the whole index
	// or file tree.
	HeavyGlobQueryRateLimiters []struct {
		Pattern             string `toml:"pattern"`
		MaxInflightRequests uint   `toml:"max-inflight-requests"`
	} `toml:"heavy-glob-query-rate-limiters"`

	APIPerPathRateLimiters []struct {
		Path                string    `toml:"path"`
		MaxInflightRequests uint      `toml:"max-inflight-requests"`
		RequestTimeout      *Duration `toml:"request-timeout"`
	} `toml:"api-per-path-rate-limiters"`

	RenderTraceLoggingEnabled bool `toml:"render-trace-logging-enabled"`
}

type pprofConfig struct {
	MutexProfileFraction int    `toml:"mutex-profile-fraction"`
	BlockProfileRate     int    `toml:"block-profile-rate"`
	Listen               string `toml:"listen"`
	Enabled              bool   `toml:"enabled"`
}

type dumpConfig struct {
	Enabled           bool   `toml:"enabled"`
	Path              string `toml:"path"`
	RestorePerSecond  int    `toml:"restore-per-second"`
	HandoffSupervisor bool   `toml:"handoff-supervisor"`
	// StoreInputHandoff lets a shared-store instance (no mirrored files) hand
	// its input sockets to a capable successor although reads cannot move;
	// see go-carbon.conf.example.
	StoreInputHandoff bool `toml:"store-input-handoff"`
	// Format of the cache dump a stop writes: "legacy" (the default),
	// "compact", or "blocks" (compact points with compressed names).
	// Restore reads every format. Releases without a format ignore its dump; see
	// go-carbon.conf.example.
	Format string `toml:"format"`
}

// cacheFormat parses Format.
func (c dumpConfig) cacheFormat() (recovery.Format, error) {
	switch c.Format {
	case "", "legacy":
		return recovery.FormatLegacy, nil
	case "compact":
		return recovery.FormatCompact, nil
	case "blocks":
		return recovery.FormatBlocks, nil
	}
	return recovery.FormatLegacy, fmt.Errorf("dump.format must be \"legacy\", \"compact\", or \"blocks\", not %q", c.Format)
}

type prometheusConfig struct {
	Enabled  bool              `toml:"enabled"`
	Endpoint string            `toml:"endpoint"`
	Labels   map[string]string `toml:"labels"`
}

type tracingConfig struct {
	Enabled        bool      `toml:"enabled"`
	JaegerEndpoint string    `toml:"jaegerEndpoint"`
	Stdout         bool      `toml:"stdout"`
	SendTimeout    *Duration `toml:"send_timeout"`
}

// Config ...
type Config struct {
	// memoryBudget and memoryLimit are resolved at config load; an explicit
	// GOMEMLIMIT wins over memoryLimit.
	memoryBudget int64
	memoryLimit  int64
	Buckyd       buckyd.Config                       `toml:"buckyd"`
	Common       commonConfig                        `toml:"common"`
	Whisper      whisperConfig                       `toml:"whisper"`
	Cache        cacheConfig                         `toml:"cache"`
	Udp          *udp.Options                        `toml:"udp"`
	Tcp          *tcp.Options                        `toml:"tcp"`
	Pickle       *tcp.FramingOptions                 `toml:"pickle"`
	Receiver     map[string](map[string]interface{}) `toml:"receiver"`
	Carbonlink   carbonlinkConfig                    `toml:"carbonlink"`
	Grpc         grpcConfig                          `toml:"grpc"`
	Tags         tagsConfig                          `toml:"tags"`
	Carbonserver carbonserverConfig                  `toml:"carbonserver"`
	Dump         dumpConfig                          `toml:"dump"`
	InputLog     inputLogConfig                      `toml:"input-log"`
	Pprof        pprofConfig                         `toml:"pprof"`
	Logging      []zapwriter.Config                  `toml:"logging"`
	Prometheus   prometheusConfig                    `toml:"prometheus"`
	Tracing      tracingConfig                       `toml:"tracing"`
}

func NewLoggingConfig() zapwriter.Config {
	return zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "info",
		Encoding:         "console",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}
}

// NewConfig ...
func NewConfig() *Config {
	cfg := &Config{
		Buckyd: buckyd.Config{
			Bind: "0.0.0.0:4242", Hash: "carbon", Replicas: 1,
			TmpDir: os.TempDir(), Timeout: 3600,
			MaxBodyBytes: 160 << 20, MaxTransfers: 0,
		},
		Common: commonConfig{
			GraphPrefix: "carbon.agents.{host}",
			MetricInterval: &Duration{
				Duration: time.Minute,
			},
			MetricEndpoint: MetricEndpointLocal,
			MaxCPU:         1,
			User:           "carbon",
			HeartbeatInterval: &Duration{
				Duration: 10 * time.Second,
			},
		},
		Whisper: whisperConfig{
			StorageBackend:              "files",
			StoreCacheSize:              256 << 20,
			StoreMemTableSize:           64 << 20,
			StoreSyncInterval:           &Duration{time.Second},
			StoreUpdateBatchSize:        1,
			StoreMigrationIngestLinger:  &Duration{Duration: time.Second},
			StoreEncoding:               "legacy",
			StoreMigrationBytesInFlight: 256 << 20,
			StoreMigrationMinFreeBytes:  8 << 30,
			DataDir:                     "/var/lib/graphite/whisper/",
			SchemasFilename:             "/etc/go-carbon/storage-schemas.conf",
			AggregationFilename:         "",
			Enabled:                     true,
			Workers:                     1,
			Sparse:                      false,
			PhysicalSizeFactor:          0.75,
			FLock:                       false,
			HashFilenames:               true,

			StoreExpiration:              &Duration{0},
			StoreExpirationCheckInterval: &Duration{time.Hour},
			StoreExpirationScanRate:      100_000,
			StoreExpirationDeleteRate:    100,
			StoreExpirationDeleteWorkers: 8,
			StoreExpirationMaxDeletes:    10_000,
			StoreExpirationMaxL0:         6,
			StoreExpirationMaxDebt:       1 << 30,

			OnlineMigration:            false,
			OnlineMigrationRate:        5,
			OnlineMigrationGlobalScope: "",

			OutOfOrder:                       false,
			OutOfOrderCompactRate:            5,
			OutOfOrderCompactThreshold:       65536,
			OutOfOrderCompactMaxPointAge:     Duration{5 * time.Minute},
			OutOfOrderCompactRetentionMargin: Duration{time.Minute},
		},
		Cache: cacheConfig{
			MaxSize:       1000000,
			WriteStrategy: "max",
		},
		Udp:    udp.NewOptions(),
		Tcp:    tcp.NewOptions(),
		Pickle: tcp.NewFramingOptions(),
		Carbonserver: carbonserverConfig{
			Listen:            "127.0.0.1:8080",
			Enabled:           false,
			Buckets:           10,
			MaxGlobs:          100,
			FailOnMaxGlobs:    false,
			MetricsAsCounters: false,
			ScanFrequency: &Duration{
				Duration: 300 * time.Second,
			},
			QuotaUsageReportFrequency: &Duration{
				Duration: 60 * time.Second,
			},
			ReadTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			IdleTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			WriteTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			QueryCacheEnabled:     true,
			QueryCacheSizeMB:      0,
			QueryCacheCompression: "gzip",
			FindCacheEnabled:      true,
			FindCacheSizeMB:       0,
			GlobCacheEnabled:      true,
			GlobCacheSizeMB:       0,
			TrigramIndex:          true,
			CacheScan:             false,
			MaxMetricsGlobbed:     10_000_000,
			MaxMetricsRendered:    1_000_000,
			FileListCacheVersion:  1, // see carbonserver/flc.go
		},
		Carbonlink: carbonlinkConfig{
			Listen:  "127.0.0.1:7002",
			Enabled: true,
			ReadTimeout: &Duration{
				Duration: 30 * time.Second,
			},
		},
		Grpc: grpcConfig{
			Listen:  "127.0.0.1:7003",
			Enabled: true,
		},
		Tags: tagsConfig{
			Enabled: false,
			TagDB:   "http://127.0.0.1:8000",
			TagDBTimeout: &Duration{
				Duration: time.Second,
			},
			TagDBChunkSize:      32,
			TagDBUpdateInterval: 100,
			LocalDir:            "/var/lib/graphite/tagging/",
		},
		Pprof: pprofConfig{
			Listen:  "127.0.0.1:7007",
			Enabled: false,
		},
		Dump: dumpConfig{
			Path: "/var/lib/graphite/dump/",
		},
		InputLog: newInputLogConfig(),
		Prometheus: prometheusConfig{
			Enabled:  false,
			Endpoint: "/metrics",
			Labels:   make(map[string]string),
		},
		Tracing: tracingConfig{
			Enabled: false,
			SendTimeout: &Duration{
				Duration: time.Second * 10,
			},
		},
		Logging: nil,
	}

	return cfg
}

// PrintDefaultConfig ...
func PrintDefaultConfig() error {
	cfg := NewConfig()
	buf := new(bytes.Buffer)

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, NewLoggingConfig())
	}

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	fmt.Print(buf.String())
	return nil
}

// ReadConfig ...
func ReadConfig(filename string) (*Config, error) {
	cfg := NewConfig()
	var defined toml.MetaData
	if filename != "" {
		b, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		body := string(b)

		// @TODO: fix for config starts with [logging]
		body = strings.ReplaceAll(body, "\n[logging]\n", "\n[[logging]]\n")

		if defined, err = toml.Decode(body, cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.resolveMemoryBudget(defined, availableMemory, os.Getenv("GOMEMLIMIT")); err != nil {
		return nil, err
	}

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if cfg.Common.LogLevel != nil || cfg.Common.Logfile != nil {
		log.Println("[WARNING] `common.log-level` and `common.logfile` is DEPRICATED. Use `logging` config section")

		l := NewLoggingConfig()
		if cfg.Common.Logfile != nil {
			l.File = *cfg.Common.Logfile
		}
		if cfg.Common.LogLevel != nil {
			l.Level = *cfg.Common.LogLevel
		}

		cfg.Logging = []zapwriter.Config{l}
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, NewLoggingConfig())
	}

	if err := zapwriter.CheckConfig(cfg.Logging, nil); err != nil {
		return nil, err
	}

	return cfg, nil
}

// TestConfig creates config with all files in root directory
func TestConfig(rootDir string) string {
	cfg := NewConfig()

	cfg.Whisper.DataDir = rootDir
	cfg.Whisper.SchemasFilename = filepath.Join(rootDir, "schemas.conf")
	// cfg.Whisper.Aggregation = filepath.Join(rootDir, "aggregation.conf")

	configFile := filepath.Join(rootDir, "go-carbon.conf")

	buf := new(bytes.Buffer)

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return configFile
	}

	os.WriteFile(cfg.Whisper.SchemasFilename, []byte(`
[default]
priority = 1
pattern = .*
retentions = 60:43200,3600:43800`), 0600)

	os.WriteFile(configFile, buf.Bytes(), 0600)

	return configFile
}

func (c *Config) getCarbonserverQuotas(reportFrequency time.Duration) (quotas []*carbonserver.Quota) {
	minutes := int64(reportFrequency / time.Minute)
	for _, q := range c.Whisper.Quotas {
		if c.Whisper.StorageBackend == "pebble-chunk" && c.Whisper.PebbleChunkIgnorePhysicalQuotas && !c.Whisper.StoreMirrorFiles {
			q.PhysicalSize = 0
		}
		quotas = append(quotas, &carbonserver.Quota{
			Pattern:          q.Pattern,
			Namespaces:       q.Namespaces,
			Metrics:          q.Metrics,
			DataPoints:       q.DataPoints,
			LogicalSize:      q.LogicalSize,
			PhysicalSize:     q.PhysicalSize,
			Throughput:       q.Throughput * minutes,
			DroppingPolicy:   carbonserver.ParseQuotaDroppingPolicy(q.DroppingPolicy),
			StatMetricPrefix: q.StatMetricPrefix,
		})
	}

	return quotas
}
