package carbon

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/go-graphite/go-carbon/receiver/udp"
	"github.com/lomik/zapwriter"
)

const MetricEndpointLocal = "local"

// Duration wrapper time.Duration for TOML
type Duration struct {
	time.Duration
}

var _ toml.TextMarshaler = &Duration{}

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
	User           string    `toml:"user"`
	Logfile        *string   `toml:"logfile"`
	LogLevel       *string   `toml:"log-level"`
	GraphPrefix    string    `toml:"graph-prefix"`
	MetricInterval *Duration `toml:"metric-interval"`
	MetricEndpoint string    `toml:"metric-endpoint"`
	MaxCPU         int       `toml:"max-cpu"`
}

type whisperConfig struct {
	DataDir                 string `toml:"data-dir"`
	SchemasFilename         string `toml:"schemas-file"`
	AggregationFilename     string `toml:"aggregation-file"`
	Workers                 int    `toml:"workers"`
	MaxUpdatesPerSecond     int    `toml:"max-updates-per-second"`
	MaxCreatesPerSecond     int    `toml:"max-creates-per-second"`
	HardMaxCreatesPerSecond bool   `toml:"hard-max-creates-per-second"`
	Sparse                  bool   `toml:"sparse-create"`
	FLock                   bool   `toml:"flock"`
	Compressed              bool   `toml:"compressed"`
	Enabled                 bool   `toml:"enabled"`
	HashFilenames           bool   `toml:"hash-filenames"`
	Schemas                 persister.WhisperSchemas
	Aggregation             *persister.WhisperAggregation
	RemoveEmptyFile         bool `toml:"remove-empty-file"`
}

type cacheConfig struct {
	MaxSize       uint32 `toml:"max-size"`
	WriteStrategy string `toml:"write-strategy"`
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
	Listen            string    `toml:"listen"`
	Enabled           bool      `toml:"enabled"`
	ReadTimeout       *Duration `toml:"read-timeout"`
	IdleTimeout       *Duration `toml:"idle-timeout"`
	WriteTimeout      *Duration `toml:"write-timeout"`
	ScanFrequency     *Duration `toml:"scan-frequency"`
	QueryCacheEnabled bool      `toml:"query-cache-enabled"`
	QueryCacheSizeMB  int       `toml:"query-cache-size-mb"`
	FindCacheEnabled  bool      `toml:"find-cache-enabled"`
	Buckets           int       `toml:"buckets"`
	MaxGlobs          int       `toml:"max-globs"`
	FailOnMaxGlobs    bool      `toml:"fail-on-max-globs"`
	MetricsAsCounters bool      `toml:"metrics-as-counters"`
	TrigramIndex      bool      `toml:"trigram-index"`
	InternalStatsDir  string    `toml:"internal-stats-dir"`
	Percentiles       []int     `toml:"stats-percentiles"`
	CacheScan         bool      `toml:"cache-scan"`

	MaxMetricsGlobbed  int `toml:"max-metrics-globbed"`
	MaxMetricsRendered int `toml:"max-metrics-rendered"`

	TrieIndex       bool   `toml:"trie-index"`
	ConcurrentIndex bool   `toml:"concurrent-index"`
	RealtimeIndex   int    `toml:"realtime-index"`
	FileListCache   string `toml:"file-list-cache"`
}

type pprofConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type dumpConfig struct {
	Enabled          bool   `toml:"enabled"`
	Path             string `toml:"path"`
	RestorePerSecond int    `toml:"restore-per-second"`
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
	Pprof        pprofConfig                         `toml:"pprof"`
	Logging      []zapwriter.Config                  `toml:"logging"`
	Prometheus   prometheusConfig                    `toml:"prometheus"`
	Tracing      tracingConfig                       `toml:"tracing"`
}

func NewLoggingConfig() zapwriter.Config {
	cfg := zapwriter.NewConfig()
	cfg.File = "/var/log/go-carbon/go-carbon.log"
	return cfg
}

// NewConfig ...
func NewConfig() *Config {
	cfg := &Config{
		Common: commonConfig{
			GraphPrefix: "carbon.agents.{host}",
			MetricInterval: &Duration{
				Duration: time.Minute,
			},
			MetricEndpoint: MetricEndpointLocal,
			MaxCPU:         1,
			User:           "carbon",
		},
		Whisper: whisperConfig{
			DataDir:             "/var/lib/graphite/whisper/",
			SchemasFilename:     "/etc/go-carbon/storage-schemas.conf",
			AggregationFilename: "",
			MaxUpdatesPerSecond: 0,
			Enabled:             true,
			Workers:             1,
			Sparse:              false,
			FLock:               false,
			HashFilenames:       true,
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
			ReadTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			IdleTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			WriteTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			QueryCacheEnabled:  true,
			QueryCacheSizeMB:   0,
			FindCacheEnabled:   true,
			TrigramIndex:       true,
			CacheScan:          false,
			MaxMetricsGlobbed:  10_000_000,
			MaxMetricsRendered: 1_000_000,
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
	if filename != "" {
		b, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		body := string(b)

		// @TODO: fix for config starts with [logging]
		body = strings.ReplaceAll(body, "\n[logging]\n", "\n[[logging]]\n")

		if _, err := toml.Decode(body, cfg); err != nil {
			return nil, err
		}
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

	ioutil.WriteFile(cfg.Whisper.SchemasFilename, []byte(`
[default]
priority = 1
pattern = .*
retentions = 60:43200,3600:43800`), 0644)

	ioutil.WriteFile(configFile, buf.Bytes(), 0644)

	return configFile
}
