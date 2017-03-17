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
	"github.com/lomik/go-carbon/persister"
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
	DataDir             string `toml:"data-dir"`
	SchemasFilename     string `toml:"schemas-file"`
	AggregationFilename string `toml:"aggregation-file"`
	Workers             int    `toml:"workers"`
	MaxUpdatesPerSecond int    `toml:"max-updates-per-second"`
	Sparse              bool   `toml:"sparse-create"`
	Enabled             bool   `toml:"enabled"`
	Schemas             persister.WhisperSchemas
	Aggregation         *persister.WhisperAggregation
}

type cacheConfig struct {
	MaxSize       uint32 `toml:"max-size"`
	WriteStrategy string `toml:"write-strategy"`
}

type udpConfig struct {
	Listen        string `toml:"listen"`
	Enabled       bool   `toml:"enabled"`
	LogIncomplete bool   `toml:"log-incomplete"`
	BufferSize    int    `toml:"buffer-size"`
}

type tcpConfig struct {
	Listen     string `toml:"listen"`
	Enabled    bool   `toml:"enabled"`
	BufferSize int    `toml:"buffer-size"`
}

type pickleConfig struct {
	Listen         string `toml:"listen"`
	MaxMessageSize int    `toml:"max-message-size"`
	Enabled        bool   `toml:"enabled"`
	BufferSize     int    `toml:"buffer-size"`
}

type carbonlinkConfig struct {
	Listen      string    `toml:"listen"`
	Enabled     bool      `toml:"enabled"`
	ReadTimeout *Duration `toml:"read-timeout"`
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
	MetricsAsCounters bool      `toml:"metrics-as-counters"`
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

// Config ...
type Config struct {
	Common       commonConfig       `toml:"common"`
	Whisper      whisperConfig      `toml:"whisper"`
	Cache        cacheConfig        `toml:"cache"`
	Udp          udpConfig          `toml:"udp"`
	Tcp          tcpConfig          `toml:"tcp"`
	Pickle       pickleConfig       `toml:"pickle"`
	Carbonlink   carbonlinkConfig   `toml:"carbonlink"`
	Carbonserver carbonserverConfig `toml:"carbonserver"`
	Dump         dumpConfig         `toml:"dump"`
	Pprof        pprofConfig        `toml:"pprof"`
	Logging      []zapwriter.Config `toml:"logging"`
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
			User:           "",
		},
		Whisper: whisperConfig{
			DataDir:             "/data/graphite/whisper/",
			SchemasFilename:     "/data/graphite/schemas",
			AggregationFilename: "",
			MaxUpdatesPerSecond: 0,
			Enabled:             true,
			Workers:             1,
			Sparse:              false,
		},
		Cache: cacheConfig{
			MaxSize:       1000000,
			WriteStrategy: "max",
		},
		Udp: udpConfig{
			Listen:        ":2003",
			Enabled:       true,
			LogIncomplete: false,
		},
		Tcp: tcpConfig{
			Listen:  ":2003",
			Enabled: true,
		},
		Pickle: pickleConfig{
			Listen:         ":2004",
			Enabled:        true,
			MaxMessageSize: 67108864, // 64 Mb
		},
		Carbonserver: carbonserverConfig{
			Listen:            "127.0.0.1:8080",
			Enabled:           false,
			Buckets:           10,
			MaxGlobs:          100,
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
			QueryCacheEnabled: true,
			QueryCacheSizeMB:  0,
			FindCacheEnabled:  true,
		},
		Carbonlink: carbonlinkConfig{
			Listen:  "127.0.0.1:7002",
			Enabled: true,
			ReadTimeout: &Duration{
				Duration: 30 * time.Second,
			},
		},
		Pprof: pprofConfig{
			Listen:  "localhost:7007",
			Enabled: false,
		},
		Dump:    dumpConfig{},
		Logging: nil,
	}

	return cfg
}

// PrintConfig ...
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
		body = strings.Replace(body, "\n[logging]\n", "\n[[logging]]\n", -1)

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

	p := filepath.Join(rootDir, "go-carbon.log")
	cfg.Common.Logfile = &p

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
