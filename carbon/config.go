package carbon

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/lomik/go-carbon/persister"
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
	Logfile        string    `toml:"logfile"`
	LogLevel       string    `toml:"log-level"`
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
	WriteTimeout      *Duration `toml:"write-timeout"`
	ScanFrequency     *Duration `toml:"scan-frequency"`
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
}

// NewConfig ...
func NewConfig() *Config {
	cfg := &Config{
		Common: commonConfig{
			Logfile:     "/var/log/go-carbon/go-carbon.log",
			LogLevel:    "info",
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
			WriteTimeout: &Duration{
				Duration: 60 * time.Second,
			},
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
		Dump: dumpConfig{},
	}

	return cfg
}

// PrintConfig ...
func PrintConfig(cfg interface{}) error {
	buf := new(bytes.Buffer)

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	fmt.Print(buf.String())
	return nil
}

// ParseConfig ...
func ParseConfig(filename string, cfg interface{}) error {
	if filename != "" {
		if _, err := toml.DecodeFile(filename, cfg); err != nil {
			return err
		}
	}
	return nil
}

// TestConfig creates config with all files in root directory
func TestConfig(rootDir string) string {
	cfg := NewConfig()

	cfg.Common.Logfile = filepath.Join(rootDir, "go-carbon.log")

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
