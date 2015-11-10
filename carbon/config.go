package carbon

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
)

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
	MaxCPU         int       `toml:"max-cpu"`
}

type whisperConfig struct {
	DataDir             string `toml:"data-dir"`
	Schemas             string `toml:"schemas-file"`
	Aggregation         string `toml:"aggregation-file"`
	Workers             int    `toml:"workers"`
	MaxUpdatesPerSecond int    `toml:"max-updates-per-second"`
	Enabled             bool   `toml:"enabled"`
}

type cacheConfig struct {
	MaxSize     int `toml:"max-size"`
	InputBuffer int `toml:"input-buffer"`
}

type udpConfig struct {
	Listen        string `toml:"listen"`
	Enabled       bool   `toml:"enabled"`
	LogIncomplete bool   `toml:"log-incomplete"`
}

type tcpConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type carbonlinkConfig struct {
	Listen       string    `toml:"listen"`
	Enabled      bool      `toml:"enabled"`
	ReadTimeout  *Duration `toml:"read-timeout"`
	QueryTimeout *Duration `toml:"query-timeout"`
}

type pprofConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

// Config ...
type Config struct {
	Common     commonConfig     `toml:"common"`
	Whisper    whisperConfig    `toml:"whisper"`
	Cache      cacheConfig      `toml:"cache"`
	Udp        udpConfig        `toml:"udp"`
	Tcp        tcpConfig        `toml:"tcp"`
	Pickle     tcpConfig        `toml:"pickle"`
	Carbonlink carbonlinkConfig `toml:"carbonlink"`
	Pprof      pprofConfig      `toml:"pprof"`
}

// NewConfig ...
func NewConfig() *Config {
	cfg := &Config{
		Common: commonConfig{
			Logfile:     "/var/log/go-carbon/go-carbon.log",
			LogLevel:    "info",
			GraphPrefix: "carbon.agents.{host}.",
			MetricInterval: &Duration{
				Duration: time.Minute,
			},
			MaxCPU: 1,
			User:   "",
		},
		Whisper: whisperConfig{
			DataDir:             "/data/graphite/whisper/",
			Schemas:             "/data/graphite/schemas",
			Aggregation:         "",
			MaxUpdatesPerSecond: 0,
			Enabled:             true,
			Workers:             1,
		},
		Cache: cacheConfig{
			MaxSize:     1000000,
			InputBuffer: 51200,
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
		Pickle: tcpConfig{
			Listen:  ":2004",
			Enabled: true,
		},
		Carbonlink: carbonlinkConfig{
			Listen:  "127.0.0.1:7002",
			Enabled: true,
			ReadTimeout: &Duration{
				Duration: 30 * time.Second,
			},
			QueryTimeout: &Duration{
				Duration: 100 * time.Millisecond,
			},
		},
		Pprof: pprofConfig{
			Listen:  "localhost:7007",
			Enabled: false,
		},
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
	cfg.Whisper.Schemas = filepath.Join(rootDir, "schemas.conf")
	// cfg.Whisper.Aggregation = filepath.Join(rootDir, "aggregation.conf")

	configFile := filepath.Join(rootDir, "go-carbon.conf")

	buf := new(bytes.Buffer)

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return configFile
	}

	ioutil.WriteFile(cfg.Whisper.Schemas, []byte(`
[default]
priority = 1
pattern = .*
retentions = 60:43200,3600:43800`), 0644)

	ioutil.WriteFile(configFile, buf.Bytes(), 0644)

	return configFile
}
