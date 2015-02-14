package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"devroom.ru/lomik/carbon/cache"
	"devroom.ru/lomik/carbon/logging"
	"devroom.ru/lomik/carbon/persister"
	"devroom.ru/lomik/carbon/receiver"

	"github.com/BurntSushi/toml"
	"github.com/Sirupsen/logrus"
)

import _ "net/http/pprof"

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
	Logfile     string `toml:"logfile"`
	GraphPrefix string `toml:"graph-prefix"`
	MaxCPU      int    `toml:"max-cpu"`
}

type whisperConfig struct {
	DataDir string `toml:"data-dir"`
	Schemas string `toml:"schemas-file"`
	Workers int    `toml:"workers"`
	Enabled bool   `toml:"enabled"`
}

type cacheConfig struct {
	MaxSize     int `toml:"max-size"`
	InputBuffer int `toml:"input-buffer"`
}

type udpConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
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
	Carbonlink carbonlinkConfig `toml:"carbonlink"`
	Pprof      pprofConfig      `toml:"pprof"`
}

func newConfig() *Config {
	cfg := &Config{
		Common: commonConfig{
			Logfile:     "",
			GraphPrefix: "carbon.agents.{host}.",
			MaxCPU:      1,
		},
		Whisper: whisperConfig{
			DataDir: "/data/graphite/whisper/",
			Schemas: "/data/graphite/schemas",
			Enabled: true,
			Workers: 1,
		},
		Cache: cacheConfig{
			MaxSize:     1000000,
			InputBuffer: 51200,
		},
		Udp: udpConfig{
			Listen:  ":2003",
			Enabled: true,
		},
		Tcp: tcpConfig{
			Listen:  ":2003",
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

func main() {

	/* CONFIG start */
	configFile := flag.String("config", "", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")

	flag.Parse()

	cfg := newConfig()

	if *printDefaultConfig {
		if err := PrintConfig(cfg); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}

	if err := ParseConfig(*configFile, cfg); err != nil {
		log.Fatal(err)
	}

	logging.SetFile(cfg.Common.Logfile)
	logrus.SetLevel(logrus.DebugLevel)

	runtime.GOMAXPROCS(cfg.Common.MaxCPU)

	// pp.Println(cfg)
	/* CONFIG end */

	// pprof
	if cfg.Pprof.Enabled {
		go func() {
			logrus.Fatal(http.ListenAndServe(cfg.Pprof.Listen, nil))
		}()
	}

	// carbon-cache prefix
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.Replace(hostname, ".", "_", -1)
		cfg.Common.GraphPrefix = strings.Replace(cfg.Common.GraphPrefix, "{host}", hostname, -1)
	} else {
		cfg.Common.GraphPrefix = strings.Replace(cfg.Common.GraphPrefix, "{host}", "localhost", -1)
	}

	core := cache.New()
	core.SetGraphPrefix(cfg.Common.GraphPrefix)
	core.SetMaxSize(cfg.Cache.MaxSize)
	core.SetInputCapacity(cfg.Cache.InputBuffer)
	core.Start()
	defer core.Stop()

	/* UDP start */
	udpCfg := cfg.Udp
	if udpCfg.Enabled {
		udpAddr, err := net.ResolveUDPAddr("udp", udpCfg.Listen)
		if err != nil {
			log.Fatal(err)
		}

		udpListener := receiver.NewUDP(core.In())
		udpListener.SetGraphPrefix(cfg.Common.GraphPrefix)

		defer udpListener.Stop()
		if err = udpListener.Listen(udpAddr); err != nil {
			log.Fatal(err)
		}
	}
	/* UDP end */

	/* TCP start */
	tcpCfg := cfg.Tcp

	if tcpCfg.Enabled {
		tcpAddr, err := net.ResolveTCPAddr("tcp", tcpCfg.Listen)
		if err != nil {
			log.Fatal(err)
		}

		tcpListener := receiver.NewTCP(core.In())
		tcpListener.SetGraphPrefix(cfg.Common.GraphPrefix)

		defer tcpListener.Stop()
		if err = tcpListener.Listen(tcpAddr); err != nil {
			log.Fatal(err)
		}
	}
	/* TCP end */

	/* WHISPER start */
	if cfg.Whisper.Enabled {
		whisperSchemas, err := persister.ReadWhisperSchemas(cfg.Whisper.Schemas)
		if err != nil {
			log.Fatal(err)
		}

		whisperPersister := persister.NewWhisper(cfg.Whisper.DataDir, whisperSchemas, core.Out())
		whisperPersister.SetGraphPrefix(cfg.Common.GraphPrefix)
		whisperPersister.SetWorkers(cfg.Whisper.Workers)

		whisperPersister.Start()
		defer whisperPersister.Stop()
	}
	/* WHISPER end */

	/* CARBONLINK start */
	if cfg.Carbonlink.Enabled {
		linkAddr, err := net.ResolveTCPAddr("tcp", cfg.Carbonlink.Listen)
		if err != nil {
			log.Fatal(err)
		}

		carbonlink := cache.NewCarbonlinkListener(core.Query())
		carbonlink.SetReadTimeout(cfg.Carbonlink.ReadTimeout.Value())
		carbonlink.SetQueryTimeout(cfg.Carbonlink.QueryTimeout.Value())

		defer carbonlink.Stop()
		if err = carbonlink.Listen(linkAddr); err != nil {
			log.Fatal(err)
		}

	}
	/* CARBONLINK end */

	logrus.Info("started")
	select {}
}
