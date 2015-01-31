package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/BurntSushi/toml"

	"devroom.ru/lomik/carbon"
)

type whisperConfig struct {
	DataDir string `toml:"data-dir"`
	Schemas string `toml:"data-file"`
	Enabled bool   `toml:"enabled"`
}

type udpConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type tcpConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

// Config ...
type Config struct {
	Logfile string               `toml:"logfile"`
	Whisper whisperConfig        `toml:"whisper"`
	Udp     map[string]udpConfig `toml:"udp"`
	Tcp     map[string]tcpConfig `toml:"tcp"`
}

func newConfig() *Config {
	cfg := &Config{
		Logfile: "",
		Whisper: whisperConfig{
			DataDir: "/data/graphite/whisper/",
			Schemas: "/data/graphite/schemas",
			Enabled: true,
		},
		Udp: map[string]udpConfig{
			"default": udpConfig{
				Listen:  ":2003",
				Enabled: true,
			},
		},
		Tcp: map[string]tcpConfig{
			"default": tcpConfig{
				Listen:  ":2003",
				Enabled: true,
			},
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

	// pp.Println(cfg)
	/* CONFIG end */

	cache := carbon.NewCache()
	cache.Start()
	defer cache.Stop()

	/* UDP start */
	for _, udpCfg := range cfg.Udp {
		if udpCfg.Enabled {
			udpAddr, err := net.ResolveUDPAddr("udp", udpCfg.Listen)
			if err != nil {
				log.Fatal(err)
			}

			udpListener := carbon.NewUDPReceiver(cache.In())
			defer udpListener.Stop()
			if err = udpListener.Listen(udpAddr); err != nil {
				log.Fatal(err)
			}
		}
	}
	/* UDP end */

	/* TCP start */
	for _, tcpCfg := range cfg.Tcp {
		if tcpCfg.Enabled {
			tcpAddr, err := net.ResolveTCPAddr("tcp", tcpCfg.Listen)
			if err != nil {
				log.Fatal(err)
			}

			tcpListener := carbon.NewTCPReceiver(cache.In())
			defer tcpListener.Stop()
			if err = tcpListener.Listen(tcpAddr); err != nil {
				log.Fatal(err)
			}
		}
	}
	/* TCP end */

	/* WHISPER start */
	if cfg.Whisper.Enabled {
		whisperSchemas, err := carbon.ReadWhisperSchemas(cfg.Whisper.Schemas)
		if err != nil {
			log.Fatal(err)
		}

		whisperPersister := carbon.NewWhisperPersister(cfg.Whisper.DataDir, whisperSchemas, cache.Out())

		whisperPersister.Start()
		defer whisperPersister.Stop()
	}
	/* WHISPER end */

	select {}
}
