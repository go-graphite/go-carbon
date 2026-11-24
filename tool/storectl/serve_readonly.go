package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type readOnlyConfig struct {
	Whisper struct {
		StorageBackend string `toml:"storage-backend"`
		DataDir        string `toml:"data-dir"`
		StoreDir       string `toml:"store-dir"`
		StoreEncoding  string `toml:"store-encoding"`
	}
	Buckyd buckyd.Config
}

func openReadOnlySource(config string) (*chunkstore.Store, buckyd.Config, error) {
	var cfg readOnlyConfig
	if _, err := toml.DecodeFile(config, &cfg); err != nil {
		return nil, cfg.Buckyd, err
	}
	if cfg.Whisper.StorageBackend != "pebble-chunk" {
		return nil, cfg.Buckyd, errors.New("serve-readonly requires storage-backend = pebble-chunk")
	}
	directory := cfg.Whisper.StoreDir
	if directory == "" {
		suffix := ".store-chunks"
		if cfg.Whisper.StoreEncoding == "exact" {
			suffix = ".store-exact-blocks"
		}
		directory = filepath.Join(cfg.Whisper.DataDir, suffix)
	}
	if !filepath.IsAbs(directory) {
		return nil, cfg.Buckyd, errors.New("absolute store directory required")
	}
	marker, err := os.ReadFile(filepath.Join(directory, "CHUNKSTORE"))
	if err != nil {
		return nil, cfg.Buckyd, err
	}
	version, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(string(marker)), "go-carbon-chunks-v"))
	if err != nil || version < 1 {
		return nil, cfg.Buckyd, errors.New("invalid chunkstore format marker")
	}
	options := chunkstore.Options{ReadOnly: true, ExactBlocks: version >= 4, CacheSize: 128 << 20, MetadataCacheSize: 1 << 20, NativeExportConcurrency: 1}
	if version >= 6 {
		options.Continuation = &whisperio.ContinuationCodec{}
		options.TimeBuckets = true
	}
	// Retain and verify the original binding, as in store-readonly. Never infer
	// a new source from today's config or create an incremental identity token.
	binding, err := os.ReadFile(filepath.Join(directory, "INCREMENTAL-FILE-MIGRATION"))
	if err == nil {
		parts := strings.Split(string(binding), "\n")
		if len(parts) != 4 || (parts[0] != "go-carbon-incremental-v1" && parts[0] != "go-carbon-incremental-v2") || !filepath.IsAbs(parts[1]) {
			return nil, cfg.Buckyd, errors.New("invalid incremental source binding")
		}
		options.IncrementalSource = parts[1]
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, cfg.Buckyd, err
	}
	db, err := chunkstore.Open(directory, options)
	if err != nil {
		return nil, cfg.Buckyd, fmt.Errorf("open stopped source read-only: %w", err)
	}
	if !db.ReadOnly() {
		_ = db.Close()
		return nil, cfg.Buckyd, errors.New("store did not open read-only")
	}
	return db, cfg.Buckyd, nil
}

func serveReadOnly(args []string) error {
	flags := flag.NewFlagSet("storectl serve-readonly", flag.ContinueOnError)
	config := flags.String("config", "", "go-carbon configuration for the stopped store (required)")
	listen := flags.String("listen", "127.0.0.1:4243", "source API listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *config == "" || flags.NArg() != 0 || *listen == "" {
		return errors.New("usage: storectl serve-readonly --config FILE [--listen ADDR]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runReadOnlySource(ctx, *config, *listen)
}

func runReadOnlySource(ctx context.Context, config, listen string) (err error) {
	db, cfg, err := openReadOnlySource(config)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	// This command owns one source listener. Never start configured profiling or
	// daemon services; in particular do not initialize a carbon App.
	cfg.Bind, cfg.Pprof, cfg.Pyroscope = listen, "", ""
	cfg.WaitForJWTSecret = false
	service, err := buckyd.New(cfg, db)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, service.Stop()) }()
	if err := service.Start(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "storectl read-only source listening on %s\n", listen)
	<-ctx.Done()
	return nil
}
