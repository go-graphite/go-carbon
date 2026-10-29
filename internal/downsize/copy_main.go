package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/internal/storecopy"
)

func CopyMain(args []string) error {
	flags := flag.NewFlagSet("resize snapshot", flag.ContinueOnError)
	action := flags.String("action", "status", "begin, status, capture, seal, pull, release, fixture")
	source := flags.String("source", "http://127.0.0.1:4242", "source Buckyd URL")
	id := flags.String("id", "", "checkpoint ID, required after begin")
	dest := flags.String("destination", "", "empty staging directory on this host")
	tokenFile := flags.String("token-file", "", "file holding a Buckyd whole-store admin JWT")
	workers := flags.Int("workers", 0, "parallel streams, 1..64; zero lets the controller use up to 64")
	chunk := flags.Int64("chunk-bytes", 16<<20, "range size, 1..64 MiB")
	rate := flags.Int64("bytes-per-second", 1_000_000_000, "aggregate transfer ceiling; 0 unlimited")
	lease := flags.String("lease", "", "expiring adaptive controller lease file")
	apply := flags.Bool("apply", false, "perform the printed actions; default is dry-run")
	fenced := flags.Bool("input-fenced", false, "relay input is paused and its old-host connections are drained; required for seal")
	preflight := flags.String("preflight", "", "target preflight.json; required for applied pull")
	benchmark := flags.Bool("local-benchmark", false, "allow unguarded loopback-only measurements")
	reuseVerified := flags.Bool("reuse-verified", false, "reuse synced checksum proofs for files with unchanged inode, size, mtime and ctime")
	profile := flags.String("cpuprofile", "", "write local CPU profile")
	metrics := flags.Int("metrics", 1_000_000, "fixture metric count")
	points := flags.Int("points", 48, "fixture points per metric")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *workers == 0 {
		*workers = 64
	}
	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *action == "serve-offline" {
		parsed, err := url.Parse(*source)
		if err != nil {
			return err
		}
		ip := net.ParseIP(parsed.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return errors.New("offline benchmark server must bind a loopback IP")
		}
		handler, manifest, err := storecopy.OpenOffline(*dest)
		if err != nil {
			return err
		}
		listener, err := net.Listen("tcp", parsed.Host)
		if err != nil {
			return err
		}
		defer listener.Close()
		server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
		defer server.Close()
		if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"url": "http://" + listener.Addr().String(), "id": manifest.ID}); err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		select {
		case <-ctx.Done():
			return nil
		case err := <-done:
			return err
		}
	}
	if *action == "fixture" {
		fmt.Printf("fixture destination=%s metrics=%d points=%d apply=%t\n", *dest, *metrics, *points, *apply)
		if !*apply {
			return nil
		}
		return fixture(ctx, *dest, *metrics, *points)
	}
	client := &storecopy.Client{URL: *source, HTTP: &http.Client{
		Timeout:   2 * time.Minute,
		Transport: &http.Transport{MaxIdleConns: 128, MaxIdleConnsPerHost: *workers, ResponseHeaderTimeout: 30 * time.Second},
	}}
	defer client.HTTP.CloseIdleConnections()
	if *tokenFile != "" {
		data, err := os.ReadFile(*tokenFile)
		if err != nil {
			return err
		}
		client.Token = strings.TrimSpace(string(data))
	}
	if *action != "begin" && *id == "" {
		return errors.New("--id is required")
	}
	if *action == "pull" {
		if *dest == "" {
			return errors.New("--destination is required")
		}
		if *benchmark {
			parsed, err := url.Parse(*source)
			if err != nil {
				return err
			}
			ip := net.ParseIP(parsed.Hostname())
			if ip == nil || !ip.IsLoopback() {
				return errors.New("local benchmark requires a loopback IP")
			}
		}
		if *apply && !*benchmark {
			if *lease == "" {
				return errors.New("applied copy requires an adaptive --lease; use loopback --local-benchmark for isolated measurements")
			}
			if err := checkPreflight(*preflight, *dest); err != nil {
				return err
			}
			var report struct {
				ConfigPath string `json:"config_path"`
			}
			data, err := os.ReadFile(*preflight)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &report); err != nil {
				return err
			}
			manifest, err := client.Manifest(ctx, "status", *id)
			if err != nil {
				return err
			}
			if err := checkIdentity(report.ConfigPath, *dest, manifest); err != nil {
				return err
			}
		}
		started := time.Now()
		reserve := int64(0)
		if *preflight != "" {
			var report struct {
				Reserve int64 `json:"disk_reserve_bytes"`
			}
			data, err := os.ReadFile(*preflight)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &report); err != nil {
				return err
			}
			reserve = report.Reserve
		}
		err := client.Pull(ctx, *dest, *id, storecopy.PullOptions{Workers: *workers, Chunk: *chunk, Rate: *rate, Lease: *lease, Apply: *apply, Output: os.Stdout, Reserve: reserve, ReuseVerified: *reuseVerified})
		fmt.Printf("copy elapsed_seconds=%.3f error=%v\n", time.Since(started).Seconds(), err)
		return err
	}
	if *action != "status" {
		fmt.Fprintf(os.Stderr, "copy action=%s source=%s id=%s apply=%t\n", *action, *source, *id, *apply)
		if !*apply {
			return nil
		}
	}
	if *action == "seal" && !*fenced {
		return errors.New("seal requires --input-fenced after relay queue pause and connection drain")
	}
	manifest, err := client.Manifest(ctx, *action, *id)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(manifest)
}

func checkIdentity(configPath, destination string, manifest *storecopy.Manifest) error {
	// Offline loopback fixtures do not carry an ingestion identity.
	if manifest.Metadata == nil {
		return nil
	}
	var config struct {
		Buckyd struct {
			Node     string   `toml:"node"`
			Hash     string   `toml:"hash"`
			Replicas int      `toml:"replicas"`
			Nodes    []string `toml:"nodes"`
		} `toml:"buckyd"`
		Whisper struct {
			Store       string `toml:"store-dir"`
			Data        string `toml:"data-dir"`
			Encoding    string `toml:"store-encoding"`
			Incremental bool   `toml:"store-incremental-migration"`
			Schemas     string `toml:"schemas-file"`
			Aggregation string `toml:"aggregation-file"`
			Quotas      string `toml:"quotas-file"`
		} `toml:"whisper"`
		Input struct {
			Path string `toml:"path"`
		} `toml:"input-log"`
	}
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		return err
	}
	switch config.Buckyd.Hash {
	case "", "carbon_ch":
		config.Buckyd.Hash = "carbon"
	case "fnv1a_ch":
		config.Buckyd.Hash = "fnv1a"
	case "jump_fnv1a_ch":
		config.Buckyd.Hash = "jump_fnv1a"
	}
	if config.Buckyd.Replicas == 0 {
		config.Buckyd.Replicas = 1
	}
	if config.Buckyd.Node != manifest.Metadata["node"] || config.Buckyd.Hash != manifest.Metadata["hash"] || float64(config.Buckyd.Replicas) != manifest.Metadata["replicas"] {
		return errors.New("target Buckyd node, hash and replicas differ from the planned identity")
	}
	nodes, _ := manifest.Metadata["nodes"].([]any)
	if len(nodes) != len(config.Buckyd.Nodes) {
		return errors.New("target has a different ingestion ring node list")
	}
	for i, node := range nodes {
		if node != config.Buckyd.Nodes[i] {
			return errors.New("target ingestion ring node order differs")
		}
	}
	if config.Whisper.Encoding != manifest.Metadata["encoding"] || config.Whisper.Incremental != manifest.Metadata["incremental"] {
		return errors.New("target store encoding/conversion mode differs")
	}
	if config.Whisper.Store != filepath.Join(destination, "store") || config.Input.Path != filepath.Join(destination, "input-log") {
		return errors.New("target store/input-log paths must point to this staged copy")
	}
	if config.Whisper.Incremental && config.Whisper.Data != filepath.Join(destination, "empty-originals") {
		return errors.New("converted target must use its prepared empty-originals directory")
	}
	policies := map[string]string{"config/storage-schemas.conf": config.Whisper.Schemas, "config/storage-aggregation.conf": config.Whisper.Aggregation, "config/storage-quotas.conf": config.Whisper.Quotas}
	for _, file := range manifest.Files {
		if path, ok := policies[file.Name]; ok && path != filepath.Join(destination, filepath.FromSlash(file.Name)) {
			return fmt.Errorf("target must use captured policy %s", file.Name)
		}
	}
	return nil
}

func checkPreflight(path, destination string) error {
	var report struct {
		Hostname     string    `json:"hostname"`
		Destination  string    `json:"destination"`
		Expires      time.Time `json:"expires"`
		Passed       bool      `json:"passed"`
		ConfigPath   string    `json:"config_path"`
		ConfigSHA256 string    `json:"config_sha256"`
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("preflight report: %w", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		return err
	}
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	if !report.Passed || host != report.Hostname || destination != report.Destination || !time.Now().Before(report.Expires) {
		return errors.New("preflight failed, expired, or belongs to a different target")
	}
	config, err := os.ReadFile(report.ConfigPath)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(config)
	if hex.EncodeToString(sum[:]) != report.ConfigSHA256 {
		return errors.New("target configuration changed after preflight")
	}
	return nil
}
