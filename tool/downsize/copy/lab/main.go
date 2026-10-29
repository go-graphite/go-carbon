// Local-only live source for whole-store copy throughput measurements.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	httpprof "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/carbon"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func address() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	defer listener.Close()
	return listener.Addr().String()
}
func cpu() float64 {
	var usage syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	return float64(usage.Utime.Nano()+usage.Stime.Nano()) / 1e9
}
func main() {
	root := flag.String("root", "", "canonical cloned fixture containing live/")
	cpus := flag.Int("cpus", 4, "source CPU budget")
	flag.Parse()
	if *root == "" {
		panic("--root required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	path := carbon.TestConfig(*root)
	cfg, err := carbon.ReadConfig(path)
	must(err)
	cfg.Common.MaxCPU = *cpus
	cfg.Common.MetricInterval = &carbon.Duration{Duration: time.Hour}
	cfg.Udp.Enabled, cfg.Pickle.Enabled, cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false, false, false
	cfg.Pprof.Enabled = false
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, address()
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreDir = filepath.Join(*root, "live")
	cfg.Whisper.DataDir = filepath.Join(*root, "originals")
	cfg.Whisper.StoreCacheSize = 256 << 20
	cfg.Whisper.StoreMetadataCacheSize = 128 << 20
	cfg.Whisper.StoreMemTableSize = 8 << 20
	cfg.Whisper.StoreCompactionWorkers = 2
	cfg.Whisper.Workers = 8
	cfg.Whisper.MaxUpdatesPerSecond = 1000000
	cfg.Whisper.StoreSyncInterval = &carbon.Duration{Duration: time.Second}
	cfg.InputLog.Enabled = true
	cfg.InputLog.Path = filepath.Join(*root, "input-log")
	cfg.InputLog.SyncInterval = carbon.Duration{Duration: 10 * time.Millisecond}
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.Buckyd.Enabled, cfg.Buckyd.Bind = true, address()
	cfg.Buckyd.CopyMinFreeBytes = 1
	cfg.Carbonserver.Enabled, cfg.Carbonserver.Listen = true, address()
	cfg.Carbonserver.TrieIndex, cfg.Carbonserver.TrigramIndex = true, false
	cfg.Carbonserver.ConcurrentIndex = true
	cfg.Carbonserver.RealtimeIndex = 100
	cfg.Carbonserver.ScanFrequency = &carbon.Duration{Duration: time.Hour}
	must(os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:1d\n"), 0600))
	f, err := os.Create(path)
	must(err)
	must(toml.NewEncoder(f).Encode(cfg))
	must(f.Close())
	app := carbon.New(path)
	must(app.ParseConfig())
	must(app.Start())
	defer app.Stop()
	app.Lock()
	reader := app.Carbonserver
	app.Unlock()
	reader.WaitForWarmup()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for reader.CurrentFileIndex() == nil {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		app.Lock()
		p, c := app.Persister, app.Cache
		app.Unlock()
		writes, _ := p.WriteStats()
		_ = json.NewEncoder(w).Encode(map[string]any{"cpu_seconds": cpu(), "committed_points": writes.Points, "committed_updates": writes.Updates, "write_errors": writes.Failures, "cache_points": c.Size()})
	})
	mux.HandleFunc("/debug/pprof/profile", httpprof.Profile)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"copy": "http://" + cfg.Buckyd.Bind, "render": "http://" + cfg.Carbonserver.Listen, "tcp": cfg.Tcp.Listen, "control": "http://" + listener.Addr().String(), "pid": os.Getpid()}))
	<-ctx.Done()
}
