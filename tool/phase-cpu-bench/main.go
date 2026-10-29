//go:build (linux || darwin) && phasecpubench

// phase-cpu-bench runs a local TCP -> cache -> shared Pebble pipeline. The
// sender is a separate process, excluded from the server's CPU accounting.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/carbon"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	tcp "github.com/go-graphite/go-carbon/receiver/tcp"
)

type mode struct {
	Phase   bool `json:"phase"`
	Mutex   int  `json:"mutex"`
	Block   int  `json:"block"`
	Seconds int  `json:"seconds"`
	Settle  int  `json:"settle"`
}

type snapshot struct {
	Fine                                          []uint64
	CPU                                           float64
	Received, Persisted, Updates, Dropped, Errors uint64
	Count                                         uint64
}

type server struct {
	paused      atomic.Bool
	cardinality uint64
	app         *carbon.App
	mu          sync.Mutex
	names       atomic.Uint64
}

func cpu() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		panic(err)
	}
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func (s *server) snapshot() snapshot {
	var r snapshot
	r.Fine, r.Count = stagetime.BenchmarkUpdateHistogram()
	for _, rcv := range s.app.Receivers {
		rcv.Receiver.Stat(func(name string, value float64) {
			switch name {
			case "metricsReceived":
				r.Received += uint64(value)
			case "errors", "parseErrors":
				r.Errors += uint64(value)
			}
		})
	}
	s.app.Persister.Stat(func(name string, value float64) {
		switch name {
		case "committedPoints":
			r.Persisted = uint64(value)
		case "updateOperations":
			r.Updates = uint64(value)
		}
	})
	s.app.Cache.Stat(func(name string, value float64) {
		if name == "overflow" {
			r.Dropped = uint64(value)
		}
	})
	r.CPU = cpu()
	return r
}

func quantile(before, after snapshot, q float64) (float64, float64, float64) {
	if after.Count == before.Count {
		return 0, 0, 0
	}
	need := q * float64(after.Count-before.Count)
	var count uint64
	for i, n := range after.Fine {
		count += n - before.Fine[i]
		if float64(count) >= need {
			return (float64(i) + .5) / 1e6, float64(i) / 1e6, float64(i+1) / 1e6
		}
	}
	return .065536, .065535, .065536
}

func (s *server) run(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cfg mode
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if cfg.Seconds < 1 || cfg.Seconds > 60 || cfg.Settle < 0 || cfg.Settle > 60 || cfg.Mutex < 0 || cfg.Block < 0 {
		http.Error(w, "invalid benchmark parameters", 400)
		return
	}
	var p *phasecpu.Profiler
	if cfg.Phase {
		var err error
		p, err = phasecpu.Start(time.Minute)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if err = s.app.PromRegistry.Register(p); err != nil {
			p.Stop()
			http.Error(w, err.Error(), 500)
			return
		}
		defer func() { p.Stop(); s.app.PromRegistry.Unregister(p) }()
	}
	previous := runtime.SetMutexProfileFraction(cfg.Mutex)
	runtime.SetBlockProfileRate(cfg.Block)
	defer runtime.SetMutexProfileFraction(previous)
	defer runtime.SetBlockProfileRate(0)
	timer := time.NewTimer(time.Duration(cfg.Settle) * time.Second)
	select {
	case <-timer.C:
	case <-r.Context().Done():
		timer.Stop()
		return
	}
	before := s.snapshot()
	names := s.names.Load()
	started := time.Now()
	timer.Reset(time.Duration(cfg.Seconds) * time.Second)
	select {
	case <-timer.C:
	case <-r.Context().Done():
		timer.Stop()
		return
	}
	after := s.snapshot()
	elapsed := time.Since(started).Seconds()
	p99, lo, hi := quantile(before, after, .99)
	var windows, profileSeconds, phaseCPU float64
	if p != nil {
		families, err := s.app.PromRegistry.Gather()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for _, f := range families {
			for _, m := range f.Metric {
				switch f.GetName() {
				case "go_carbon_phase_cpu_profile_windows_total":
					windows = m.GetCounter().GetValue()
				case "go_carbon_phase_cpu_profile_seconds_total":
					profileSeconds = m.GetCounter().GetValue()
				case "go_carbon_phase_cpu_seconds_total":
					phaseCPU += m.GetCounter().GetValue()
				}
			}
		}
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	rss := int64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	result := map[string]any{
		"mode": cfg, "wall_seconds": elapsed, "cpu_seconds": after.CPU - before.CPU,
		"received_points": after.Received, "persisted_points": after.Persisted, "updates": after.Updates,
		"cpu_seconds_per_million": (after.CPU - before.CPU) * 1e6 / float64(after.Persisted),
		"points_per_second":       float64(after.Persisted) / elapsed, "received_points_per_second": float64(after.Received) / elapsed,
		"update_p99_us": p99 * 1e6, "p99_overflow": hi >= .065536, "p99_bucket_lower_us": lo * 1e6, "p99_bucket_upper_us": hi * 1e6,
		"dropped": after.Dropped, "receiver_errors": after.Errors, "cache_points_end": s.app.Cache.Size(),
		"input_unique_metrics": min(s.names.Load()-names, s.cardinality), "total_input_metric_batches": s.names.Load(), "input_metric_batches_before": names,
		"profile_windows": windows, "profile_seconds": profileSeconds, "estimated_phase_cpu": phaseCPU, "peak_rss_bytes": rss,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func metricName(dst []byte, i int) []byte {
	dst = append(dst, "example.prod.region_a.metrics.database.queries.by_cluster."...)
	dst = strconv.AppendInt(dst, int64(i%1024), 10)
	dst = append(dst, ".by_host.database-store-"...)
	dst = strconv.AppendInt(dst, int64(i%16384), 10)
	dst = append(dst, ".by_schema.application.by_status.success.series_"...)
	return strconv.AppendInt(dst, int64(i), 10)
}

func send(args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	address := fs.String("addr", "", "TCP receiver")
	control := fs.String("control", "", "local control URL")
	metrics := fs.Int("metrics", 1_000_000, "distinct metrics")
	perMetric := fs.Int("points", 8, "points per metric batch")
	cycles := fs.Int("cycles", 0, "cycles; zero runs until killed")
	startCycle := fs.Int("cycle", 0, "initial value/timestamp cycle")
	_ = fs.Parse(args)
	runtime.GOMAXPROCS(1)
	conn, err := net.Dial("tcp", *address)
	if err != nil {
		return err
	}
	defer conn.Close()
	if c, ok := conn.(*net.TCPConn); ok {
		_ = c.SetWriteBuffer(64 << 10)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	data := make([]byte, 0, 2<<20)
	name := make([]byte, 0, 160)

	var batches uint64
	for cycle := *startCycle; *cycles == 0 || cycle < *startCycle+*cycles; cycle++ {
		for first := 0; first < *metrics; first += 512 {
			for {
				resp, err := client.Get(*control + "/backlog")
				if err != nil {
					return err
				}
				var state struct {
					Points int64 `json:"points"`
					Paused bool  `json:"paused"`
				}
				err = json.NewDecoder(resp.Body).Decode(&state)
				_ = resp.Body.Close()
				if err != nil {
					return err
				}
				if state.Paused {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if state.Points < 65536 {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			epoch := time.Now().Unix() - int64(*perMetric)
			data = data[:0]
			last := min(first+512, *metrics)
			for i := first; i < last; i++ {
				name = metricName(name[:0], i)
				for j := range *perMetric {
					data = append(data, name...)
					data = append(data, ' ')
					n := cycle**perMetric + j
					value := float64(i % 127)
					switch i % 4 {
					case 1:
						value += float64(n)
					case 2:
						value += float64(n % 3)
					case 3:
						value = float64(i%100003)/17.3 + float64(n)*.125
					}
					data = strconv.AppendFloat(data, value, 'g', -1, 64)
					data = append(data, ' ')
					data = strconv.AppendInt(data, epoch+int64(j), 10)
					data = append(data, '\n')
				}
			}
			payload := data
			for len(payload) > 0 {
				n, err := conn.Write(payload)
				if err != nil {
					return err
				}
				payload = payload[n:]
			}
			batches += uint64(last - first)
			req, _ := http.NewRequest(http.MethodPost, *control+"/names", strings.NewReader(strconv.FormatUint(batches, 10)))
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}
	return nil
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dir := fs.String("dir", "", "private benchmark directory")
	cardinality := fs.Int("metrics", 1_000_000, "catalog cardinality")
	procs := fs.Int("procs", 4, "fixed server GOMAXPROCS")
	perMetric := fs.Int("points", 8, "points per batch")
	reuse := fs.Bool("reuse", false, "reuse an already verified catalog")
	_ = fs.Parse(args)
	if *dir == "" {
		return fmt.Errorf("-dir is required")
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		return err
	}
	schemas := filepath.Join(*dir, "schemas.conf")
	if err := os.WriteFile(schemas, []byte("[default]\npattern = .*\nretentions = 1s:2h,60s:7d\n"), 0600); err != nil {
		return err
	}
	cfg := carbon.NewConfig()
	cfg.Common.MaxCPU = *procs
	cfg.Common.MetricInterval = &carbon.Duration{Duration: time.Hour}
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Udp.Enabled, cfg.Pickle.Enabled, cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false, false, false
	cfg.Carbonserver.Enabled, cfg.Buckyd.Enabled, cfg.Pprof.Enabled, cfg.Prometheus.Enabled = false, false, false, false
	cfg.Whisper.StorageBackend, cfg.Whisper.StoreEncoding = "pebble-chunk", "exact"
	cfg.Whisper.StoreDir = filepath.Join(*dir, "store")
	cfg.Whisper.DataDir = filepath.Join(*dir, "unused-files")
	cfg.Whisper.SchemasFilename = schemas
	cfg.Whisper.Workers = 16
	cfg.Whisper.MaxUpdatesPerSecond = 0
	cfg.Whisper.StoreCacheSize = 256 << 20
	cfg.Whisper.StoreMemTableSize = 32 << 20
	cfg.Whisper.StoreMemTableMaxUnflushed = 256 << 20
	cfg.Whisper.StoreMetadataCacheSize = 256 << 20
	cfg.Cache.MaxSize = 4_000_000
	cfg.Cache.WriteoutMinPoints = *perMetric
	cfg.Cache.WriteoutMaxDelay = carbon.Duration{Duration: time.Second}
	path := filepath.Join(*dir, "go-carbon.conf")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	err = toml.NewEncoder(file).Encode(cfg)
	_ = file.Close()
	if err != nil {
		return err
	}
	app := carbon.New(path)
	if err = app.ParseConfig(); err != nil {
		return err
	}
	if err = app.Start(); err != nil {
		return err
	}
	defer app.Stop()
	app.Lock()
	collector := app.Collector
	app.Collector = nil
	app.Unlock()
	if collector != nil {
		collector.Stop()
	}
	s := &server{app: app, cardinality: uint64(*cardinality)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	url := "http://" + listener.Addr().String()
	quit := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/backlog", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"points": app.Cache.Size(), "paused": s.paused.Load()})
	})
	mux.HandleFunc("/names", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 64))
		n, _ := strconv.ParseUint(string(body), 10, 64)
		s.names.Store(n)
	})
	mux.HandleFunc("/pause", func(w http.ResponseWriter, r *http.Request) { s.paused.Store(r.URL.Query().Get("value") == "1") })
	mux.HandleFunc("/warm", func(w http.ResponseWriter, r *http.Request) {
		before := s.names.Load()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for s.names.Load()-before < uint64(*cardinality) {
			select {
			case <-tick.C:
			case <-r.Context().Done():
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metric_batches": s.names.Load() - before})
	})
	mux.HandleFunc("/run", s.run)
	mux.HandleFunc("/quit", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-quit:
		default:
			close(quit)
		}
	})
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = httpServer.Serve(listener) }()
	defer httpServer.Close()
	var addr string
	for _, r := range app.Receivers {
		if c, ok := r.Receiver.(*tcp.TCP); ok {
			addr = c.Addr().String()
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	child := func(cycles, cycle int) *exec.Cmd {
		cmd := exec.Command(exe, "send", "-addr", addr, "-control", url, "-metrics", strconv.Itoa(*cardinality), "-points", strconv.Itoa(*perMetric), "-cycles", strconv.Itoa(cycles), "-cycle", strconv.Itoa(cycle))
		cmd.Stderr = os.Stderr
		return cmd
	}
	if !*reuse {
		runtime.GOMAXPROCS(min(*procs, 2))
		fmt.Fprintf(os.Stderr, "seeding %d metrics through TCP\n", *cardinality)
		seed := child(1, 0)
		if err = seed.Run(); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for app.Cache.Size() != 0 || app.Cache.NotConfirmedLength() != 0 {
		<-ticker.C
	}

	count := 0
	after := ""
	for {
		page, err := app.MetricStore.ListPage(context.Background(), "example.", after, 10000)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		count += len(page)
		after = page[len(page)-1].Name
	}
	if count < *cardinality {
		return fmt.Errorf("catalog has %d metrics, need %d", count, *cardinality)
	}
	runtime.GOMAXPROCS(*procs)
	s.names.Store(0)
	source := child(0, 1)
	if err = source.Start(); err != nil {
		return err
	}
	defer func() { _ = source.Process.Kill(); _ = source.Wait() }()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"ready": true, "control": url, "metrics": count, "procs": runtime.GOMAXPROCS(0), "points_per_batch": *perMetric})
	<-quit
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: phase-cpu-bench serve|send")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "send":
		err = send(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
