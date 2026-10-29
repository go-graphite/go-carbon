// Local-only three-node resize benchmark. No production endpoints are accepted.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/bits"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/carbon"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func address() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	defer l.Close()
	return l.Addr().String()
}
func jump(name string, n int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	key := h.Sum64()
	var b int64 = -1
	var j int64
	for j < int64(n) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(uint64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}
func cpu() float64 {
	var r syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &r))
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
func report(event string, v map[string]any) {
	v["event"] = event
	_ = json.NewEncoder(os.Stdout).Encode(v)
}
func start(root, node string, nodes []string, continuation bool, cacheMB, memtableMB, cpus, metadataMB int) *carbon.App {
	must(os.MkdirAll(root, 0750))
	path := filepath.Join(root, "go-carbon.conf")
	existing := false
	if _, err := os.Stat(path); err == nil {
		existing = true
	} else {
		path = carbon.TestConfig(root)
	}
	cfg, err := carbon.ReadConfig(path)
	must(err)
	cfg.Common.MaxCPU = cpus
	cfg.Common.MetricInterval = &carbon.Duration{Duration: time.Hour}
	cfg.Udp.Enabled = false
	cfg.Pickle.Enabled = false
	cfg.Grpc.Enabled = false
	cfg.Carbonlink.Enabled = false
	cfg.Pprof.Enabled = false
	cfg.Prometheus.Enabled = false
	cfg.Tcp.Enabled = true
	cfg.Tcp.Listen = address()
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreDir = filepath.Join(root, "store")
	if !existing {
		cfg.Whisper.DataDir = filepath.Join(root, "originals")
	}
	cfg.Whisper.StoreCacheSize = int64(cacheMB) << 20
	cfg.Whisper.StoreMetadataCacheSize = int64(metadataMB) << 20
	cfg.Whisper.StoreMemTableSize = uint64(memtableMB) << 20
	cfg.Whisper.StoreSyncInterval = &carbon.Duration{Duration: time.Second}
	cfg.Whisper.Workers = 8
	cfg.Whisper.MaxUpdatesPerSecond = 1000000
	cfg.Whisper.StoreCompactionWorkers = 2
	cfg.Whisper.StoreL0StopWritesThreshold = 32
	cfg.Whisper.StoreIncrementalMigration = continuation
	cfg.Whisper.StoreTimeBuckets = continuation
	cfg.Whisper.StoreMigrationWorkers = 0
	cfg.Whisper.StoreMigrationBytesInFlight = 8 << 20
	cfg.Whisper.StoreMigrationMinFreeBytes = 0
	cfg.Whisper.FLock = true
	cfg.Whisper.Compressed = true
	cfg.Carbonserver.Enabled = continuation
	cfg.Carbonserver.Listen = address()
	cfg.Carbonserver.TrieIndex = true
	cfg.Carbonserver.TrigramIndex = false
	cfg.Carbonserver.FileListCache = filepath.Join(root, "files.flc")
	cfg.Carbonserver.FileListCacheVersion = 2
	cfg.Carbonserver.ConcurrentIndex = false
	cfg.Carbonserver.RealtimeIndex = 0
	cfg.Carbonserver.ScanFrequency = &carbon.Duration{Duration: time.Hour}
	cfg.Carbonserver.QueryCacheEnabled = false
	cfg.Carbonserver.FindCacheEnabled = false
	cfg.Buckyd.Enabled = true
	cfg.Buckyd.Bind = address()
	cfg.Buckyd.Node = node
	cfg.Buckyd.Nodes = nodes
	cfg.Buckyd.Hash = "jump_fnv1a"
	cfg.Buckyd.Replicas = 1
	cfg.Buckyd.MaxTransfers = 32
	cfg.Buckyd.MaxBodyBytes = 160 << 20
	cfg.Buckyd.TmpDir = filepath.Join(root, "tmp")
	must(os.MkdirAll(cfg.Whisper.DataDir, 0750))
	must(os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[default]\npattern = .*\nretentions = 10s:1d,1m:30d\n"), 0600))
	f, err := os.Create(path)
	must(err)
	must(toml.NewEncoder(f).Encode(cfg))
	must(f.Close())
	app := carbon.New(path)
	must(app.ParseConfig())
	must(app.Start())
	return app
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func run() error {
	root := flag.String("root", "", "local fixture directory")
	reuse := flag.Bool("reuse", false, "resume seeding or run against a stopped/cloned fixture")
	cpus := flag.Int("cpus", 4, "shared backend process CPU budget; overrides carbon.App default max-cpu=1")
	metadataMB := flag.Int("metadata-cache-mb", 0, "per-App existing metadata/state cache budget")
	warmExport := flag.Bool("warm-exports", false, "prewarm the source metadata/block caches before measured transfer")
	cacheMB := flag.Int("cache-mb", 128, "per-App cache budget (must leave headroom beyond all live memtables)")
	memtableMB := flag.Int("memtable-mb", 8, "per-App memtable size")
	count := flag.Int("metrics", 1000000, "total metric cardinality across three old owners")
	points := flag.Int("points", 128, "points per metric")
	continuation := flag.Bool("continuation", true, "v7 continuation store (legacy rebalance rejects it)")
	mode := flag.String("mode", "bulk", "bulk, legacy, seed, or idle")
	unified := flag.Bool("unified", false, "mover uses the unified resize run command")
	mover := flag.String("mover", "/tmp/resize", "mover binary")
	bucky := flag.String("bucky", "/tmp/bucky", "buckytools binary")
	batch := flag.Int("batch", 2048, "bulk batch cap")
	streams := flag.Int("streams", 4, "bulk per-host stream cap")
	readQPS := flag.Int("read-qps", 1000, "storage fetch requests/s per target when reads > 0")
	idleDuration := flag.Duration("duration", 30*time.Second, "idle baseline duration")
	reads := flag.Int("reads", 0, "concurrent direct target store reader workers")
	ingest := flag.Int("ingest", 0, "updates/s per loaded host; zero disables load")
	ingestSource := flag.Bool("ingest-source", false, "keep the source ingesting too; requires --delete=false")
	auto := flag.Bool("auto", false, "use adaptive ramp rather than fixed ceiling")
	remove := flag.Bool("delete", true, "verify then remove source copies")
	overlap := flag.Int("overlap", 100, "one in N moved metrics preexists on target; zero disables")
	profile := flag.String("cpuprofile", "", "CPU profile of transfer stage (apps and load only, excludes mover child)")
	flag.Parse()
	if *ingestSource && *remove {
		panic("live source ingestion requires --delete=false")
	}
	loadHosts := 2
	if *ingestSource {
		loadHosts = 3
	}
	if *root == "" {
		panic("--root required")
	}
	must(os.MkdirAll(*root, 0750))
	if _, err := os.Stat(filepath.Join(*root, "node0")); err == nil && !*reuse {
		panic("fixture directory must be new")
	}
	// Bucky assumes one buckyd port for all members. A local Host-header proxy
	// maps case-distinct localhost names to distinct embedded App instances.
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	port := strings.Split(proxyListener.Addr().String(), ":")[1]
	identities := []string{"localhost", "LOCALHOST", "127.0.0.1"}
	newRing := []string{"localhost:" + port + "=000", "LOCALHOST:" + port + "=001"}
	apps := make([]*carbon.App, 3)
	for i := range apps {
		apps[i] = start(filepath.Join(*root, fmt.Sprintf("node%d", i)), identities[i], newRing, *continuation, *cacheMB, *memtableMB, *cpus, *metadataMB)
		defer apps[i].Stop()
	}
	proxies := map[string]*httputil.ReverseProxy{}
	for i, name := range identities {
		endpoint := "http://" + apps[i].Config.Buckyd.Bind
		request, err := http.NewRequest("GET", endpoint, nil)
		must(err)
		proxies[name] = httputil.NewSingleHostReverseProxy(request.URL)
	}
	proxy := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		p := proxies[host]
		if p == nil {
			http.NotFound(w, r)
			return
		}
		p.ServeHTTP(w, r)
	})}
	go func() { _ = proxy.Serve(proxyListener) }()
	defer proxy.Close()
	ctx := context.Background()
	now := time.Now().Unix() / 60 * 60
	// Tile 256 independently encoded histories so compression sees real-sized
	// exact blocks without allocating a million Go objects at once.
	templateSource := filepath.Join(*root, "template-originals")
	if marker, err := os.ReadFile(filepath.Join(*root, "template", "INCREMENTAL-FILE-MIGRATION")); err == nil {
		lines := strings.Split(string(marker), "\n")
		if len(lines) > 1 {
			templateSource = lines[1]
		}
	}
	must(os.MkdirAll(templateSource, 0750))
	opts := store.Options{ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	if *continuation {
		opts.Continuation = &whisperio.ContinuationCodec{DirectLateWrites: true}
		opts.TimeBuckets = true
		opts.IncrementalSource = templateSource
	}
	template, err := store.Open(filepath.Join(*root, "template"), opts)
	must(err)
	config := store.MetricConfig{Retentions: []store.Retention{{Step: 10, Count: 8640}, {Step: 60, Count: 43200}}, AggregationMethod: store.Sum}
	var templates []store.TransferMetric
	for i := range 256 {
		config.Name = fmt.Sprintf("template.%03d", i)
		_, err := template.Create(ctx, config)
		if err != nil && !errors.Is(err, store.ErrExists) {
			must(err)
		}
		data := make([]store.Point, *points)
		for j := range data {
			data[j] = store.Point{Timestamp: now - int64((*points-j)*10), Value: math.Sqrt(float64(i*997+j+1)) * 1.000001}
		}
		if err == nil {
			must(template.UpdateMany(ctx, config.Name, data))
		}
		records, err := template.ExportTransfer(ctx, []string{config.Name})
		must(err)
		templates = append(templates, records[0])
	}
	must(template.Close())
	last := make([]string, 3)
	if *reuse {
		for owner, app := range apps {
			after := ""
			for {
				page, err := app.MetricStore.ListTransferNames(ctx, "synthetic.metric.", after, 10000)
				must(err)
				for _, name := range page {
					if jump(name, 3) == owner {
						last[owner] = name
					}
				}
				if len(page) < 10000 {
					break
				}
				after = page[len(page)-1]
			}
		}
	}
	seedImported := 0
	seedStart := time.Now()
	seedCPU := cpu()
	pending := make([][]store.TransferMetric, 3)
	names := make([][]string, 2)
	var rawBytes int64
	var moved int
	flush := func(owner int) {
		if len(pending[owner]) == 0 {
			return
		}
		_, err := apps[owner].MetricStore.ImportTransfer(ctx, pending[owner])
		must(err)
		pending[owner] = nil
	}
	for i := 0; i < *count; i++ {
		name := fmt.Sprintf("synthetic.metric.%09d", i)
		owner := jump(name, 3)
		r := templates[i%len(templates)]
		r.Config.Name = name
		r.Version.Name = name
		r.PointDigest = ""
		r.Counts = nil
		if name > last[owner] {
			seedImported++
			pending[owner] = append(pending[owner], r)
		}
		if len(pending[owner]) >= 2048 {
			flush(owner)
		}
		if owner == 2 {
			target := jump(name, 2)
			names[target] = append(names[target], name)
			moved++
			for _, kv := range r.Keys {
				rawBytes += int64(len(kv.Key) + len(kv.Value))
			}
		}
	}
	for i := range pending {
		flush(i)
	}
	report("seed", map[string]any{"metrics": *count, "seed_imported": seedImported, "moved": moved, "seconds": time.Since(seedStart).Seconds(), "cpu_seconds": cpu() - seedCPU, "raw_moved_bytes": rawBytes, "raw_bytes_per_metric": float64(rawBytes) / float64(moved)})
	if *overlap > 0 {
		for target, list := range names {
			for i, name := range list {
				if i%*overlap != 0 {
					continue
				}
				cfg := config
				cfg.Name = name
				_, err := apps[target].MetricStore.Create(ctx, cfg)
				if err != nil && !errors.Is(err, store.ErrExists) {
					must(err)
				}
				must(apps[target].MetricStore.UpdateMany(ctx, name, []store.Point{{Timestamp: now - 10, Value: 999.5}, {Timestamp: now + 10, Value: 777.25}}))
			}
		}
	}
	planDir := filepath.Join(*root, "plan")
	must(os.MkdirAll(planDir, 0750))
	planID := fmt.Sprintf("%x", sha256.Sum256([]byte(*root)))
	type stream struct {
		Path, Source, Destination string
		Metrics                   int
		EstimatedBytes            int64 `json:"estimated_bytes"`
		SHA256                    string
	}
	var streamsOut []stream
	for target, list := range names {
		path := fmt.Sprintf("2-%d.jsonl", target)
		f, err := os.Create(filepath.Join(planDir, path))
		must(err)
		h := sha256.New()
		encoder := json.NewEncoder(io.MultiWriter(f, h))
		src, dst := "n2", fmt.Sprintf("n%d", target)
		must(encoder.Encode(map[string]any{"type": "header", "format": "go-carbon-ring", "version": 1, "plan_id": planID, "source": src, "destination": dst}))
		for _, name := range list {
			must(encoder.Encode(map[string]any{"type": "move", "metric": name, "estimated_bytes": 1024}))
		}
		must(encoder.Encode(map[string]any{"type": "end", "metrics": len(list), "estimated_bytes": len(list) * 1024}))
		must(f.Close())
		streamsOut = append(streamsOut, stream{path, src, dst, len(list), int64(len(list) * 1024), hex.EncodeToString(h.Sum(nil))})
	}
	var members []map[string]string
	for i, app := range apps {
		members = append(members, map[string]string{"id": fmt.Sprintf("n%d", i), "node": identities[i] + ":" + port + fmt.Sprintf("=%03d", i), "endpoint": "http://" + app.Config.Buckyd.Bind})
	}
	manifest := map[string]any{"format": "go-carbon-ring", "version": 1, "plan_id": planID, "spec": map[string]any{"version": 1, "cluster": "synthetic", "site": "local", "old": map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": members}, "new": map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": members[:2]}}, "streams": streamsOut}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	must(err)
	must(os.WriteFile(filepath.Join(planDir, "manifest.json"), encoded, 0600))
	barrier, err := json.Marshal(map[string]any{"plan_id": planID, "expires_at": time.Now().Add(time.Hour).Unix(), "relay_switched": true, "old_writers_drained": true, "reconciled": true, "sources": []string{"n2"}})
	must(err)
	barrierPath := filepath.Join(*root, "barrier.json")
	must(os.WriteFile(barrierPath, barrier, 0600))
	if *mode == "seed" {
		return nil
	}
	expected := map[string]store.Snapshot{}
	for target, list := range names {
		for i := 0; i < len(list); i += max(1, len(list)/100) {
			name := list[i]
			snapshot, err := apps[2].MetricStore.Snapshot(ctx, name)
			must(err)
			if existing, err := apps[target].MetricStore.Snapshot(ctx, name); err == nil {
				for ai, a := range existing.Archives {
					values := map[int64]store.Point{}
					for _, p := range snapshot.Archives[ai].Points {
						values[p.Timestamp] = p
					}
					for _, p := range a.Points {
						values[p.Timestamp] = p
					}
					points := make([]store.Point, 0, len(values))
					for _, p := range values {
						points = append(points, p)
					}
					sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
					snapshot.Archives[ai].Points = points
				}
			} else if !errors.Is(err, store.ErrNotFound) {
				must(err)
			}
			expected[name] = snapshot
		}
	}
	if *ingest > 0 {
		for target := 0; target < loadHosts; target++ {
			for i := range 1000 {
				cfg := config
				cfg.Name = fmt.Sprintf("concurrent.target.%03d", i)
				if _, err := apps[target].MetricStore.Create(ctx, cfg); err != nil && !errors.Is(err, store.ErrExists) {
					must(err)
				}
			}
		}
	}
	if *warmExport {
		list := append(append([]string(nil), names[0]...), names[1]...)
		if *metadataMB > 0 {
			stats, err := apps[2].MetricStore.PrewarmMetadata(ctx, slices.Values(list), store.PrewarmOptions{Workers: 2, Batch: 2048})
			must(err)
			report("metadata_prewarm", map[string]any{"stats": stats})
		}
		for start := 0; start < len(list); start += 8192 {
			_, err := apps[2].MetricStore.ExportTransfer(ctx, list[start:min(start+8192, len(list))])
			must(err)
		}
	}
	loadCtx, stopLoad := context.WithCancel(ctx)
	var load sync.WaitGroup
	var updates, readCount, loadErrors atomic.Int64
	var readLatency [64]atomic.Uint64
	for target := 0; target < loadHosts; target++ {
		if *ingest > 0 {
			// A fixed hot set exercises the real receiver/cache/persister while transfer
			// merges other metrics. Disjoint names keep write verification deterministic.
			conn, err := net.Dial("tcp", apps[target].Config.Tcp.Listen)
			must(err)
			load.Add(1)
			go func() {
				defer load.Done()
				defer conn.Close()
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-loadCtx.Done():
						return
					case <-ticker.C:
						var b bytes.Buffer
						for j := 0; j < max(1, *ingest/100); j++ {
							fmt.Fprintf(&b, "concurrent.target.%03d %f %d\n", j%1000, float64(j), time.Now().Unix())
						}
						if _, err := conn.Write(b.Bytes()); err != nil {
							loadErrors.Add(1)
							return
						}
						updates.Add(int64(max(1, *ingest/100)))
					}
				}
			}()
		}
		if target == 2 {
			continue
		}
		for reader := 0; reader < *reads; reader++ {
			reader := reader
			load.Add(1)
			go func() {
				defer load.Done()
				ticker := time.NewTicker(time.Second * time.Duration(*reads) / time.Duration(max(1, *readQPS)))
				defer ticker.Stop()
				for i := reader * 104729; ; i++ {
					select {
					case <-loadCtx.Done():
						return
					case <-ticker.C:
					}
					name := fmt.Sprintf("synthetic.metric.%09d", i%*count)
					for jump(name, 3) != target {
						i++
						name = fmt.Sprintf("synthetic.metric.%09d", i%*count)
					}
					start := time.Now()
					_, err := apps[target].MetricStore.Fetch(loadCtx, name, int(now-3600), int(now))
					readLatency[min(63, bits.Len64(uint64(max(1, time.Since(start).Microseconds()))))].Add(1)
					if err != nil && loadCtx.Err() == nil {
						loadErrors.Add(1)
					}
					readCount.Add(1)
				}
			}()
		}
	}
	time.Sleep(2 * time.Second)
	if *profile != "" {
		f, err := os.Create(*profile)
		must(err)
		must(pprof.StartCPUProfile(f))
		defer f.Close()
	}
	sentBefore, readBefore := updates.Load(), readCount.Load()
	var readBucketsBefore [64]uint64
	for i := range readLatency {
		readBucketsBefore[i] = readLatency[i].Load()
	}
	cacheBefore := make([]int64, loadHosts)
	for i := range loadHosts {
		cacheBefore[i] = apps[i].Cache.Size()
	}
	committedBefore := make([]uint64, loadHosts)
	for i := range loadHosts {
		p, _ := apps[i].Persister.WriteStats()
		committedBefore[i] = p.Points
	}
	before := []store.Stats{apps[0].MetricStore.Stats(), apps[1].MetricStore.Stats(), apps[2].MetricStore.Stats()}
	started := time.Now()
	cpuStart := cpu()
	var command *exec.Cmd
	switch *mode {
	case "idle":
		command = exec.Command("sleep", fmt.Sprint(idleDuration.Seconds()))
		*remove = false
	case "legacy":
		args := []string{"rebalance", "-host", "localhost:" + port, "-workers", strconvInt(*streams), "-offload", "-filter-cache-metrics=false", "-no-random-easing", "-sync-speed-up-interval=0"}
		if *remove {
			args = append(args, "-delete")
		}
		args = append(args, "127.0.0.1:"+port)
		command = exec.Command(*bucky, args...)
	default:
		args := []string{"--plan", planDir, "--state", filepath.Join(*root, "state.jsonl"), "--apply", "--local-lab", "--streams", strconvInt(*streams), "--batch", strconvInt(*batch), fmt.Sprintf("--auto=%t", *auto), "--delete-rate", "100000", "--tick", "1s"}
		if *remove {
			args = append(args, "--delete", "--barrier", barrierPath)
		}
		if *unified {
			args = append([]string{"run"}, args...)
		}
		command = exec.Command(*mover, args...)
	}
	log, err := os.Create(filepath.Join(*root, "move.log"))
	must(err)
	command.Stdout = log
	command.Stderr = log
	runErr := command.Run()
	duration := time.Since(started).Seconds()
	cpuUsed := cpu() - cpuStart
	if *profile != "" {
		pprof.StopCPUProfile()
	}
	must(log.Close())
	stopLoad()
	load.Wait()
	var childCPU float64
	if command.ProcessState != nil {
		childCPU = command.ProcessState.UserTime().Seconds() + command.ProcessState.SystemTime().Seconds()
	}
	cacheAfter := make([]int64, loadHosts)
	for i := range loadHosts {
		cacheAfter[i] = apps[i].Cache.Size()
	}
	committed := make([]uint64, loadHosts)
	for i := range loadHosts {
		p, _ := apps[i].Persister.WriteStats()
		committed[i] = p.Points - committedBefore[i]
	}
	readP99US := uint64(0)
	var cumulative, totalRead uint64
	for i := range readLatency {
		totalRead += readLatency[i].Load() - readBucketsBefore[i]
	}
	for i := range readLatency {
		cumulative += readLatency[i].Load() - readBucketsBefore[i]
		if totalRead > 0 && cumulative*100 >= totalRead*99 {
			readP99US = uint64(1) << i
			break
		}
	}
	after := []store.Stats{apps[0].MetricStore.Stats(), apps[1].MetricStore.Stats(), apps[2].MetricStore.Stats()}
	reportedMoved := moved
	reportedBytes := rawBytes
	if *mode == "idle" || runErr != nil {
		reportedMoved = 0
		reportedBytes = 0
	}
	var cpuPerGB any
	if reportedBytes > 0 {
		cpuPerGB = (cpuUsed + childCPU) / (float64(reportedBytes) / 1e9)
	}
	report("transfer", map[string]any{"mode": *mode, "continuation": *continuation, "metrics": *count, "moved": moved, "seconds": duration, "metrics_per_second": float64(reportedMoved) / duration, "raw_MB_per_second": float64(reportedBytes) / 1e6 / duration, "cpu_seconds": cpuUsed, "mover_cpu_seconds": childCPU, "cpu_seconds_per_raw_GB": cpuPerGB, "streams": *streams, "batch": *batch, "backend_cpus": runtime.GOMAXPROCS(0), "cache_mb": *cacheMB, "metadata_cache_mb": *metadataMB, "warm_exports": *warmExport, "memtable_mb": *memtableMB, "reads": *reads, "read_requests": readCount.Load() - readBefore, "read_p99_us_upper": readP99US, "read_qps_per_host": *readQPS, "cache_points_before": cacheBefore, "cache_points_after": cacheAfter, "committed_points": committed, "ingest_sent_points": updates.Load() - sentBefore, "ingest_source": *ingestSource, "load_errors": loadErrors.Load(), "error": fmt.Sprint(runErr), "before": before, "after": after})
	if runErr != nil {
		data, _ := os.ReadFile(filepath.Join(*root, "move.log"))
		lines := strings.Split(string(data), "\n")
		if len(lines) > 15 {
			lines = lines[len(lines)-15:]
		}
		fmt.Fprintln(os.Stderr, strings.Join(lines, "\n"))
		return runErr
	}
	if *mode == "idle" {
		return nil
	}
	// Sample point-level public snapshots, and verify source catalogue removal.
	for target, list := range names {
		sort.Strings(list)
		for i := 0; i < len(list); i += max(1, len(list)/100) {
			snap, err := apps[target].MetricStore.Snapshot(ctx, list[i])
			must(err)
			must(store.EqualSnapshots(expected[list[i]], snap))
		}
	}
	sourceList, err := apps[2].MetricStore.List(ctx, "")
	must(err)
	if *remove && len(sourceList) > 0 {
		panic(fmt.Sprintf("source retains %d metrics", len(sourceList)))
	}
	report("parity", map[string]any{"sampled_histories": "passed", "source_remaining": len(sourceList)})
	return nil
}
func strconvInt(n int) string { return fmt.Sprint(n) }
