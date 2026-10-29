package carbonserver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

func catalogHeapName(i int) string {
	return fmt.Sprintf("kafka.by_cluster.cluster%03d.by_topic.customer_order_events_region_%03d.by_partition.%03d.consumer.fetch_manager.records_lag_max_%02d", i/100000, (i/1000)%100, (i/10)%100, i%10)
}

func catalogHeapClientName(i int) string {
	x := uint32(i)
	x = (x ^ (x >> 16)) * 0x7feb352d
	x = (x ^ (x >> 15)) * 0x846ca68b
	x ^= x >> 16
	return fmt.Sprintf("kafka.by_cluster.cluster%03d.by_topic.customer_order_events_region_%03d.by_partition.%03d.consumer.client_%08x.records_lag_max", i/100000, (i/1000)%100, (i/10)%100, x)
}

// Separate seed/build/window invocations keep the opt-in 10M-name experiment
// bounded. Every mode uses the real store, ListPage, FST and query implementations.
func TestMetricStoreHeapScale(t *testing.T) {
	directory, mode := os.Getenv("CATALOG_HEAP_DIR"), os.Getenv("CATALOG_HEAP_MODE")
	if directory == "" || mode == "" {
		t.Skip("set CATALOG_HEAP_DIR and CATALOG_HEAP_MODE for the local scale probe")
	}
	name := catalogHeapName
	clients := os.Getenv("CATALOG_HEAP_NAMES") == "clients"
	if clients {
		name = catalogHeapClientName
	}
	root := filepath.Join(directory, "originals")
	if err := os.MkdirAll(root, 0750); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(directory, "store"), store.Options{ExactBlocks: true, IncrementalSource: root, SyncInterval: time.Hour, CacheSize: 4 << 20, MemTableSize: 16 << 20, MaxUnflushedMemTableBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	high, err := db.CatalogHighWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "seed" {
		count, _ := strconv.Atoi(os.Getenv("CATALOG_HEAP_SEED"))
		if count == 0 {
			count = 250000
		}
		started := time.Now()
		for i := int(high); i < int(high)+count; i++ {
			if _, err := db.Create(ctx, store.MetricConfig{Name: name(i), Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("seeded %d total=%d wall=%v name_bytes=%d", count, int(high)+count, time.Since(started), len(name(0)))
		return
	}
	l, _ := newScanTestListener(root, filepath.Join(directory, "files.flc"), 1)
	l.logger = zap.NewNop()
	l.SetMetricStore(db)
	l.SetEstimateSize(nil)
	l.SetMaxGlobs(10000)
	l.SetIncrementalIndexScratch(filepath.Join(directory, "scratch"))
	started := time.Now()
	if mode == "heap_union" {
		// Run fixed first to prepare the complete mapped base. heap_union's union
		// refresh retains that base while writing its mapped replacement.
		s, err := openIndexSnapshot(l.fileListCache, root)
		if err != nil {
			t.Fatal(err)
		}
		ti := newTrie(".wsp", 0, nil)
		ti.snapshot = s
		l.UpdateFileIndex(&fileIndex{trieIdx: ti})
		l.SetMetricStoreFileIndex(true)
		l.updateFileList(root, nil, nil)
	} else if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	build := time.Since(started)
	if !l.HasMappedIndex() || l.CurrentFileIndex().trieIdx.snapshot.manifest.Records != high {
		t.Fatalf("wrong mapped catalog: metrics=%d want=%d", l.metrics.MetricsKnown, high)
	}
	for i := range min(int(high), 10000) {
		if err := db.UpdateMany(ctx, name(i), []store.Point{{Timestamp: 1700000000, Value: 0}}); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	readMetrics := func() map[string]float64 {
		samples := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/gc/scan/heap:bytes"}, {Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/gc/cycles/total:gc-cycles"}}
		metrics.Read(samples)
		result := make(map[string]float64)
		for _, s := range samples {
			if s.Value.Kind() == metrics.KindUint64 {
				result[s.Name] = float64(s.Value.Uint64())
			} else {
				result[s.Name] = s.Value.Float64()
			}
		}
		return result
	}
	queries := []string{
		name(123456),
		"kafka.by_cluster.cluster001.by_topic.customer_order_events_region_023.by_partition.045.consumer.fetch_manager.*",
		"kafka.by_cluster.cluster001.by_topic.*",
		"kafka.by_cluster.cluster001.by_topic.customer_order_events_region_023.by_partition.*.consumer.fetch_manager.records_lag_max_01",
	}
	if clients {
		for i := range queries {
			queries[i] = strings.ReplaceAll(queries[i], "consumer.fetch_manager.*", "consumer.*.records_lag_max")
			queries[i] = strings.ReplaceAll(queries[i], "consumer.fetch_manager.records_lag_max_01", "consumer.*.records_lag_max")
		}
	}
	latencies := make(map[string]float64)
	for _, query := range queries {
		path := "/" + strings.ReplaceAll(query, ".", "/")
		started := time.Now()
		for range 1500 {
			names, _, _, _, err := l.CurrentFileIndex().trieIdx.query(path, 10000, nil)
			if err != nil || len(names) == 0 {
				t.Fatalf("query %s: %d %v", query, len(names), err)
			}
		}
		latencies[query] = float64(time.Since(started).Nanoseconds()) / 1500
	}
	window := 10 * time.Second
	if text := os.Getenv("CATALOG_HEAP_WINDOW"); text != "" {
		window, err = time.ParseDuration(text)
		if err != nil {
			t.Fatal(err)
		}
	}
	before := readMetrics()
	processCPU := func() float64 {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Fatal(err)
		}
		return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	}
	cpuStart := processCPU()
	started = time.Now()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	updates, reads, refreshes := 0, 0, 0
loop:
	for {
		select {
		case <-deadline.C:
			break loop
		case <-ticker.C:
			for range 32 {
				metric := name(updates % min(int(high), 10000))
				if err := db.UpdateMany(ctx, metric, []store.Point{{Timestamp: 1700000000 + int64(updates/10000), Value: float64(updates)}}); err != nil {
					t.Fatal(err)
				}
				updates++
			}
			path := "/" + strings.ReplaceAll(queries[reads%len(queries)], ".", "/")
			if _, _, _, _, err := l.CurrentFileIndex().trieIdx.query(path, 10000, nil); err != nil {
				t.Fatal(err)
			}
			reads++
			if mode != "heap_union" && reads%100 == 0 {
				if err := l.RefreshMetricStoreIndex(); err != nil {
					t.Fatal(err)
				}
				refreshes++
			}
		}
	}
	wall := time.Since(started)
	cpu := processCPU() - cpuStart
	after := readMetrics()
	runtime.GC()
	live := readMetrics()
	rssText, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	rssKB, err := strconv.ParseUint(strings.TrimSpace(string(rssText)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	if !l.PrepareReadCheckpoint() {
		t.Fatal("mapped checkpoint preparation failed")
	}
	// Exercise the pending-name warmup and overlay, rather than checkpointing
	// only an empty trie. Their count is independent of the 10M-name base.
	factory, finish := l.WarmCheckpointMetricLookups()
	known := factory()
	for i := range 1000 {
		if !known(fmt.Sprintf("pending.catalog_heap.metric%06d", i)) {
			t.Fatal("pending name not warmed")
		}
	}
	finish()
	if _, err := l.CheckpointReadIndex(); err != nil {
		t.Fatal(err)
	}
	stop := time.Since(started)
	result := map[string]any{"mode": mode, "metrics": high, "name_bytes": len(name(0)), "build_seconds": build.Seconds(), "heap_live_bytes": live["/gc/heap/live:bytes"], "heap_scan_bytes": live["/gc/scan/heap:bytes"], "window_seconds": wall.Seconds(), "gc_cpu_seconds": after["/cpu/classes/gc/total:cpu-seconds"] - before["/cpu/classes/gc/total:cpu-seconds"], "gc_cycles": after["/gc/cycles/total:gc-cycles"] - before["/gc/cycles/total:gc-cycles"], "updates": updates, "reads": reads, "refreshes": refreshes, "query_ns": latencies, "stop_seconds": stop.Seconds()}
	result["window_cpu_seconds"], result["rss_bytes"] = cpu, rssKB*1024
	result["index_sha256"] = fmt.Sprintf("%x", l.CurrentFileIndex().trieIdx.snapshot.manifest.Index.SHA256)
	raw, _ := json.Marshal(result)
	t.Log(string(raw))
	if err := os.WriteFile(filepath.Join(directory, mode+"-result.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(l)
}
