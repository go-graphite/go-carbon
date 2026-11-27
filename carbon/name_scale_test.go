package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// TestClassicNameScale is an opt-in comparison with the v2daemon name fixture.
// It measures the current classic App using its exact-block Pebble backend,
// cache, persister and mapped discovery index; it does not create Whisper files.
func TestClassicNameScale(t *testing.T) {
	text := os.Getenv("CLASSIC_SCALE_NAMES")
	if text == "" {
		t.Skip("opt-in full classic App scaling")
	}
	n, err := strconv.Atoi(text)
	root := os.Getenv("CLASSIC_SCALE_DIR")
	if err != nil || n < 1 || n > 5_000_000 || !filepath.IsAbs(root) {
		t.Fatal("requires count 1..5000000 and absolute fresh CLASSIC_SCALE_DIR")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("requires fresh root")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	path, cfg := sharedAppConfig(t)
	cfg.Common.MaxCPU = 4
	cfg.Whisper.StoreDir = filepath.Join(root, "store")
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreSyncInterval = &Duration{time.Second}
	cfg.Whisper.StoreCacheSize = 256 << 20
	cfg.Whisper.StoreMemTableSize = 64 << 20
	cfg.Carbonserver.ScanFrequency = &Duration{time.Hour}
	cfg.Buckyd.Enabled = false
	writeSharedConfig(t, path, cfg)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	app := startApp(t, path)
	defer app.Stop()
	ctx := context.Background()
	stamp := time.Now().Unix()
	for i := 0; i < n; i++ {
		length := [...]int{64, 96, 128, 192}[i%4]
		base := fmt.Sprintf("app.region.cluster.component.host%08d.requests.duration", i)
		name := base + strings.Repeat("x", max(0, length-len(base)))
		archives := []store.Retention{{Step: 10, Count: 8640}}
		if i%3 > 0 {
			archives = append(archives, store.Retention{Step: 60, Count: 10080})
		}
		if i%3 > 1 {
			archives = append(archives, store.Retention{Step: 3600, Count: 8760})
		}
		method := [...]store.AggregationMethod{store.Average, store.Sum, store.Last}[i%3]
		if _, err := app.MetricStore.Create(ctx, store.MetricConfig{Name: name, Retentions: archives, AggregationMethod: method, XFilesFactor: .5}); err != nil {
			t.Fatal(err)
		}
		if err := app.MetricStore.UpdateMany(ctx, name, []store.Point{{Timestamp: stamp, Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if count, err := app.MetricStore.CatalogHighWatermark(ctx); err != nil || count != uint64(n) {
		t.Fatalf("catalog count=%d want=%d err=%v", count, n, err)
	}
	seeded := time.Since(started)
	if err := app.Carbonserver.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	ready := time.Since(started)
	fmt.Printf("CLASSIC_READY names=%d seed_seconds=%f discovery_ready_seconds=%f\n", n, seeded.Seconds(), ready.Seconds())
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	peak := usage.Maxrss
	if runtime.GOOS != "darwin" {
		peak *= 1024
	}
	f, err := os.Create(filepath.Join(root, "heap.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{"when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "names": n, "seed_seconds": seeded.Seconds(), "discovery_ready_seconds": ready.Seconds(), "heap_alloc": memory.HeapAlloc, "heap_bytes_per_name": float64(int64(memory.HeapAlloc)-int64(before.HeapAlloc)) / float64(n), "heap_objects": memory.HeapObjects, "peak_rss_bytes": peak, "store_cache_bytes": cfg.Whisper.StoreCacheSize, "store_memtable_bytes": cfg.Whisper.StoreMemTableSize, "scope": "classic_app_exact_pebble_one_point_per_name", "gomemlimit": os.Getenv("GOMEMLIMIT")}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "result.json"), append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(b))
	runtime.KeepAlive(app)
}
