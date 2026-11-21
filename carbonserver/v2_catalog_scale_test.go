package carbonserver

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2index"
)

// Opt in explicitly: V2_INDEX_SCALE=10000000 V2_INDEX_BACKEND=trie go test
// ./carbonserver -run '^TestV2CatalogScale$' -v -count=1 -timeout=15m.
func TestV2CatalogScale(t *testing.T) {
	raw := os.Getenv("V2_INDEX_SCALE")
	if raw == "" {
		t.Skip("opt-in synthetic memory/build measurement")
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		t.Fatal("invalid scale")
	}
	backend := os.Getenv("V2_INDEX_BACKEND")
	if backend != "trie" && backend != "trigram" {
		t.Fatal("backend must be trie or trigram")
	}
	start := time.Now()
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("services.svc%03d.nodes.host%05d.metric%03d", i/100000, i/100%1000, i%100)
	}
	c := &v2index.Catalog{}
	if err := c.Publish(1, names); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	catalogSeconds := time.Since(start).Seconds()
	l := NewCarbonserverListener(nil)
	l.trieIndex = backend == "trie"
	l.maxGlobs = math.MaxInt
	l.maxMetricsGlobbed = math.MaxInt
	start = time.Now()
	if err := l.SetV2Catalog(c); err != nil {
		t.Fatal(err)
	}
	seconds := time.Since(start).Seconds()
	runtime.GC()
	runtime.ReadMemStats(&after)
	for _, i := range []int{0, n / 2, n - 1} {
		query := fmt.Sprintf("services.svc%03d.nodes.host%05d.metric%03d", i/100000, i/100%1000, i%100)
		result := l.expandV2Catalog(l.v2Catalog.Load(), query)
		if result.Err != nil || len(result.Files) != 1 || result.Files[0] != query || !result.Leafs[0] {
			t.Fatal("sample mismatch", result)
		}
	}
	report := map[string]any{"names": n, "backend": backend, "catalog_seconds": catalogSeconds, "build_seconds": seconds, "catalog_heap_bytes": before.HeapAlloc, "retained_heap_bytes": after.HeapAlloc, "index_heap_delta_bytes": int64(after.HeapAlloc) - int64(before.HeapAlloc), "build_total_alloc_bytes": after.TotalAlloc - before.TotalAlloc, "gomaxprocs": runtime.GOMAXPROCS(0)}
	data, _ := json.Marshal(report)
	t.Log(string(data))
	runtime.KeepAlive(l)
	runtime.KeepAlive(c)
}
