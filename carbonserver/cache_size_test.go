package carbonserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// A cache of a given size in megabytes keeps entries and evicts them once the
// stored bytes exceed it. Gzip entries count their stored size, so the same
// budget holds more of them.
func TestQueryCacheSizeMB(t *testing.T) {
	t.Parallel()
	metrics := make([]string, 10)
	for i := range metrics {
		metrics[i] = fmt.Sprintf("servers.api.m%d", i)
	}
	listener, server := newQueryCacheTestServer(t, metrics, 600, renderSampleMixed)
	listener.SetQueryCacheSizeMB(1)
	const requests = 200
	items := map[string]int{}
	for _, compression := range []string{"none", "gzip"} {
		setQueryCacheCompression(t, listener, compression)
		hits := atomic.LoadUint64(&listener.metrics.QueryCacheHit)
		for i := range requests {
			r := queryCacheRequest(t, server, http.MethodGet, "protobuf", "servers.api.*", queryCacheTestNow-600+i, true)
			if r.status != http.StatusOK {
				t.Fatalf("%s: status %d", compression, r.status)
			}
			if i == 0 {
				queryCacheRequest(t, server, http.MethodGet, "protobuf", "servers.api.*", queryCacheTestNow-600, true)
				if atomic.LoadUint64(&listener.metrics.QueryCacheHit) != hits+1 {
					t.Fatalf("%s: a 1 MB cache did not keep the response", compression)
				}
			}
		}
		ec := listener.queryCache.ec
		items[compression] = ec.Items()
		if ec.Size() > 1<<20 || ec.Items() >= requests || ec.Items() < 5 {
			t.Fatalf("%s: %d entries take %d bytes after %d responses; want at least 5 within 1 MB", compression, ec.Items(), ec.Size(), requests)
		}
	}
	if items["gzip"] < 2*items["none"] {
		t.Fatalf("1 MB holds %d gzip entries and %d uncompressed ones", items["gzip"], items["none"])
	}
}

func TestFindAndGlobCacheSizeMB(t *testing.T) {
	t.Parallel()
	listener := newDiscoveryCacheTestListener(t)
	trie := listener.CurrentFileIndex().trieIdx
	// 300 directories of 50 metrics with long names: 10-20 KB per result.
	long := strings.Repeat("x", 200)
	for dir := range 300 {
		for m := range 50 {
			if listener.insertRealtimeMetric(trie, fmt.Sprintf("sized.d%03d.%s%02d", dir, long, m)) == nil {
				t.Fatal("insert failed")
			}
		}
	}
	listener.SetFindCacheSizeMB(1)
	listener.SetGlobCacheSizeMB(1)
	ctx, logger := context.Background(), zap.NewNop()
	for _, tt := range []struct {
		name  string
		cache *expireCache
		query func(string) bool
	}{
		{"find", &listener.findCache, func(q string) bool {
			_, hit, err := listener.findResponse(ctx, logger, time.Now(), jsonFormat, "json", []string{q})
			if err != nil {
				t.Fatal(err)
			}
			return hit
		}},
		{"glob", &listener.globCache, func(q string) bool {
			_, hit, err := listener.getExpandedGlobsWithCache(ctx, logger, "render", []string{q})
			if err != nil {
				t.Fatal(err)
			}
			return hit
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Finds fill the glob cache too, so start both empty.
			listener.initializeCaches()
			if tt.query("sized.d000.*") || !tt.query("sized.d000.*") {
				t.Fatal("a 1 MB cache did not keep the result")
			}
			for dir := range 300 {
				tt.query(fmt.Sprintf("sized.d%03d.*", dir))
			}
			ec := tt.cache.ec
			if ec.Size() > 1<<20 || ec.Items() >= 300 || ec.Items() < 20 {
				t.Fatalf("%d entries take %d bytes after 300 results; want at least 20 within 1 MB", ec.Items(), ec.Size())
			}
		})
	}
}
