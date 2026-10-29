package carbonserver

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestExpirationBatchRefreshesIndexAndQuotaUsage(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("mapped=%v", mapped), func(t *testing.T) {
			checkExpirationBatchRefreshesIndexAndQuotaUsage(t, mapped)
		})
	}
}

func checkExpirationBatchRefreshesIndexAndQuotaUsage(t *testing.T, mapped bool) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var metrics []store.Metadata
	for _, name := range []string{"jobs.a", "jobs.b"} {
		m, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average})
		if err != nil {
			t.Fatal(err)
		}
		metrics = append(metrics, m)
	}
	listener := newMetricStoreTestListener(t, cache.New(), db, true)
	defer listener.Stop()
	if mapped {
		listener.SetFileListCache(filepath.Join(t.TempDir(), "files.gz"))
		listener.SetFileListCacheVersion(2)
		listener.SetIncrementalIndexScratch(t.TempDir())
		if err := listener.RefreshMetricStoreIndex(); err != nil {
			t.Fatal(err)
		}
	}
	listener.SetQuotas([]*Quota{{Pattern: "/", Metrics: 2}})
	if err := listener.applySharedStoreQuotas(listener.CurrentFileIndex().trieIdx); err != nil {
		t.Fatal(err)
	}
	rootUsage := func() *QuotaUsage { return listener.CurrentFileIndex().trieIdx.root.meta.Load().(*dirMeta).usage }
	if got := atomic.LoadInt64(&rootUsage().Metrics); got != 2 {
		t.Fatalf("initial usage %d", got)
	}
	outcomes, err := db.DeleteBatchIfUnchanged(ctx, metrics[:1])
	if err != nil || outcomes[0] != nil {
		t.Fatalf("delete %v %v", outcomes, err)
	}
	if err := listener.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	assertStoreGlob(t, listener, "jobs.a", false)
	assertStoreGlob(t, listener, "jobs.b", true)
	if got := atomic.LoadInt64(&rootUsage().Metrics); got != 1 {
		t.Fatalf("stale quota metric usage %d", got)
	}
	if got := atomic.LoadInt64(&rootUsage().LogicalSize); got != 16+12+60*12 {
		t.Fatalf("stale quota bytes %d", got)
	}
	if listener.HasMappedIndex() != mapped {
		t.Fatal("expiration changed the discovery representation")
	}
}
