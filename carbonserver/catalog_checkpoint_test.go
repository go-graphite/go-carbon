package carbonserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

func TestCatalogCheckpointRestartAppliesOnlyDelta(t *testing.T) {
	ctx := context.Background()
	root, directory, cache := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "files.gz")
	options := store.Options{MetadataCacheSize: 1 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
	db, err := store.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	listener := func() *CarbonserverListener {
		l, _ := newScanTestListener(root, cache, 1)
		l.logger = zap.NewNop()
		l.SetMetricStore(db)
		l.SetMetricStoreFileIndex(false)
		l.SetScanFrequency(time.Hour)
		l.SetQuotas(nil)
		t.Cleanup(func() { _ = l.Stop() })
		return l
	}
	create := func(name string) {
		t.Helper()
		_, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range 4096 {
		create(fmt.Sprintf("services.app%d.host%d.requests", i%32, i))
	}
	first := listener()
	if err := first.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	generation := first.CurrentFileIndex().trieIdx.snapshot.manifest.Index.Name
	create("created.since.scan") // Not delivered to realtime discovery yet.
	// Production prepares the metadata checkpoint after stopping writers, before
	// saving the discovery overlay. Close must not change the certified sequence.
	if err := db.PrepareMetadataCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := first.CheckpointReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := readOverlayManifest(overlayManifestPath(cache))
	if err != nil || manifest.CatalogCheckpoint == "" {
		t.Fatalf("uncertified: %+v, %v", manifest, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = store.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	next := listener()
	next.WarmupIndexWithPending(func(got string, visit, _ func(string) error) error {
		if got != id {
			return fmt.Errorf("identity %s want %s", got, id)
		}
		return visit("pending.unclaimed")
	})
	next.WaitForWarmup()
	if next.metricStoreIndex != db || next.metricStoreCursor != 1 {
		t.Fatal("certified catalog did not seed restart cursor")
	}
	// A poisoned full-build scratch path proves that this refresh uses only the
	// journal, including writes committed after Open and before warmup ends.
	poison := filepath.Join(t.TempDir(), "not-directory")
	if err := os.WriteFile(poison, nil, 0600); err != nil {
		t.Fatal(err)
	}
	next.SetIncrementalIndexScratch(poison)
	create("created.after.open")
	if err := next.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if next.CurrentFileIndex().trieIdx.snapshot.manifest.Index.Name != generation {
		t.Fatal("restart rebuilt base")
	}
	for _, name := range []string{"services.app0.host0.requests", "created.since.scan", "created.after.open", "pending.unclaimed"} {
		if !next.MetricExists(name) {
			t.Fatal("lost name", name)
		}
	}
}

func TestCatalogCheckpointRejectsChangedStoreAndDeletedNames(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		t.Run(fmt.Sprint(deletion), func(t *testing.T) {
			ctx := context.Background()
			root, directory, cache := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "files.gz")
			options := store.Options{MetadataCacheSize: 1 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			db, err := store.Open(directory, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			create := func(name string) {
				t.Helper()
				if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
					t.Fatal(err)
				}
			}
			l, _ := newScanTestListener(root, cache, 1)
			l.logger = zap.NewNop()
			l.SetMetricStore(db)
			defer l.Stop()
			create("stored.metric")
			if err := l.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			if deletion {
				if err := db.Delete(ctx, "stored.metric"); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.PrepareMetadataCheckpoint(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := l.CheckpointReadIndex(); err != nil {
				t.Fatal(err)
			}
			m, err := readOverlayManifest(overlayManifestPath(cache))
			if err != nil {
				t.Fatal(err)
			}
			if deletion && m.CatalogCheckpoint != "" {
				t.Fatal("certified a deletion absent from saved index")
			}
			// A write after the certificate invalidates it even if the same directory
			// and every saved accelerator file survive the restart.
			create("unindexed.after.checkpoint")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(directory, options)
			if err != nil {
				t.Fatal(err)
			}
			next, _ := newScanTestListener(root, cache, 1)
			next.logger = zap.NewNop()
			next.SetMetricStore(db)
			next.SetScanFrequency(time.Hour)
			defer next.Stop()
			next.WarmupIndexWithPending(func(string, func(string) error, func(string) error) error { return nil })
			next.WaitForWarmup()
			if next.metricStoreIndex != nil {
				t.Fatal("changed store accepted certificate")
			}
			if err := next.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			if !next.MetricExists("unindexed.after.checkpoint") || deletion && next.MetricExists("stored.metric") {
				t.Fatal("fallback did not use authoritative catalog")
			}
		})
	}
}

// All checkpoint-only points are still unclaimed: cacheGet returns nothing and
// no persister has created their store metadata. The background rebuild must
// preserve discovery and quota accounting until those writers can run.
func TestCatalogRebuildPreservesUnclaimedCheckpointNamesAndQuota(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, cache := t.TempDir(), filepath.Join(t.TempDir(), "files.gz")
	l, _ := newScanTestListener(root, cache, 1)
	l.logger = zap.NewNop()
	l.SetMetricStore(db)
	defer l.Stop()
	config := store.MetricConfig{Name: "history.persisted", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	// Match the configured quota estimate for pending names, as normal startup
	// does before loading its saved index and recovery checkpoint.
	l.SetEstimateSize(func(string) (int64, int64, int64) { return 748, 0, 60 })
	l.CurrentFileIndex().trieIdx.estimateSize = l.estimateSize
	for _, name := range []string{"recovery.unclaimed.a", "recovery.unclaimed.b"} {
		if err := l.CurrentFileIndex().trieIdx.insertPendingMetric(name); err != nil {
			t.Fatal(err)
		}
	}
	l.metricStoreCursor = 0 // force the real full-catalog path before writeout
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	ti := l.CurrentFileIndex().trieIdx
	for _, name := range []string{"history.persisted", "recovery.unclaimed.a", "recovery.unclaimed.b"} {
		if !l.MetricExists(name) {
			t.Fatal("rebuild hid checkpoint name", name)
		}
	}
	before, err := ti.snapshotQuotaUsage("", ti.snapshot.namespaceExists)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"recovery.unclaimed.a", "recovery.unclaimed.b"} {
		config.Name = name
		if _, err := db.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	l.metricStoreCursor = 0
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	after, err := l.CurrentFileIndex().trieIdx.snapshotQuotaUsage("", l.CurrentFileIndex().trieIdx.snapshot.namespaceExists)
	if err != nil {
		t.Fatal(err)
	}
	if before.Metrics != 3 || before.LogicalSize != 2244 || before.DataPoints != 180 {
		t.Fatalf("incomplete pre-drain usage: %+v", before)
	}
	if before != after {
		t.Fatalf("quota changed across recovery persistence: before=%+v after=%+v", before, after)
	}
}
