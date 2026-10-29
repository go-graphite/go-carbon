package carbonserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func TestMetricStoreCompletedCatalogHeapBound(t *testing.T) {
	const count = 32768
	ctx := context.Background()
	root := t.TempDir()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, IncrementalSource: root, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := range count {
		if _, err := db.Create(ctx, store.MetricConfig{Name: catalogHeapClientName(i), Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	// Warm the bounded store read cache before measuring discovery's retained
	// heap. No catalog page or slice of all names survives this traversal.
	if err := store.EachPage(ctx, db.ListPage, "", metricStoreCatalogPageSize, func([]store.Metadata) error { return nil }); err != nil {
		t.Fatal(err)
	}
	l, _ := newScanTestListener(root, filepath.Join(t.TempDir(), "files.gz"), 1)
	l.logger = zap.NewNop()
	l.SetMetricStore(db)
	l.SetEstimateSize(nil)
	l.SetIncrementalIndexScratch(t.TempDir())
	// This is the catalog-only selection made after migration completion.
	l.SetMetricStoreFileIndex(false)
	defer l.Stop()
	heap := func() int64 {
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return int64(stats.HeapAlloc)
	}
	before := heap()
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	growth := heap() - before
	t.Logf("completed catalog: names=%d live_heap_growth=%d bytes_per_name=%.2f", count, growth, float64(growth)/count)
	if growth >= count*64 {
		t.Fatalf("completed catalog retained %d heap bytes for %d names, want <64 bytes/name", growth, count)
	}
	index := l.CurrentFileIndex()
	if !l.HasMappedIndex() || index.trieIdx.snapshot.manifest.Records != count || index.trieIdx.getFileCount() != 0 || len(index.details) != 0 {
		t.Fatal("completed catalog retained per-name heap state")
	}
	for _, i := range []int{0, count / 2, count - 1} {
		if !l.MetricExists(catalogHeapClientName(i)) {
			t.Fatal("mapped catalog lost metric", i)
		}
	}
	runtime.KeepAlive(l)
}

func TestMetricStoreMappedCatalogMatchesTrie(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, IncrementalSource: root, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l, _ := newScanTestListener(root, filepath.Join(t.TempDir(), "files.gz"), 1)
	l.logger = zap.NewNop()
	l.SetMetricStore(db)
	l.SetEstimateSize(nil)
	l.SetMaxGlobs(100)
	l.SetIncrementalIndexScratch(t.TempDir())
	oracle := newTrie(".wsp", 0, nil)
	create := func(name string) store.MetricConfig {
		t.Helper()
		config := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Sum}
		m, err := db.Create(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		logical, points := metricStoreLogicalSize(m)
		if _, err := oracle.insert(metricStoreVirtualPath(name), logical, 0, points, 0); err != nil {
			t.Fatal(err)
		}
		return config
	}
	for _, name := range []string{"a", "a.child", "a-sibling.child", "a0.child", "空间.值", "kafka.by_cluster.c.by_topic.t.by_partition.0.count"} {
		create(name)
	}
	// A foreign file must not enter catalog discovery, even on a required
	// full rebuild. The migration walker also rejects this source symlink.
	writeScanFile(t, filepath.Join(root, "must-not-index.wsp"), 1)
	if err := os.Symlink(root, filepath.Join(root, "must-not-walk")); err != nil {
		t.Fatal(err)
	}
	refresh := func() {
		t.Helper()
		if err := l.RefreshMetricStoreIndex(); err != nil {
			t.Fatal(err)
		}
		if !l.HasMappedIndex() || len(l.CurrentFileIndex().details) != 0 {
			t.Fatal("catalog materialized per-metric heap state")
		}
		if l.MetricExists("must-not-index") {
			t.Fatal("catalog refresh consulted the source tree")
		}
	}
	refresh()
	initial := l.CurrentFileIndex().trieIdx
	refresh()
	if l.CurrentFileIndex().trieIdx != initial {
		t.Fatal("idle refresh replaced generation")
	}
	cache := cache.New()
	l.cacheGet = cache.Get
	cache.Add(&points.Points{Metric: "pending.only", Data: []points.Point{{Value: 1, Timestamp: 1700000000}}})
	l.insertRealtimeMetric(initial, "pending.only")
	l.insertRealtimeMetric(initial, "new.metric")
	changed := create("new.metric")
	refresh()
	if l.CurrentFileIndex().trieIdx != initial || initial.getFileCount() != 2 {
		t.Fatal("create did not extend the small overlay")
	}
	for _, pattern := range []string{"a*", "a.*", "{a,a0}.*", "new.*", "空间.*", "kafka.by_cluster.*.by_topic.*.by_partition.*.*", "absent.*"} {
		path := "/" + strings.ReplaceAll(pattern, ".", "/")
		want, wl, _, _, err := oracle.query(path, 100, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, gl, _, _, err := initial.query(path, 100, nil)
		if len(want) == 0 && pattern != "absent.*" {
			t.Fatalf("empty oracle for %s", pattern)
		}
		rows := func(names []string, leaves []bool) []snapshotResult {
			result := make([]snapshotResult, len(names))
			for i, name := range names {
				result[i] = snapshotResult{name: name, leaf: leaves[i]}
			}
			sort.Slice(result, func(i, j int) bool {
				return snapshotResultLess(result[i].name, result[i].leaf, result[j].name, result[j].leaf)
			})
			return result
		}
		if err != nil || !reflect.DeepEqual(rows(got, gl), rows(want, wl)) {
			t.Fatalf("%s: got %v/%v want %v/%v: %v", pattern, got, gl, want, wl, err)
		}
	}
	changed.Name = "a"
	changed.Retentions[0].Count = 120
	if _, err := db.Replace(ctx, store.Snapshot{Metadata: store.Metadata{MetricConfig: changed}, Archives: []store.Archive{{Retention: changed.Retentions[0]}}}); err != nil {
		t.Fatal(err)
	}
	refresh()
	if l.CurrentFileIndex().trieIdx == initial {
		t.Fatal("policy change retained old generation")
	}
	entry, found, err := l.CurrentFileIndex().trieIdx.snapshot.lookup(metricStoreVirtualPath(changed.Name))
	if err != nil || !found || entry.LogicalSize != 16+12+120*12 {
		t.Fatalf("policy metadata: %v %v %v", entry, found, err)
	}
	if err := db.Delete(ctx, "a.child"); err != nil {
		t.Fatal(err)
	}
	refresh()
	if l.MetricExists("a.child") || !l.MetricExists("a") {
		t.Fatal("deletion resurrected source or lost parent metric")
	}
	// Force the count boundary without an oversized unit-test fixture.
	initial = l.CurrentFileIndex().trieIdx
	initial.fileCount = metricStoreOverlayLimit
	create("compact.metric")
	refresh()
	if l.CurrentFileIndex().trieIdx == initial || l.CurrentFileIndex().trieIdx.getFileCount() != 1 || !l.MetricExists("pending.only") {
		t.Fatal("overlay was not compacted into the mapped base")
	}
	before, cursor := l.CurrentFileIndex(), l.metricStoreCursor
	l.metricStoreCursor = 0 // journal reset must use the same catalog builder
	poison := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(poison, nil, 0600); err != nil {
		t.Fatal(err)
	}
	l.SetIncrementalIndexScratch(poison)
	if err := l.RefreshMetricStoreIndex(); err == nil || l.CurrentFileIndex() != before || l.metricStoreCursor != 0 {
		t.Fatal("failed rebuild replaced live index or advanced journal")
	}
	l.SetIncrementalIndexScratch(t.TempDir())
	refresh()
	if l.metricStoreCursor < cursor || !l.MetricExists("compact.metric") {
		t.Fatal("journal reset lost committed names")
	}
}

func TestMetricStoreMappedNotificationsDuringRefresh(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, IncrementalSource: root, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l, _ := newScanTestListener(root, filepath.Join(t.TempDir(), "files.gz"), 1)
	l.logger = zap.NewNop()
	l.SetMetricStore(db)
	l.SetScanFrequency(time.Hour)
	l.SetQuotas(nil)
	l.WarmupIndex()
	l.WaitForWarmup()
	defer l.Stop()
	c := cache.New()
	l.cacheGet = c.Get
	ctx := context.Background()
	for i := range 32 {
		name := fmt.Sprintf("notified.metric%02d", i)
		c.Add(points.OnePoint(name, 1, 1700000000))
		l.newMetricsChan <- name
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
		if i%8 == 0 {
			l.metricStoreIndexMu.Lock()
			l.metricStoreCursor = 0
			l.metricStoreIndexMu.Unlock()
		}
		if err := l.RefreshMetricStoreIndex(); err != nil {
			t.Fatal(err)
		}
	}
	// Stop the real notification worker and drain its bounded backlog through
	// the checkpoint path. Every committed name must survive generation swaps.
	if _, err := l.CheckpointReadIndex(); err != nil {
		t.Fatal(err)
	}
	for i := range 32 {
		if !l.MetricExists(fmt.Sprintf("notified.metric%02d", i)) {
			t.Fatal("refresh lost a live notification", i)
		}
	}
}
