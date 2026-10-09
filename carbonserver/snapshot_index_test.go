package carbonserver

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func snapshotTrieFixture(t *testing.T) (*trieIndex, *trieIndex, string, string) {
	t.Helper()
	cache, root, entries := snapshotFixture(t)
	snapshot, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.close() })
	hybrid, oracle := newTrie(".wsp", 0, nil), newTrie(".wsp", 0, nil)
	hybrid.snapshot = snapshot
	for _, e := range entries {
		if _, err := oracle.insert(e.Path, e.LogicalSize, e.PhysicalSize, e.DataPoints, e.FirstSeenAt); err != nil {
			t.Fatal(err)
		}
	}
	return hybrid, oracle, cache, root
}

func canonicalIndexResult(names []string, leaves []bool) []string {
	result := make([]string, len(names))
	for i, name := range names {
		result[i] = fmt.Sprintf("%s/%t", name, leaves[i])
	}
	sort.Strings(result)
	return result
}

func TestSnapshotOverlayMatchesTrie(t *testing.T) {
	hybrid, oracle, _, _ := snapshotTrieFixture(t)
	for i, path := range []string{"/a/overlay.wsp", "/b/other.wsp", "/new/branch/value.wsp", "/new/branch.wsp", "/空间/new.wsp"} {
		for _, index := range []*trieIndex{hybrid, oracle} {
			if _, err := index.insert(path, int64(i+1), 17, 19, 12345); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got, want := hybrid.allMetrics('.'), oracle.allMetrics('.'); !reflect.DeepEqual(got, want) {
		t.Fatalf("all metrics %v != %v", got, want)
	}
	for _, expr := range []string{"*", "a/*", "b/*", "new/*", "new/branch/*", "空间/*", "{a,b,new}/*", "missing/*", "*/*/*"} {
		got, gl, _, _, ge := hybrid.query(expr, 10000, nil)
		want, wl, _, _, we := oracle.query(expr, 10000, nil)
		if ge != nil || we != nil || !reflect.DeepEqual(canonicalIndexResult(got, gl), canonicalIndexResult(want, wl)) {
			t.Fatalf("query %q %v/%v differs %v/%v", expr, got, gl, want, wl)
		}
	}
	for _, metric := range append(oracle.allMetrics('.'), "a.absent", "new.branch.absent", "missing.value") {
		_, gn := hybrid.metricPath(metric, nil)
		_, wn := oracle.metricPath(metric, nil)
		if gn != wn {
			t.Fatalf("existence %q %v != %v", metric, gn, wn)
		}
		gd, gn := hybrid.metricPath(metric, make([]*trieNode, 0))
		wd, wn := oracle.metricPath(metric, make([]*trieNode, 0))
		// Existing metrics return early without charging creation quotas.
		if gn != wn || (gn && len(gd) != len(wd)) {
			t.Fatalf("metric directories %q %d/%v != %d/%v", metric, len(gd), gn, len(wd), wn)
		}
	}
	for _, statsOnly := range []bool{false, true} {
		for _, prefix := range []string{"a", "b", "new", "new.branch", "空间"} {
			var results []*ListQueryResult
			for _, index := range []*trieIndex{hybrid, oracle} {
				listener := NewCarbonserverListener(nil)
				listener.SetTrieIndex(true)
				listener.UpdateFileIndex(&fileIndex{trieIdx: index})
				result, err := listener.queryMetricsList(prefix, 10000, false, statsOnly)
				if err != nil {
					t.Fatal(err)
				}
				sort.Slice(result.Metrics, func(i, j int) bool { return result.Metrics[i].Name < result.Metrics[j].Name })
				results = append(results, result)
			}
			if !reflect.DeepEqual(results[0], results[1]) {
				t.Fatalf("list prefix %q stats %v: %+v != %+v", prefix, statsOnly, results[0], results[1])
			}
		}
	}
}

func TestSnapshotOverlayQuotaParity(t *testing.T) {
	hybrid, oracle, _, _ := snapshotTrieFixture(t)
	quota := []*Quota{{Pattern: "/", Metrics: 100, Namespaces: 100}, {Pattern: "*", Metrics: 100, Namespaces: 100}, {Pattern: "*.*", Metrics: 100, Namespaces: 100}}
	for _, index := range []*trieIndex{hybrid, oracle} {
		index.estimateSize = func(string) (int64, int64, int64) { return 7, 11, 13 }
		for _, path := range []string{"/a/newdir/value.wsp", "/a/newdir/other.wsp", "/new/child.wsp", "/b/another.wsp"} {
			if _, err := index.insert(path, 7, 11, 13, 12345); err != nil {
				t.Fatal(err)
			}
		}
		tp, err := index.applyQuotas(time.Minute, quota...)
		if err != nil {
			t.Fatal(err)
		}
		index.refreshUsage(tp)
	}
	if len(hybrid.quotaNodes) != len(oracle.quotaNodes) {
		t.Fatal("quota node counts differ")
	}
	for name, want := range oracle.quotaNodes {
		got := hybrid.quotaNodes[name]
		if got == nil || *got.usage != *want.usage {
			t.Fatalf("quota %q %+v != %+v", name, got, want.usage)
		}
	}
	for _, index := range []*trieIndex{hybrid, oracle} {
		if _, err := index.applyQuotas(time.Minute, &Quota{Pattern: "a", Metrics: 1}); err != nil {
			t.Fatal(err)
		}
		index.refreshUsage(index.throughputs)
		if !index.throttle(points.OnePoint("a.more", 1, 1), false) {
			t.Fatal("namespace quota not enforced")
		}
		if index.throttle(points.OnePoint("a.value", 1, 1), false) {
			t.Fatal("existing metric rejected")
		}
		if index.throttle(points.OnePoint("new.other", 1, 1), false) {
			t.Fatal("removed quota still applied")
		}
	}
}

func TestSnapshotNewMetricAncestorsMatchTrie(t *testing.T) {
	hybrid, oracle, _, _ := snapshotTrieFixture(t)
	for _, index := range []*trieIndex{hybrid, oracle} {
		for _, path := range []string{"/a/newdir/value.wsp", "/overlay/deep/value.wsp", "/空间/新/值.wsp"} {
			if _, err := index.insert(path, 7, 11, 13, 12345); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, metric := range []string{
		"a.value", "a.new", "a.newdir.new", "a.newdir.value.child", "a.unknown.child",
		"a-sibling.new", "a0.new", "b.child.new", "b.child.more.new", "unknown.child",
		"overlay.deep.new", "overlay.deep.value.child", "空间.新.另一", "空间.未见.值",
	} {
		got, gotNew := hybrid.metricPath(metric, make([]*trieNode, 0, 32))
		want, wantNew := oracle.metricPath(metric, make([]*trieNode, 0, 32))
		if gotNew != wantNew || gotNew && len(got) != len(want) {
			t.Errorf("%q: new=%v ancestors=%d, want new=%v ancestors=%d", metric, gotNew, len(got), wantNew, len(want))
		}
	}
}

func TestSnapshotAdmissionMatchesRangeChecks(t *testing.T) {
	hybrid, _, _, _ := snapshotTrieFixture(t)
	for _, metric := range []string{"", ".a", "a..value", "a/value.new", "/.new", "a.\x00.new", "a.missing.child", "空间.新.值"} {
		got, gotNew := hybrid.metricPath(metric, make([]*trieNode, 0, 32))
		want, wantNew := snapshotMetricPathWithRanges(hybrid, metric, make([]*trieNode, 0, 32))
		if gotNew != wantNew || !reflect.DeepEqual(got, want) {
			t.Errorf("%q: admission ancestors differ from range checks", metric)
		}
	}
}

func TestSnapshotQuotaReloadSeedsExistingUsage(t *testing.T) {
	hybrid, oracle, _, _ := snapshotTrieFixture(t)
	for _, index := range []*trieIndex{hybrid, oracle} {
		index.estimateSize = func(string) (int64, int64, int64) { return 7, 11, 13 }
		for _, path := range []string{"/a/newdir/value.wsp", "/a/newdir/other.wsp", "/b/child/new.wsp", "/overlay/deep/value.wsp"} {
			if _, err := index.insert(path, 7, 11, 13, 12345); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := index.applyQuotas(time.Minute, &Quota{Pattern: "/", Metrics: 100}); err != nil {
			t.Fatal(err)
		}
		index.refreshUsage(index.throughputs)
	}
	// Match previously unrestricted namespaces without a periodic usage refresh.
	// Remove and re-add the same rules to cover reuse of cached virtual nodes.
	for _, rules := range [][]*Quota{
		{{Pattern: "*", Metrics: 1}, {Pattern: "*.*", Metrics: 1}},
		{},
		{{Pattern: "*", Metrics: 1}, {Pattern: "*.*", Metrics: 1}},
	} {
		for _, index := range []*trieIndex{hybrid, oracle} {
			if _, err := index.applyQuotas(time.Minute, rules...); err != nil {
				t.Fatal(err)
			}
		}
		for name, want := range oracle.quotaNodes {
			got := hybrid.quotaNodes[name]
			if got == nil || got.usage.Metrics != want.usage.Metrics || got.usage.Namespaces != want.usage.Namespaces ||
				got.usage.LogicalSize != want.usage.LogicalSize || got.usage.PhysicalSize != want.usage.PhysicalSize || got.usage.DataPoints != want.usage.DataPoints {
				t.Fatalf("new quota %q: got %+v want %+v", name, got, want.usage)
			}
		}
		for _, metric := range []string{"a.new", "a.newdir.new", "b.child.newer", "overlay.deep.new"} {
			if hybrid.throttle(points.OnePoint(metric, 1, 1), false) != oracle.throttle(points.OnePoint(metric, 1, 1), false) {
				t.Fatalf("new quota admission differs for %q", metric)
			}
		}
	}
}

func TestSnapshotStartupReadinessAndReconciliation(t *testing.T) {
	_, _, cache, root := snapshotTrieFixture(t)
	listener := NewCarbonserverListener(nil)
	listener.SetWhisperData(root)
	listener.SetTrieIndex(true)
	listener.SetConcurrentIndex(true)
	listener.SetFileListCache(cache)
	listener.SetFileListCacheVersion(int(FLCVersion2))
	listener.SetQuotaUsageReportFrequency(time.Minute)
	listener.SetQuotas([]*Quota{{Pattern: "/", Metrics: 1}})
	listener.SetEstimateSize(func(string) (int64, int64, int64) { return 1, 2, 3 })
	ch := listener.SetRealtimeIndex(10)
	ch <- "a.pending"
	if !listener.updateFileList(root, nil, nil) {
		t.Fatal("snapshot not used for startup")
	}
	first := listener.CurrentFileIndex()
	if first == nil || first.trieIdx.snapshot == nil {
		t.Fatal("startup rebuilt legacy trie")
	}
	if !listener.MetricExists("a.value") || !listener.MetricExists("a.pending") {
		t.Fatal("startup missed complete snapshot or queued notification")
	}
	if !listener.ShouldThrottleMetric(points.OnePoint("a.more", 1, 1), false) {
		t.Fatal("ready before quota enforcement")
	}
	// Keep a query on the retired index alive across on-disk generation replacement.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var reads atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			names, _, _, _, err := first.trieIdx.query("a/*", 100, nil)
			if err != nil || len(names) != 2 {
				t.Errorf("retired snapshot read: %v/%v", names, err)
				return
			}
			reads.Add(1)
		}
	}()
	for _, name := range []string{"a/pending.wsp", "new/value.wsp"} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(root, "a-sibling/value.wsp")); err != nil {
		t.Fatal(err)
	}
	if listener.updateFileList(root, nil, nil) {
		t.Fatal("scan unexpectedly loaded old cache")
	}
	close(stop)
	wg.Wait()
	next := listener.CurrentFileIndex()
	if next.trieIdx.snapshot == nil || next.trieIdx.snapshot == first.trieIdx.snapshot {
		t.Fatal("new complete generation not installed")
	}
	for _, name := range []string{"a.value", "a.pending", "new.value"} {
		if !listener.MetricExists(name) {
			t.Fatal("missing", name)
		}
	}
	if listener.MetricExists("a-sibling.value") {
		t.Fatal("deleted metric retained")
	}
	if next.trieIdx.fileCount != 0 {
		t.Fatalf("persisted metrics remained in overlay: %d", next.trieIdx.fileCount)
	}
	if reads.Load() == 0 {
		t.Fatal("no overlapping read")
	}
	// A missing/incompatible accelerator must retain the complete legacy fallback.
	if err := os.WriteFile(snapshotManifestPath(cache), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	// Older cache versions cannot build the accelerator and still use the trie.
	rewriteCacheVersion(t, cache, FLCVersion1)
	fallback := NewCarbonserverListener(nil)
	fallback.SetWhisperData(root)
	fallback.SetTrieIndex(true)
	fallback.SetConcurrentIndex(true)
	fallback.SetFileListCache(cache)
	if !fallback.updateFileList(root, nil, nil) || fallback.CurrentFileIndex().trieIdx.snapshot != nil {
		t.Fatal("legacy fallback failed")
	}
	for _, name := range next.trieIdx.allMetrics('.') {
		if !fallback.MetricExists(name) {
			t.Fatal("fallback missing", strings.TrimSpace(name))
		}
	}
}

func TestRealtimeIndexSkipsUnstorableMetrics(t *testing.T) {
	_, _, cache, root := snapshotTrieFixture(t)
	long := "flink.tsk." + strings.Repeat("operator-", 40) + ".numRecordsIn"
	l := NewCarbonserverListener(func(name string) []points.Point {
		if name == "pending.value" || name == long {
			return []points.Point{{Value: 1, Timestamp: 1}}
		}
		return nil
	})
	l.SetWhisperData(root)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetFileListCache(cache)
	l.SetFileListCacheVersion(int(FLCVersion2))
	l.SetRealtimeIndex(10)
	if !l.updateFileList(root, nil, nil) {
		t.Fatal("warmup failed")
	}
	trie := l.CurrentFileIndex().trieIdx
	if node := l.insertRealtimeMetric(trie, long); node != nil || l.MetricExists(long) {
		t.Fatal("indexed a metric the persister cannot store")
	}
	l.insertRealtimeMetric(trie, "pending.value")
	// An index or saved overlay from an earlier release may still hold one.
	path, _ := l.storableMetricPath(long)
	if _, err := trie.insert(path, 0, 0, 0, 0); err != nil || !l.MetricExists(long) {
		t.Fatal("setup failed", err)
	}
	l.updateFileList(root, nil, nil)
	if !l.MetricExists("pending.value") {
		t.Fatal("scan removed accepted points awaiting persistence")
	}
	if l.MetricExists(long) {
		t.Fatal("scan retained a metric the persister cannot store")
	}
	cached := splitAndInsert(map[string]struct{}{}, []map[string]struct{}{{long: {}, "cached.value": {}}}, func(metric string) bool {
		_, ok := l.storableMetricPath(metric)
		return ok
	})
	if _, ok := cached["/cached/value.wsp"]; !ok || len(cached) != 2 {
		t.Fatalf("cache-scan names: %v", cached)
	}
}

func TestSnapshotScanPreservesUnpersistedMetric(t *testing.T) {
	_, _, cache, root := snapshotTrieFixture(t)
	l := NewCarbonserverListener(func(name string) []points.Point {
		if name == "pending.value" {
			return []points.Point{{Value: 1, Timestamp: 1}}
		}
		return nil
	})
	l.SetWhisperData(root)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetFileListCache(cache)
	l.SetFileListCacheVersion(int(FLCVersion2))
	l.SetRealtimeIndex(10)
	if !l.updateFileList(root, nil, nil) {
		t.Fatal("warmup failed")
	}
	l.insertRealtimeMetric(l.CurrentFileIndex().trieIdx, "pending.value")
	l.insertRealtimeMetric(l.CurrentFileIndex().trieIdx, "removed.value")
	l.updateFileList(root, nil, nil)
	if !l.MetricExists("pending.value") {
		t.Fatal("scan removed accepted points awaiting persistence")
	}
	if l.MetricExists("removed.value") {
		t.Fatal("scan retained removed metric with no pending points")
	}
}
