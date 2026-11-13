package carbonserver

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRealtimeBurstDuringScan(t *testing.T) {
	for _, workers := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			cachePath, root, _ := snapshotFixture(t)
			c := cache.New()
			l := NewCarbonserverListener(c.Get)
			l.SetWhisperData(root)
			l.SetTrieIndex(true)
			l.SetConcurrentIndex(true)
			l.SetFileListCache(cachePath)
			l.SetFileListCacheVersion(int(FLCVersion2))
			l.SetScanWorkers(workers)
			l.SetQuotaUsageReportFrequency(time.Minute)
			l.SetQuotas([]*Quota{{Pattern: "/", Metrics: 10000}})
			l.SetMaxMetricsGlobbed(10000)
			c.SetNewMetricsQueue(l.SetRealtimeIndex(1))
			c.SetMetricExists(l.MetricExists)
			c.SetBloomSize(10000)
			l.updateFileList(root, nil, nil)
			previous := l.CurrentFileIndex()
			writeScanFile(t, filepath.Join(root, "scan", "block.wsp"), 0)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var blocked atomic.Bool
			l.SetEstimateSize(func(metric string) (int64, int64, int64) {
				if metric == "scan.block" && blocked.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
				return 10, 20, 30
			})
			// The snapshot's estimator was configured at construction, so only the
			// scan's disk metadata lookup waits on the barrier.
			force := make(chan struct{}, 1)
			l.startFileListUpdater(root, nil, force, l.exitChan)
			t.Cleanup(func() { once.Do(func() { close(release) }); _ = l.Stop() })
			c.Add(points.OnePoint("before.scan", 1, 1))
			beforeDeadline := time.Now().Add(5 * time.Second)
			for !l.MetricExists("before.scan") && time.Now().Before(beforeDeadline) {
				time.Sleep(time.Millisecond)
			}
			if !l.MetricExists("before.scan") {
				t.Fatal("pre-scan name was not indexed")
			}
			force <- struct{}{}
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("scan did not reach barrier")
			}
			const count = 1024
			var wg sync.WaitGroup
			for worker := range 8 {
				wg.Go(func() {
					for i := worker; i < count; i += 8 {
						c.Add(points.OnePoint(fmt.Sprintf("burst.metric%04d", i), 1, 1))
					}
				})
			}
			added := make(chan struct{})
			go func() { wg.Wait(); close(added) }()
			select {
			case <-added:
			case <-time.After(10 * time.Second):
				once.Do(func() { close(release) })
				<-added
				t.Fatal("scan blocked realtime ingestion")
			}
			assertNames := func() bool {
				for i := range count {
					if !l.MetricExists(fmt.Sprintf("burst.metric%04d", i)) {
						return false
					}
				}
				return true
			}
			deadline := time.Now().Add(5 * time.Second)
			for !assertNames() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !assertNames() {
				t.Fatal("burst names missing before scan finished")
			}
			if l.CurrentFileIndex() != previous {
				t.Fatal("scan passed its barrier")
			}
			// Exercise real glob expansion, including its limit, while the scan waits.
			globs, _, err := l.getExpandedGlobsWithCache(context.Background(), zap.NewNop(), "find", []string{"burst.*"})
			if err != nil || len(globs) != 1 || len(globs[0].Files) != count {
				t.Fatalf("find during scan: %v, %v", globs, err)
			}
			l.metricStoreIndexMu.Lock()
			names, leaves, _, _, err := previous.trieIdx.query("burst/*", 7, nil)
			l.metricStoreIndexMu.Unlock()
			if err != nil || len(names) != 7 || len(leaves) != 7 {
				t.Fatalf("glob limit: %d/%d, %v", len(names), len(leaves), err)
			}
			// Persistence can remove points after the scanner listed the root. The
			// new directory will be absent from this generation despite a disk write.
			writeScanFile(t, filepath.Join(root, "before", "scan.wsp"), 0)
			c.Pop("before.scan")
			once.Do(func() { close(release) })
			deadline = time.Now().Add(10 * time.Second)
			for l.CurrentFileIndex() == previous && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if l.CurrentFileIndex() == previous {
				t.Fatal("scan did not publish replacement")
			}
			if !assertNames() || !l.MetricExists("before.scan") {
				t.Fatal("snapshot swap lost realtime names")
			}
			l.metricStoreIndexMu.Lock()
			ti := l.CurrentFileIndex().trieIdx
			got := atomic.LoadInt64(&ti.root.meta.Load().(*dirMeta).usage.Metrics)
			want := int64(ti.snapshot.manifest.Records) + ti.getFileCount()
			l.metricStoreIndexMu.Unlock()
			if got != want {
				t.Fatalf("quota metrics=%d; index contains %d", got, want)
			}
			c.Stat(func(name string, value float64) {
				if name == "droppedRealtimeIndex" && value != 0 {
					t.Errorf("lost %v notifications", value)
				}
			})
		})
	}
}

func TestRealtimeNotificationBetweenSnapshotTransferAndPublication(t *testing.T) {
	l := savedIndex(t, "existing.metric")
	rewriteCacheVersion(t, l.fileListCache, FLCVersion1)
	l.updateFileList(l.whisperData, nil, nil)
	u := newFileListUpdate(l, nil)
	if !u.scanFiles(l.whisperData, nil) {
		t.Fatal("scan failed")
	}
	u.pruneRealtimeMetrics()
	u.closeFileListCaches()
	u.replaceSnapshot()
	if !l.IndexNewMetric("after.transfer") {
		t.Fatal("notification rejected")
	}
	if !l.MetricExists("after.transfer") {
		t.Fatal("live index missed notification")
	}
	u.publish(l.whisperData, nil)
	if !l.MetricExists("after.transfer") {
		t.Fatal("publication lost notification")
	}
}

func TestRealtimeDiscoveryCacheRefresh(t *testing.T) {
	for _, protocol := range []string{"http-json", "http-v2", "http-v3", "grpc", "render-globs"} {
		t.Run(protocol, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newDiscoveryCacheTestListener(t)
				l.SetConcurrentIndex(true)
				l.SetRealtimeIndex(1)
				ctx, logger := context.Background(), zap.NewNop()
				query := "burst.*"
				fetch := func() int {
					switch protocol {
					case "grpc":
						response, err := l.Find(ctx, &protov2.GlobRequest{Query: query})
						if status.Code(err) == codes.NotFound {
							return 0
						}
						if err != nil {
							t.Fatal(err)
						}
						return len(response.Matches)
					case "render-globs":
						result, _, err := l.getExpandedGlobsWithCache(ctx, logger, "render", []string{query})
						if err != nil || len(result) != 1 {
							t.Fatalf("globs: %v, %v", result, err)
						}
						return len(result[0].Files)
					default:
						format := jsonFormat
						if protocol == "http-v2" {
							format = protoV2Format
						}
						if protocol == "http-v3" {
							format = protoV3Format
						}
						result, _, err := l.findResponse(ctx, logger, time.Now(), format, protocol, []string{query})
						if err != nil && result == nil {
							t.Fatal(err)
						}
						return result.files
					}
				}
				if fetch() != 0 {
					t.Fatal("expected cached empty discovery")
				}
				if !l.IndexNewMetric("burst.first") || fetch() != 1 {
					t.Fatal("first notification did not invalidate empty discovery")
				}
				if !l.IndexNewMetric("burst.second") {
					t.Fatal("notification failed")
				}
				time.Sleep(1100 * time.Millisecond)
				if fetch() != 2 {
					t.Fatal("last arrival stayed hidden in discovery cache")
				}
				// Stable namespaces retain the configured long TTL and cache hits.
				_, hit, err := l.findResponse(ctx, logger, time.Now(), jsonFormat, "json", []string{"aggregations.*"})
				if err != nil || hit {
					t.Fatal("unexpected initial response", hit, err)
				}
				unrelated := "unrelated"
				for discoveryBucket(unrelated) == discoveryBucket("aggregations") || discoveryBucket(unrelated) == discoveryBucket("burst") {
					unrelated += "x"
				}
				l.IndexNewMetric(unrelated + ".name")
				_, hit, err = l.findResponse(ctx, logger, time.Now(), jsonFormat, "json", []string{"aggregations.*"})
				if err != nil || !hit {
					t.Fatal("unrelated cache was invalidated", hit, err)
				}
			})
		})
	}
}

func TestRealtimeInitialCatalogOverflow(t *testing.T) {
	c := cache.New()
	l := NewCarbonserverListener(c.Get)
	l.SetWhisperData(t.TempDir())
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetQuotaUsageReportFrequency(time.Minute)
	l.SetQuotas([]*Quota{{Pattern: "/", Metrics: 2}})
	l.SetEstimateSize(func(string) (int64, int64, int64) { return 1, 2, 3 })
	c.SetNewMetricsQueue(l.SetRealtimeIndex(1))
	u := newFileListUpdate(l, nil)
	u.acceptRealtime = true
	c.Add(points.OnePoint("cold.first", 1, 1))
	c.Add(points.OnePoint("cold.second", 1, 1))
	if l.CurrentFileIndex() != nil {
		t.Fatal("initial catalogue advertised ready before completion")
	}
	if !u.scanFiles(l.whisperData, nil) {
		t.Fatal("scan failed")
	}
	u.pruneRealtimeMetrics()
	u.publish(l.whisperData, nil)
	if !l.MetricExists("cold.first") || !l.MetricExists("cold.second") {
		t.Fatal("first scan lost cold-start names")
	}
	if !l.ShouldThrottleMetric(points.OnePoint("cold.third", 1, 1), false) {
		t.Fatal("first scan did not refresh quota usage")
	}
	c.Stat(func(name string, value float64) {
		if name == "droppedRealtimeIndex" && value != 0 {
			t.Errorf("lost %v notifications", value)
		}
	})
}

// Compare the allocation-free encoder with the original normalization path,
// including atypical names and caller reuse of the encoder's scratch bytes.
func TestRealtimeCanonicalOracle(t *testing.T) {
	names := []string{"ordinary.name", "ns.deep.metric", "another.thing", "a..b", ".leading", "trailing.", "slash/name", "tagged;key=value", "unicode.λ", "x\\y.z", "a.\x00b", "", "a." + strings.Repeat("x", 252), strings.Repeat("n", 260) + ".value"}
	l := NewCarbonserverListener(nil)
	l.SetWhisperData("/tmp/data")
	got, want := newTrie(".wsp", 0, nil), newTrie(".wsp", 0, nil)
	for _, name := range names {
		l.insertRealtimeMetric(got, name)
		if path, ok := l.storableMetricPath(name); ok {
			_, _ = want.insert(path, 0, 0, 0, 0)
		}
	}
	actual, _, _, _, _ := got.allMetricsNodeMutable(got.root, '.', "", 10000, false)
	expected, _, _, _, _ := want.allMetricsNodeMutable(want.root, '.', "", 10000, false)
	sort.Strings(actual)
	sort.Strings(expected)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("actual=%q expected=%q", actual, expected)
	}
}

func TestRealtimeIndexDisabled(t *testing.T) {
	l := NewCarbonserverListener(nil)
	if q := l.SetRealtimeIndex(0); q != nil || l.IndexNewMetric("disabled.metric") {
		t.Fatal("disabled index accepted a name")
	}
}

func TestRealtimeCursorSurvivesSplitsAndPrune(t *testing.T) {
	l := NewCarbonserverListener(nil)
	l.SetWhisperData("/tmp")
	got, want := newTrie(".wsp", 0, nil), newTrie(".wsp", 0, nil)
	for generation := range 3 {
		got.root.gen++
		want.root.gen++
		for i := range 10000 {
			// Permuting shared-prefix names forces radix splits and cursor invalidation.
			n := (i * 7919) % 10000
			if generation > 0 && n%3 == 0 {
				continue
			}
			name := fmt.Sprintf("ns%d.service%d.metric%d.value", n%17, n%53, n)
			l.insertRealtimeMetric(got, name)
			path, _ := l.storableMetricPath(name)
			_, _ = want.insert(path, 0, 0, 0, 0)
		}
		got.prune()
		want.prune()
		actual, _, _, _, _ := got.allMetricsNodeMutable(got.root, '.', "", 20000, false)
		expected, _, _, _, _ := want.allMetricsNodeMutable(want.root, '.', "", 20000, false)
		sort.Strings(actual)
		sort.Strings(expected)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("generation%d cursor diverged", generation)
		}
	}
}
