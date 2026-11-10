package carbonserver

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

// A realtime name can precede both the source walk and native publication.
// Its points then leave cache after the catalog view was read but before prune.
// Neither cache emptiness nor absence from that older view proves deletion.
func TestIncrementalScanKeepsNameCommittedAfterCatalogRead(t *testing.T) {
	for _, cacheMode := range []string{"without-cache", "with-cache"} {
		t.Run(cacheMode, func(t *testing.T) {
			cachePath, root, _ := snapshotFixture(t)
			l := overlayListener(t, cachePath, root)
			l.SetQuotas(nil)
			l.SetMaxGlobs(100)
			l.SetMaxMetricsRendered(100)
			l.SetFileListCacheVersion(int(FLCVersion2))
			if cacheMode == "with-cache" {
				l.cacheGet = func(name string) []points.Point {
					if name == "pending.cache" {
						return []points.Point{{Timestamp: 1, Value: 1}}
					}
					return nil
				}
				l.insertRealtimeMetric(l.CurrentFileIndex().trieIdx, "pending.cache")
			}
			destination := t.TempDir()
			db, err := store.Open(destination, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: root, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			l.SetMetricStore(db)
			l.SetMetricStoreFileIndex(true)
			l.SetIncrementalIndexScratch(filepath.Join(destination, "index"))
			ctx := context.Background()
			if _, err := db.Create(ctx, store.MetricConfig{Name: "gone.native", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
				t.Fatal(err)
			}
			l.insertRealtimeMetric(l.CurrentFileIndex().trieIdx, "late.native")
			l.insertRealtimeMetric(l.CurrentFileIndex().trieIdx, "gone.native")
			if err := db.Delete(ctx, "gone.native"); err != nil {
				t.Fatal(err)
			}
			u := newFileListUpdate(l, nil)
			defer u.closeFileListCaches()
			if !u.loadFileListCache(false) || !u.scanFiles(root, nil) {
				t.Fatal("source/catalog scan failed")
			}
			if _, err := db.Create(ctx, store.MetricConfig{Name: "late.native", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
				t.Fatal(err)
			}
			stamp := time.Now().Unix() - 10
			if err := db.UpdateMany(ctx, "late.native", []store.Point{{Timestamp: stamp, Value: .125}}); err != nil {
				t.Fatal(err)
			}
			u.pruneRealtimeMetrics()
			u.closeFileListCaches()
			if u.snapshotReady {
				u.replaceSnapshot()
			}
			u.publish(root, nil)
			if !l.MetricExists("late.native") {
				t.Fatal("scan pruned a native name committed after its catalog view")
			}
			if cacheMode == "with-cache" && !l.MetricExists("pending.cache") {
				t.Fatal("scan pruned an unpersisted cache name")
			}
			if l.MetricExists("gone.native") {
				t.Fatal("scan retained a name absent from cache, source and committed catalog")
			}
			// The no-cache subcase exercises discovery without a cache hook.
			// Configure the ordinary empty cache reader for the HTTP oracle.
			if l.cacheGet == nil {
				l.cacheGet = func(string) []points.Point { return nil }
			}
			status, body := incrementalWireReply(t, l, incrementalWireCase{"late.native", "/render/", "json", stamp - 1, stamp + 1})
			if status != http.StatusOK {
				t.Fatalf("committed native name is unreadable: %d %s", status, body)
			}
			var series struct {
				Metrics []struct {
					StartTime, StepTime int64
					Values              []float64
					IsAbsent            []bool
				}
			}
			if err := json.Unmarshal(body, &series); err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, metric := range series.Metrics {
				if len(metric.Values) != len(metric.IsAbsent) {
					t.Fatalf("render values/absence flags differ: %s", body)
				}
				for i, value := range metric.Values {
					if !metric.IsAbsent[i] {
						count++
						if metric.StartTime+int64(i)*metric.StepTime != stamp || math.Float64bits(value) != math.Float64bits(.125) {
							t.Fatalf("render changed the committed timestamp/value: %s", body)
						}
					}
				}
			}
			if count != 1 {
				t.Fatalf("render omitted or duplicated the committed point: %s", body)
			}
		})
	}
}
