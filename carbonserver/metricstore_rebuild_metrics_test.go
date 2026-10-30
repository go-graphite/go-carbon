package carbonserver

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func TestMetricStoreRebuildMetricsExcludeIncrementalUpdates(t *testing.T) {
	for _, mode := range []string{"trie", "mapped", "trigram"} {
		t.Run(mode, func(t *testing.T) {
			db, err := store.Open(t.TempDir(), store.Options{SyncInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			l := NewCarbonserverListener(nil)
			l.logger = zap.NewNop()
			l.SetWhisperData(t.TempDir())
			l.SetTrieIndex(mode != "trigram")
			l.SetTrigramIndex(mode == "trigram")
			l.SetMetricStore(db)
			if mode == "mapped" {
				l.SetFileListCache(filepath.Join(t.TempDir(), "files.gz"))
				l.SetFileListCacheVersion(2)
				l.SetIncrementalIndexScratch(t.TempDir())
			}
			defer l.Stop()
			registry := prom.NewRegistry()
			l.InitPrometheus(registry)
			check := func(want uint64) {
				t.Helper()
				families, err := registry.Gather()
				if err != nil {
					t.Fatal(err)
				}
				found := 0
				for _, family := range families {
					switch family.GetName() {
					case "metric_store_index_rebuilds_total":
						found++
						if got := family.Metric[0].GetCounter().GetValue(); got != float64(want) {
							t.Fatalf("rebuild attempts = %v, want %d", got, want)
						}
					case "metric_store_index_rebuild_duration_seconds":
						found++
						h := family.Metric[0].GetHistogram()
						if h.GetSampleCount() != want || want > 0 && h.GetSampleSum() <= 0 {
							t.Fatalf("rebuild durations = %v", h)
						}
					}
				}
				if found != 2 {
					t.Fatal("missing rebuild metrics")
				}
			}
			refresh := func() {
				t.Helper()
				if err := l.RefreshMetricStoreIndex(); err != nil {
					t.Fatal(err)
				}
			}
			check(0)
			refresh() // Initial full rebuild, including an empty catalog.
			check(1)
			refresh() // No changes: not a rebuild.
			check(1)
			ctx := context.Background()
			m, err := db.Create(ctx, store.MetricConfig{Name: "jobs.metric", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average})
			if err != nil {
				t.Fatal(err)
			}
			refresh() // Tries can append; trigram must rebuild.
			want := uint64(1)
			if mode == "trigram" {
				want++
			}
			check(want)
			if outcomes, err := db.DeleteBatchIfUnchanged(ctx, []store.Metadata{m}); err != nil || outcomes[0] != nil {
				t.Fatalf("delete = %v, %v", outcomes, err)
			}
			refresh() // Deletion requires a full rebuild in every representation.
			check(want + 1)
		})
	}
}
