package carbonserver

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestQuotaTrimGlobReloadAndStats(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprint(mapped), func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(t.TempDir(), store.Options{SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, name := range []string{"apps.a.old", "apps.a.new", "apps.b.old", "apps.b.new", "unrelated.name"} {
				if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
					t.Fatal(err)
				}
			}
			listener := newMetricStoreTestListener(t, cache.New(), db, true)
			defer listener.Stop()
			if mapped {
				listener.SetFileListCache(filepath.Join(t.TempDir(), "files.gz"))
				listener.SetFileListCacheVersion(2)
				listener.SetIncrementalIndexScratch(t.TempDir())
			}
			listener.SetQuotas([]*Quota{{Pattern: "apps.*", Metrics: 1}})
			if err := listener.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			rules, err := listener.QuotaTrimRules()
			if err != nil || len(rules) != 0 {
				t.Fatalf("unconfigured quotas selected: %+v %v", rules, err)
			}
			reload := func(q ...*Quota) []QuotaTrimRule {
				t.Helper()
				if err := listener.ReloadQuotas(q); err != nil {
					t.Fatal(err)
				}
				rules, err := listener.QuotaTrimRules()
				if err != nil {
					t.Fatal(err)
				}
				return rules
			}
			glob := &Quota{Pattern: "apps.*", Metrics: 1, OverQuota: "delete-newest", StatMetricPrefix: "team."}
			rules = reload(glob)
			if len(rules) != 2 || rules[0].Namespace != "apps.a" || rules[1].Namespace != "apps.b" || rules[0].StatMetric != "team.apps-a" {
				t.Fatalf("glob was not materialized per namespace: %+v", rules)
			}
			// Snapshot values survive removal and later edits of the caller's rule.
			snapshot := rules[0]
			rules = reload(glob, &Quota{Pattern: "apps.b", Metrics: 1})
			if len(rules) != 1 || rules[0].Namespace != "apps.a" {
				t.Fatalf("last rule did not override: %+v", rules)
			}
			if len(reload(&Quota{Pattern: "apps.*", Metrics: 1})) != 0 {
				t.Fatal("removing option kept trimming")
			}
			if snapshot.Quota.OverQuota != "delete-newest" {
				t.Fatal("reload mutated snapshot")
			}
			root := reload(&Quota{Pattern: "/", Metrics: 1, OverQuota: "delete-newest", StatMetricPrefix: "total."})
			if len(root) != 1 || root[0].StatMetric != "total.root" {
				t.Fatalf("root stat path: %+v", root)
			}
			long := quotaMetricName("prefix.", strings.Repeat("x", 300))
			if len(long) != len("prefix.")+250 {
				t.Fatalf("stat path shortening changed: %d", len(long))
			}
			listener.SetMetricStoreFileIndex(true)
			if _, err := listener.QuotaTrimRules(); err == nil || !strings.Contains(err.Error(), "catalog-owned discovery") {
				t.Fatalf("file migration accepted for trimming: %v", err)
			}
		})
	}
}
