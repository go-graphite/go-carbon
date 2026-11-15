package carbonserver

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

func partsMetadata(name string) store.Metadata {
	return store.Metadata{MetricConfig: store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}, {Step: 60, Count: 120}}}}
}
func partsFound(names []string, leaf []bool) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = fmt.Sprintf("%s:%v", n, leaf[i])
	}
	slices.Sort(out)
	return out
}

func TestPartsFindDifferential(t *testing.T) {
	// Real-shaped service/namespace/host series; no private deployment data.
	var metadata []store.Metadata
	for service := range 16 {
		for host := range 32 {
			for _, suffix := range []string{"cpu.user", "cpu.system", "requests.total", "latency.p99"} {
				metadata = append(metadata, partsMetadata(fmt.Sprintf("services.svc%02d.nodes.host%03d.%s", service, host, suffix)))
			}
		}
	}
	for _, n := range []string{"services.svc00", "services.svc00.nodes", "standalone", "metric;a=1;z=2", "unicode.日本.cpu", "dash-name.under_score.cpu", "collision", "collision.child"} {
		metadata = append(metadata, partsMetadata(n))
	}
	paths := make(map[string]bool)
	for _, m := range metadata {
		for _, p := range metricStoreTrigramPaths(metricStoreVirtualPath(m.Name)) {
			paths[p] = true
		}
	}
	var files []string
	for p := range paths {
		files = append(files, p)
	}
	slices.Sort(files)
	queries := []string{"*", "services.*", "services.svc00", "services.svc00.nodes.*", "services.svc00.nodes.host000.cpu.{user,system}", "services.{svc00,svc01}.nodes.host00?.*.?ser", "services.svc[0-1][0-9].nodes.*.requests.*", "{services.svc00.nodes.host000.cpu,unicode.日本}.cpu", "collision", "collision.*", "standalone", "missing.*", "metric;a=1;z=2", "dash-name.*.*", "services.{svc00,svc01}.nodes.{host000,host001}.latency.p99"}
	for service := range 16 {
		for host := range 32 {
			queries = append(queries, fmt.Sprintf("services.svc%02d.nodes.host%03d.*.*", service, host))
		}
	}
	for _, trie := range []bool{false, true} {
		var old *CarbonserverListener
		if trie {
			old = newTrieServer(files, t)
		} else {
			old = newTrigramServer(files, t)
		}
		idx, err := NewPartsCatalogIndex(metadata, PartsIndexOptions{Trie: trie, ReportFrequency: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queries {
			got, gl, ge := idx.Find(q)
			var want []string
			var wl []bool
			var we error
			if trie {
				want, wl, _, _, we = old.expandGlobsTrie(q)
			} else {
				var globs []string
				globs, we = old.globPatterns(q, old.logger)
				if we == nil {
					want = old.matchGlobIndex(old.CurrentFileIndex(), globs, false)
					wl = make([]bool, len(want))
					for i, p := range want {
						want[i], wl[i] = old.normalizeGlobFile(p, nil)
					}
				}
			}
			if (ge == nil) != (we == nil) || !reflect.DeepEqual(partsFound(got, gl), partsFound(want, wl)) {
				t.Fatalf("trie=%v query=%s got=%v/%v want=%v/%v", trie, q, partsFound(got, gl), ge, partsFound(want, wl), we)
			}
		}
	}
	t.Logf("%d names, %d queries/backend, %d differential queries", len(metadata), len(queries), 2*len(queries))
}

func TestPartsQuotaDecisionsDifferential(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metadata := []store.Metadata{partsMetadata("service.a.cpu"), partsMetadata("service.b.cpu"), partsMetadata("other.a.cpu")}
		estimate := func(string) (int64, int64, int64) { l, d := metricStoreLogicalSize(metadata[0]); return l, 0, d }
		logical, _, dp := estimate("")
		scenarios := [][]*Quota{
			{{Pattern: "/", Metrics: 3}}, {{Pattern: "service", Metrics: 2}}, {{Pattern: "/", Namespaces: 2}},
			{{Pattern: "service", DataPoints: 2 * dp}}, {{Pattern: "service", LogicalSize: 2 * logical}},
			{{Pattern: "service", Metrics: 1, DroppingPolicy: QDPNone}}, {{Pattern: "service", Throughput: 20}},
			{{Pattern: "*", Throughput: 100}, {Pattern: "service.*", Throughput: 10}},
			{{Pattern: "/", Metrics: 3, Throughput: 100}, {Pattern: "service", DataPoints: dp, DroppingPolicy: QDPNone}},
			{{Pattern: "service", Throughput: math.MaxInt64}}, nil,
		}
		decisions := 0
		for si, rules := range scenarios {
			old := newTrie(".wsp", 0, estimate)
			for _, m := range metadata {
				l, d := metricStoreLogicalSize(m)
				if _, err := old.insert(metricStoreVirtualPath(m.Name), l, 0, d, 0); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := old.applyQuotas(time.Minute, rules...); err != nil {
				t.Fatal(err)
			}
			old.refreshUsage(old.throughputs)
			idx, err := NewPartsCatalogIndex(metadata, PartsIndexOptions{Trie: true, Quotas: rules, ReportFrequency: time.Minute, EstimateSize: estimate})
			if err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewSource(100 + int64(si)))
			for event := range 1000 {
				name := []string{"service.a.cpu", "service.b.cpu", "other.a.cpu", "service.a.new", "service.c.new", "new.namespace.metric"}[rng.Intn(6)]
				ps := &points.Points{Metric: name, Data: make([]points.Point, rng.Intn(8))}
				cached := rng.Intn(2) == 0
				want, got := old.throttle(ps, cached), idx.Throttle(ps, cached)
				if want != got {
					t.Fatalf("scenario=%d event=%d name=%s cached=%v got=%v want=%v", si, event, name, cached, got, want)
				}
				decisions++
				if event == 300 {
					old.refreshUsage(old.throughputs)
					idx.RefreshUsage()
				}
				if event == 600 {
					next := []*Quota{{Pattern: "service", Metrics: 1, Throughput: 11}}
					if _, err := old.applyQuotas(time.Minute, next...); err != nil {
						t.Fatal(err)
					}
					if err := idx.ReloadQuotas(next); err != nil {
						t.Fatal(err)
					}
				}
				if event == 800 {
					if _, err := old.applyQuotas(time.Minute); err != nil {
						t.Fatal(err)
					}
					if err := idx.ReloadQuotas(nil); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		t.Logf("%d quota decisions identical", decisions)
	})
}

func TestPartsIndexNextKeepsThroughputAndReleasesDeletedUsage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metadata := []store.Metadata{partsMetadata("service.a.cpu"), partsMetadata("service.b.cpu")}
		opts := PartsIndexOptions{Trie: true, Quotas: []*Quota{{Pattern: "service", Metrics: 2, Throughput: 2}}, ReportFrequency: time.Minute, EstimateSize: func(string) (int64, int64, int64) { return 1, 0, 1 }}
		idx, err := NewPartsCatalogIndex(metadata, opts)
		if err != nil {
			t.Fatal(err)
		}
		ps := points.OnePoint("service.a.cpu", 1, 1)
		for range 2 {
			if idx.Throttle(ps, true) {
				t.Fatal("initial throughput rejected")
			}
		}
		next, err := idx.Next(metadata[:1])
		if err != nil {
			t.Fatal(err)
		}
		if !next.Throttle(ps, true) {
			t.Fatal("catalog publication reset consumed throughput")
		}
		if err := next.ReloadQuotas([]*Quota{{Pattern: "service", Metrics: 2}}); err != nil {
			t.Fatal(err)
		}
		if next.Throttle(points.OnePoint("service.c.cpu", 1, 1), false) {
			t.Fatal("expired metric still consumes metric quota")
		}
		oldNames, _, _ := idx.Find("service.*.cpu")
		newNames, _, _ := next.Find("service.*.cpu")
		if len(oldNames) != 2 || len(newNames) != 1 {
			t.Fatal("find snapshots aliased", oldNames, newNames)
		}
	})
}

func TestPartsQuotaConfigTranslation(t *testing.T) {
	rules := persister.WhisperQuotas{{Pattern: "service.*", Namespaces: 2, Metrics: 3, DataPoints: 4, LogicalSize: 5, PhysicalSize: 6, Throughput: 7, DroppingPolicy: "none", StatMetricPrefix: "level1."}}
	if _, err := PartsQuotaRules(rules, time.Minute, false); err == nil {
		t.Fatal("shared physical quota accepted")
	}
	got, err := PartsQuotaRules(rules, 5*time.Minute, true)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].PhysicalSize != 0 || got[0].Throughput != 35 || got[0].DroppingPolicy != QDPNone || strings.TrimSpace(got[0].StatMetricPrefix) != "level1." {
		t.Fatal(got)
	}
}
