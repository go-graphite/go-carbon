package carbonserver

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2index"
)

func v2Found(names []string, leaf []bool) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = fmt.Sprintf("%s:%v", n, leaf[i])
	}
	slices.Sort(out)
	return out
}

func TestV2FindDifferential(t *testing.T) {
	// Real-shaped service/namespace/host series; no private deployment data.
	var names []string
	for service := range 16 {
		for host := range 32 {
			for _, suffix := range []string{"cpu.user", "cpu.system", "requests.total", "latency.p99"} {
				names = append(names, fmt.Sprintf("services.svc%02d.nodes.host%03d.%s", service, host, suffix))
			}
		}
	}
	for _, n := range []string{"services.svc00", "services.svc00.nodes", "standalone", "metric;a=1;z=2", "unicode.日本.cpu", "dash-name.under_score.cpu", "collision", "collision.child"} {
		names = append(names, n)
	}
	paths := make(map[string]bool)
	for _, name := range names {
		for _, p := range metricStoreTrigramPaths(metricStoreVirtualPath(name)) {
			paths[p] = true
		}
	}
	var files []string
	for p := range paths {
		files = append(files, p)
	}
	slices.Sort(files)
	queries := []string{"", " ** ", "unicode.??.cpu", "unicode.??????.cpu", "services.svc[", "[", "*", "services.*", "services.svc00", "services.svc00.nodes.*", "services.svc00.nodes.host000.cpu.{user,system}", "services.{svc00,svc01}.nodes.host00?.*.?ser", "services.svc[0-1][0-9].nodes.*.requests.*", "{services.svc00.nodes.host000.cpu,unicode.日本}.cpu", "collision", "collision.*", "standalone", "missing.*", "metric;a=1;z=2", "dash-name.*.*", "services.{svc00,svc01}.nodes.{host000,host001}.latency.p99"}
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
		catalog := &v2index.Catalog{}
		if err := catalog.Publish(1, names); err != nil {
			t.Fatal(err)
		}
		idx := &CarbonserverListener{trieIndex: trie, maxGlobs: math.MaxInt, maxMetricsGlobbed: math.MaxInt, failOnMaxGlobs: true}
		err := idx.SetV2Catalog(catalog)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queries {
			found := idx.expandV2Catalog(idx.v2Catalog.Load(), q)
			got, gl, ge := found.Files, found.Leafs, found.Err
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
			if (ge == nil) != (we == nil) || !reflect.DeepEqual(v2Found(got, gl), v2Found(want, wl)) {
				t.Fatalf("trie=%v query=%s got=%v/%v want=%v/%v", trie, q, v2Found(got, gl), ge, v2Found(want, wl), we)
			}
		}
	}
	t.Logf("%d names, %d queries/backend, %d differential queries", len(names), len(queries), 2*len(queries))
}

// Prove real handler expansion observes arrivals, expiry, recreation, and the
// removal of the last descendant, even with both discovery caches enabled.
func TestV2LiveCatalogAndCaches(t *testing.T) {
	for _, trie := range []bool{false, true} {
		t.Run(fmt.Sprint(trie), func(t *testing.T) {
			l := NewCarbonserverListener(nil)
			l.trieIndex = trie
			l.trigramIndex = !trie
			l.maxGlobs = math.MaxInt
			l.maxMetricsGlobbed = math.MaxInt
			l.findCacheEnabled = true
			l.globCacheEnabled = true
			c := &v2index.Catalog{}
			if err := c.Publish(1, []string{"old.only.cpu", "collision", "collision.child"}); err != nil {
				t.Fatal(err)
			}
			if err := l.SetV2Catalog(c); err != nil {
				t.Fatal(err)
			}
			check := func(query string, want []string) {
				t.Helper()
				got, hit, err := l.getExpandedGlobsWithCache(context.Background(), l.logger, "find", []string{query})
				if err != nil || hit {
					t.Fatalf("%v hit=%v", err, hit)
				}
				if !slices.Equal(v2Found(got[0].Files, got[0].Leafs), want) {
					t.Fatalf("query=%s got=%v want=%v", query, v2Found(got[0].Files, got[0].Leafs), want)
				}
			}
			check("old.*", []string{"old.only:false"})
			pinned := l.v2Catalog.Load().image.Load()
			if err := c.Apply(2, []string{"new.only.cpu"}, []string{"old.only.cpu", "collision.child"}); err != nil {
				t.Fatal(err)
			}
			check("old.*", nil)
			check("new.*", []string{"new.only:false"})
			check("collision", []string{"collision:true"})
			if pinned.snapshot.Len() != 3 {
				t.Fatal("old reader changed")
			}
			if err := c.Apply(3, []string{"old.only.cpu"}, nil); err != nil {
				t.Fatal(err)
			}
			check("old.*", []string{"old.only:false"})
			if err := c.Publish(4, nil); err != nil {
				t.Fatal(err)
			}
			check("*", nil)
			if l.shouldBlockForIndex() {
				t.Fatal("attached catalog is ready")
			}
		})
	}
}

func TestV2CatalogConcurrentRebuild(t *testing.T) {
	for _, trie := range []bool{false, true} {
		c := &v2index.Catalog{}
		l := NewCarbonserverListener(nil)
		l.trieIndex = trie
		l.maxGlobs = math.MaxInt
		l.maxMetricsGlobbed = math.MaxInt
		if err := l.SetV2Catalog(c); err != nil {
			t.Fatal(err)
		}
		binding := l.v2Catalog.Load()
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 50 {
					r := l.expandV2Catalog(binding, "service.*")
					if r.Err != nil {
						t.Error(r.Err)
					}
				}
			}()
		}
		for i := uint64(1); i <= 25; i++ {
			if err := c.Publish(i, []string{fmt.Sprintf("service.%d", i)}); err != nil {
				t.Fatal(err)
			}
		}
		wg.Wait()
		r := l.expandV2Catalog(binding, "service.*")
		if !slices.Equal(r.Files, []string{"service.25"}) {
			t.Fatal(r)
		}
	}
}
