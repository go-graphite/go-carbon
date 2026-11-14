package carbonserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestLongNamesReadFindAndGlob(t *testing.T) {
	const now = 1700000000
	for _, trie := range []bool{false, true} {
		t.Run(map[bool]string{false: "trigram", true: "trie"}[trie], func(t *testing.T) {
			db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(now, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var names []string
			for _, size := range []int{300, 4000} {
				name := "long." + strings.Repeat("x", size) + ".value"
				names = append(names, name)
				if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 1, Count: 120}}}); err != nil {
					t.Fatal(err)
				}
				if err := db.UpdateMany(context.Background(), name, []store.Point{{Timestamp: now - 2, Value: 42}}); err != nil {
					t.Fatal(err)
				}
			}
			listener := newMetricStoreTestListener(t, cache.New(), db, trie)
			listener.SetMaxMetricsGlobbed(100)
			for _, name := range names {
				if _, ok := listener.storableMetricPath(name); !ok || !listener.pendingMetricStorable(name) {
					t.Fatal("store catalog applied filesystem limits")
				}
				assertStoreGlob(t, listener, name, true)
				fine, err := listener.fetchSingleMetric(context.Background(), name, "", now-10, now)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, value := range fine.Values {
					found = found || value == 42
				}
				if !found {
					t.Fatal("long name did not serve persisted point")
				}
			}
			assertStoreGlob(t, listener, "long.*.value", true)
			request := httptest.NewRequest(http.MethodGet, "/metrics/find/?query="+url.QueryEscape("long.*.value")+"&format=json", http.NoBody)
			response := httptest.NewRecorder()
			listener.findHandler(response, request)
			for _, name := range names {
				if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), name) {
					t.Fatalf("long name missing from find: status=%d length=%d", response.Code, len(name))
				}
			}
			assertMetricStoreCatalogDoesNotCreateFiles(t, listener)
		})
	}
}
