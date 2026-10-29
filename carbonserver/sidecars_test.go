package carbonserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestSidecarsAdmin(t *testing.T) {
	listener := NewCarbonserverListener(nil)
	call := func(method, path string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		listener.sidecarsHandler(w, httptest.NewRequest(method, path, http.NoBody))
		return w
	}
	if r := call(http.MethodGet, "/"); r.Code != 503 {
		t.Fatal(r.Code)
	}
	now := time.Unix(1792432631, 0)
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, Now: func() time.Time { return now }, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	listener.SetMetricStore(db)
	ctx := context.Background()
	if _, err := db.Create(ctx, store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	var seed []store.Point
	for age := int64(20); age > 0; age-- {
		seed = append(seed, store.Point{Timestamp: now.Unix() - age, Value: float64(age) + .1})
	}
	for _, batch := range [][]store.Point{seed, {{Timestamp: now.Unix() - 10, Value: 42.5}}} {
		if err := db.UpdateMany(ctx, "metric", batch); err != nil {
			t.Fatal(err)
		}
	}
	r := call(http.MethodGet, "/")
	var inventory store.SidecarInventory
	if err := json.Unmarshal(r.Body.Bytes(), &inventory); err != nil || inventory.Count != 1 {
		t.Fatalf("GET: %s %v", r.Body.String(), err)
	}
	for _, path := range []string{"?action=scan&limit=x", "?action=scan&dry-run=nope", "?action=fold&rate=x", "?action=fold&rate=NaN", "?action=fold&limit=0", "?action=unknown"} {
		if r := call(http.MethodPost, "/"+path); r.Code < 400 {
			t.Fatalf("accepted %s", path)
		}
	}
	if r := call(http.MethodDelete, "/"); r.Code != 405 {
		t.Fatal(r.Code)
	}
	if r := call(http.MethodPost, "/?action=scan&dry-run=true"); r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	if r := call(http.MethodPost, "/?action=fold&dry-run=true"); r.Code != 200 || db.Sidecars().Count != 1 {
		t.Fatal("dry run changed sidecars", r.Body.String())
	}
	if r := call(http.MethodPost, "/?action=fold"); r.Code != 200 || db.Sidecars().Count != 0 {
		t.Fatal("fold failed", r.Body.String())
	}
	listener.sharedRequestMu.Lock()
	listener.sharedStoreStopped = true
	listener.sharedRequestMu.Unlock()
	if r := call(http.MethodPost, "/?action=fold"); r.Code != 503 {
		t.Fatal("accepted admin request during shutdown")
	}

}
