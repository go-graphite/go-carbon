package buckyd

import (
	"context"
	"encoding/json"
	"github.com/golang-jwt/jwt/v5"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestSidecarsAdmin(t *testing.T) {
	var service *Service
	call := func(method, path string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		service.sidecars(w, httptest.NewRequest(method, path, http.NoBody))
		return w
	}
	now := time.Unix(1792432631, 0)
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, Now: func() time.Time { return now }, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, err = New(Config{TmpDir: t.TempDir()}, db)
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestSidecarsAuthAndAdmission(t *testing.T) {
	db := anchorStore(t)
	s, err := New(Config{TmpDir: t.TempDir()}, db)
	if err != nil {
		t.Fatal(err)
	}
	s.secret = []byte("sidecar-test-secret")
	token := func(namespaces, ops []string) string {
		t.Helper()
		v, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(time.Hour).Unix(), "namespaces": namespaces, "ops": ops}).SignedString(s.secret)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, tt := range []struct {
		method, path string
		ns, ops      []string
		want         int
	}{
		{"GET", "", []string{"*"}, []string{"read"}, 200},
		{"GET", "", []string{"**"}, []string{"read"}, 403},
		{"POST", "?action=scan&dry-run=true", []string{"*"}, []string{"read"}, 200},
		{"POST", "?action=scan", []string{"*"}, []string{"read"}, 403},
		{"POST", "?action=fold", []string{"*"}, []string{"read"}, 403},
		{"POST", "?action=scan", []string{"*"}, []string{"update"}, 200},
		{"POST", "?action=scan&limit=10001", []string{"*"}, []string{"update"}, 400},
		{"DELETE", "", []string{"*"}, []string{"update"}, 405},
	} {
		req := httptest.NewRequest(tt.method, "/v1/maintenance/sidecars"+tt.path, http.NoBody)
		req.Header.Set("X-Buckyd-Authorization", token(tt.ns, tt.ops))
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != tt.want {
			t.Fatalf("%+v: %d %s", tt, w.Code, w.Body)
		}
	}
	for i := 0; i < cap(s.maintenance); i++ {
		s.maintenance <- struct{}{}
	}
	req := httptest.NewRequest("POST", "/v1/maintenance/sidecars?action=scan&dry-run=true", http.NoBody)
	req.Header.Set("X-Buckyd-Authorization", token([]string{"*"}, []string{"read"}))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 429 {
		t.Fatal(w.Code, w.Body)
	}
}
