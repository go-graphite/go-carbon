package buckyd

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
)

func TestProofMaintenanceAuthenticatedAndFenced(t *testing.T) {
	db := anchorStore(t)
	s, err := New(Config{TmpDir: t.TempDir()}, db)
	if err != nil {
		t.Fatal(err)
	}
	s.secret = []byte("proof-release-test-secret")
	call := func(method, action, body string, ns, ops []string) *httptest.ResponseRecorder {
		t.Helper()
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(time.Hour).Unix(), "namespaces": ns, "ops": ops}).SignedString(s.secret)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(method, "/v1/maintenance/mutation-proofs?action="+action, bytes.NewBufferString(body))
		req.Header.Set(authHeader, token)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		return w
	}
	for _, tt := range []struct {
		method, action, body string
		ns, ops              []string
		want                 int
	}{
		{"POST", "reset", "{}", []string{"metric.*"}, []string{"update"}, 403},
		{"POST", "reset", "{}", []string{"*"}, []string{"read"}, 403},
		{"GET", "reset", "{}", []string{"*"}, []string{"update"}, 405},
		{"POST", "unknown", "{}", []string{"*"}, []string{"update"}, 400},
		{"POST", "release", `{"Names":[]}`, []string{"*"}, []string{"update"}, 400},
		{"POST", "reset", `{} {}`, []string{"*"}, []string{"update"}, 400},
		{"POST", "release", `{"Names":[{"Name":"x","From":9,"Until":8}]}`, []string{"*"}, []string{"update"}, 400},
		{"POST", "reset", `{"Unknown":true}`, []string{"*"}, []string{"update"}, 400},
	} {
		if w := call(tt.method, tt.action, tt.body, tt.ns, tt.ops); w.Code != tt.want {
			t.Fatalf("%+v: %d %s", tt, w.Code, w.Body.String())
		}
	}
	if _, err := db.Create(t.Context(), chunkstore.MetricConfig{Name: "proof", Retentions: []chunkstore.Retention{{Step: 1, Count: 60}}, AggregationMethod: chunkstore.Average}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.MutationProof(t.Context(), "proof", 1, 100); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		w := call("POST", "release", `{"Names":[{"Name":"proof"}]}`, []string{"*"}, []string{"update"})
		var results []chunkstore.ProofReleaseResult
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &results) != nil || len(results) != 1 || results[0].Status == "error" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := call("POST", "reset", "{}", []string{"*"}, []string{"update"})
	var receipt chunkstore.ProofResetReceipt
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &receipt) != nil || receipt.Finished.IsZero() {
		t.Fatal(w.Code, w.Body.String())
	}
	// A valid page of long names must fit the documented 1000-entry bound.
	names := make([]chunkstore.ProofRelease, 300)
	for i := range names {
		names[i].Name = strings.Repeat("x", chunkstore.MaxMetricNameBytes)
	}
	body, err := json.Marshal(struct{ Names []chunkstore.ProofRelease }{names})
	if err != nil {
		t.Fatal(err)
	}
	if w := call("POST", "release", string(body), []string{"*"}, []string{"update"}); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	s.secret = nil
	if w := call("POST", "reset", "{}", []string{"*"}, []string{"update"}); w.Code != 403 {
		t.Fatal("unauthenticated reset", w.Code)
	}
}
