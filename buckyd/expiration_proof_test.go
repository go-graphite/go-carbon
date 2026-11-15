package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
)

func TestExpirationEvidenceReadAuthorizationPolicyAndPending(t *testing.T) {
	now := time.Unix(1790000000, 0)
	db, err := chunkstore.Open(filepath.Join(t.TempDir(), "store"), chunkstore.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m, err := db.Create(context.Background(), chunkstore.MetricConfig{Name: "old", Retentions: []chunkstore.Retention{{Step: 60, Count: 1440}}, AggregationMethod: chunkstore.Average})
	if err != nil {
		t.Fatal(err)
	}
	receipt := chunkstore.ExpirationReceipt{Policy: "policy-v1", DeletedAt: now.Unix(), LastUpdate: now.Unix() - 10*86400}
	result, err := db.DeleteBatchWithExpirationReceipts(context.Background(), []chunkstore.Metadata{m}, map[string]chunkstore.ExpirationReceipt{"old": receipt})
	if err != nil || result[0] != nil {
		t.Fatal(result, err)
	}
	// Coarse-only/future data must be included even when the fine archive is empty.
	future := now.Unix()/3600*3600 + 86400
	_, err = db.CreateFromSnapshot(context.Background(), chunkstore.Snapshot{
		Metadata: chunkstore.Metadata{MetricConfig: chunkstore.MetricConfig{Name: "coarse", Retentions: []chunkstore.Retention{{Step: 60, Count: 1440}, {Step: 3600, Count: 240}}, AggregationMethod: chunkstore.Average}},
		Archives: []chunkstore.Archive{{Retention: chunkstore.Retention{Step: 60, Count: 1440}}, {Retention: chunkstore.Retention{Step: 3600, Count: 240}, Points: []chunkstore.Point{{Timestamp: future, Value: 42}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{TmpDir: t.TempDir()}, db)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Stop()
	service.secret = []byte("proof-test")
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{Namespaces: []string{"*"}, Ops: []string{"read"}}).SignedString(service.secret)
	if err != nil {
		t.Fatal(err)
	}
	policy, pending := "policy-v1", false
	service.SetExpirationPolicy(func(string) (string, time.Duration, bool) { return policy, 96 * time.Hour, pending })
	invoke := func(credential string) (int, map[string]ExpirationEvidence) {
		request := httptest.NewRequest(http.MethodPost, "/v1/expiration/evidence", bytes.NewBufferString(`{"Names":["old","absent","coarse"]}`))
		request.Header.Set(authHeader, credential)
		response := httptest.NewRecorder()
		service.Handler().ServeHTTP(response, request)
		var out map[string]ExpirationEvidence
		if response.Code == 200 {
			if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
		}
		return response.Code, out
	}
	if status, _ := invoke(""); status != 403 {
		t.Fatal("unauthorized", status)
	}
	if status, out := invoke(token); status != 200 || out["old"].Receipt == nil || out["absent"].Receipt != nil || out["coarse"].Latest != future {
		t.Fatal(status, out)
	}
	policy = "policy-v2"
	if status, out := invoke(token); status != 200 || out["old"].Receipt != nil {
		t.Fatal("old policy accepted", status, out)
	}
	policy = "policy-v1"
	pending = true
	if status, out := invoke(token); status != 200 || !out["old"].Pending || out["old"].Receipt != nil {
		t.Fatal("pending accepted", status, out)
	}
}
