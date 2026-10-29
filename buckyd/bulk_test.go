package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestBulkPullVerifyDeleteAndAuthorization(t *testing.T) {
	source, db := testService(t)
	destination, target := testServiceWithSyncInterval(t, time.Hour)
	config := chunkstore.MetricConfig{Name: "bulk.metric", Retentions: []chunkstore.Retention{{Step: 10, Count: 12}}, AggregationMethod: chunkstore.Sum}
	if _, err := db.Create(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateManyForArchive(context.Background(), config.Name, []chunkstore.Point{{Timestamp: 9990, Value: 7}}, 120); err != nil {
		t.Fatal(err)
	}
	secret := []byte("bulk-secret")
	source.secret = secret
	destination.secret = secret
	remote := httptest.NewServer(source.Handler())
	defer remote.Close()
	invoke := func(path string, body any, jwt string) *httptest.ResponseRecorder {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
		req.Header.Set(authHeader, jwt)
		response := httptest.NewRecorder()
		destination.Handler().ServeHTTP(response, req)
		return response
	}
	root := token(t, secret, []string{"*"}, []string{"*"})
	request := struct {
		Source string
		Names  []string
	}{remote.URL, []string{config.Name}}
	for _, signed := range []string{token(t, secret, []string{"bulk.*"}, []string{"*"}), token(t, secret, []string{"*"}, []string{"update"})} {
		if response := invoke("/v1/transfer/pull", request, signed); response.Code != 403 {
			t.Fatal("scoped/update-only token accepted", response.Code)
		}
	}
	if response := invoke("/v1/transfer/pull", request, root); response.Code != 403 {
		t.Fatal("pull from an unlisted source accepted", response.Code)
	}
	destination.config.TransferSources = []string{"127.0.0.1"}
	response := invoke("/v1/transfer/pull", request, root)
	if response.Code != 200 {
		t.Fatalf("pull %d %s", response.Code, response.Body.String())
	}
	var proofs []chunkstore.TransferProof
	if err := json.Unmarshal(response.Body.Bytes(), &proofs); err != nil {
		t.Fatal(err)
	}
	if response := invoke("/v1/transfer/verify", proofs, root); response.Code != 200 {
		t.Fatal("verify", response.Body.String())
	}
	if _, err := target.Snapshot(context.Background(), config.Name); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal([]chunkstore.TransferVersion{proofs[0].Source})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/transfer/delete", bytes.NewReader(data))
	req.Header.Set(authHeader, root)
	result := httptest.NewRecorder()
	source.Handler().ServeHTTP(result, req)
	if result.Code != 200 {
		t.Fatal("delete", result.Body.String())
	}
}
func TestBulkLimitsAndStatusAdmission(t *testing.T) {
	s, _ := testService(t)
	if !s.acquireTransfer() {
		t.Fatal("acquire")
	}
	defer s.releaseTransfer()
	for s.acquireTransfer() {
		defer s.releaseTransfer()
	}
	status := httptest.NewRecorder()
	s.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/v1/transfer/status", http.NoBody))
	if status.Code != 200 {
		t.Fatal("status should remain available under transfer pressure")
	}
	transfer := httptest.NewRecorder()
	s.Handler().ServeHTTP(transfer, httptest.NewRequest(http.MethodPost, "/v1/transfer/export", bytes.NewBufferString(`[]`)))
	if transfer.Code != 429 {
		t.Fatal("unbounded transfer admission")
	}
}

func TestBulkRequiresExplicitRootWildcard(t *testing.T) {
	s, _ := testService(t)
	s.secret = []byte("test-secret")
	for _, namespace := range []string{"?", "[*]", `\*`} {
		signed := token(t, s.secret, []string{namespace}, []string{"read"})
		request := httptest.NewRequest(http.MethodGet, "/v1/transfer/status", http.NoBody)
		request.Header.Set(authHeader, signed)
		if err := s.allowed("*", "read", request); err != nil {
			t.Fatal("literal-star fixture", err)
		}
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("namespace %q granted bulk root: %d", namespace, response.Code)
		}
	}
}
