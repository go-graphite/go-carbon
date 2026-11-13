package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
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
	// No transfer-sources allowlist: any source is accepted with a valid token.
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

func TestTransferDebtLimit(t *testing.T) {
	s, _ := testService(t)
	if s.TransferMaxCompactionDebt() != 8<<30 {
		t.Fatalf("default debt limit %d", s.TransferMaxCompactionDebt())
	}
	for _, c := range []struct {
		limit, debt int64
		paused      bool
	}{
		{8 << 30, 8 << 30, false},
		{8 << 30, 8<<30 + 1, true},
		{24 << 30, 12 << 30, false},
		{8 << 30, 12 << 30, true},
		{0, 8<<30 + 1, true},
		{-1, 1 << 40, false},
	} {
		s.SetTransferMaxCompactionDebt(c.limit)
		if got := s.storagePressure(chunkstore.Stats{CompactionDebt: uint64(c.debt)}); got != c.paused {
			t.Errorf("limit %d debt %d: paused %v, want %v", c.limit, c.debt, got, c.paused)
		}
	}
	if s.config.TransferMaxCompactionDebt != 8<<30 {
		t.Fatal("live updates changed immutable startup config")
	}
	s.SetTransferMaxCompactionDebt(-1)
	if !s.storagePressure(chunkstore.Stats{L0Sublevels: s.store.TransferL0StopWrites() * 3 / 4}) {
		t.Error("L0 must pause transfers with the debt check disabled")
	}
	if !s.storagePressure(chunkstore.Stats{WriteStallActive: true}) {
		t.Error("write stall must pause transfers with the debt check disabled")
	}
}

func TestTransferDebtReloadConcurrentAdmission(t *testing.T) {
	s, _ := testService(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				s.SetTransferMaxCompactionDebt(24 << 30)
				s.SetTransferMaxCompactionDebt(0)
			}
		})
		wg.Go(func() {
			for range 1000 {
				s.storagePressure(chunkstore.Stats{CompactionDebt: 12 << 30})
				if !s.storagePressure(chunkstore.Stats{WriteStallActive: true}) {
					t.Error("reload bypassed write-stall guard")
				}
			}
		})
	}
	wg.Wait()
}
