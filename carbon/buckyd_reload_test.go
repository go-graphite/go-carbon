package carbon

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/golang-jwt/jwt/v5"
)

func TestBuckydTransferDebtReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Buckyd.Enabled = true
	cfg.Buckyd.JWTSecretFile = filepath.Join(filepath.Dir(path), "buckyd.secret")
	if err := os.WriteFile(cfg.Buckyd.JWTSecretFile, []byte("reload-test-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	service, db, writer := app.Buckyd, app.MetricStore, app.Persister
	for _, limit := range []int64{24 << 30, 8 << 30, -1, 0} {
		cfg.Buckyd.TransferMaxCompactionDebt = limit
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if service.TransferMaxCompactionDebt() != limit {
			t.Fatalf("live debt cap %d, want %d", service.TransferMaxCompactionDebt(), limit)
		}
		if app.Buckyd != service || app.MetricStore != db || app.Persister != writer {
			t.Fatal("debt reload replaced a running component")
		}
	}
	// max_transfers applies live too (a SIGHUP may raise 16 -> 64).
	for _, limit := range []int{16, 64, 3} {
		cfg.Buckyd.MaxTransfers = limit
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if service.MaxTransfers() != limit || app.Buckyd != service {
			t.Fatalf("live transfer cap %d, want %d", service.MaxTransfers(), limit)
		}
	}
	var calls atomic.Int64
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusTeapot) }))
	defer donor.Close()
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, buckyd.ACL{Namespaces: []string{"*"}, Ops: []string{"*"}}).SignedString([]byte("reload-test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	for _, allowed := range []bool{true, false, true} {
		cfg.Buckyd.TransferSources = nil
		if allowed {
			cfg.Buckyd.TransferSources = []string{strings.TrimPrefix(donor.URL, "http://")}
		}
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.Buckyd != service || app.MetricStore != db {
			t.Fatal("source reload replaced running components")
		}
		before := calls.Load()
		req := httptest.NewRequest(http.MethodPost, "/v1/transfer/pull", strings.NewReader(`{"source":"`+donor.URL+`","names":["reload.metric"]}`))
		req.Header.Set("X-Buckyd-Authorization", signed)
		response := httptest.NewRecorder()
		service.Handler().ServeHTTP(response, req)
		if allowed {
			if calls.Load() != before+1 {
				t.Fatalf("reloaded source not contacted: %d %s", response.Code, response.Body.String())
			}
		} else if response.Code != http.StatusForbidden || calls.Load() != before {
			t.Fatalf("removed source reached donor: %d", response.Code)
		}
	}
	cfg.Buckyd.TransferMaxCompactionDebt = 24 << 30
	cfg.Buckyd.Bind = "127.0.0.1:4243"
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("listener change accepted without restart")
	}
	if service.TransferMaxCompactionDebt() != 0 || service.MaxTransfers() != 3 {
		t.Fatal("rejected reload changed live debt or transfer cap")
	}
}
