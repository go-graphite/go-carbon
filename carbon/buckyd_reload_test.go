package carbon

import (
	"os"
	"path/filepath"
	"testing"
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
		want := limit
		if want == 0 {
			want = 8 << 30
		}
		if service.TransferMaxCompactionDebt() != want {
			t.Fatalf("live debt cap %d, want %d", service.TransferMaxCompactionDebt(), want)
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
	cfg.Buckyd.TransferMaxCompactionDebt = 24 << 30
	cfg.Buckyd.Bind = "127.0.0.1:4243"
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("listener change accepted without restart")
	}
	if service.TransferMaxCompactionDebt() != 8<<30 || service.MaxTransfers() != 3 {
		t.Fatal("rejected reload changed live debt or transfer cap")
	}
}
