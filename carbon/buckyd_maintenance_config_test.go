package carbon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuckydMaintenanceConcurrencyConfig(t *testing.T) {
	t.Parallel()
	if b := NewConfig().Buckyd; b.MaxMaintenanceRequests != 2 || b.MaintenanceWorkers != 8 {
		t.Fatalf("defaults requests=%d workers=%d", b.MaxMaintenanceRequests, b.MaintenanceWorkers)
	}
	example, err := ReadConfig("../go-carbon.conf.example")
	if err != nil {
		t.Fatal(err)
	}
	if b := example.Buckyd; b.MaxMaintenanceRequests != 2 || b.MaintenanceWorkers != 8 {
		t.Fatalf("example requests=%d workers=%d", b.MaxMaintenanceRequests, b.MaintenanceWorkers)
	}
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	if err := os.WriteFile(path, []byte("[buckyd]\nmax-maintenance-requests = 6\nmaintenance-workers = 16\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if b := cfg.Buckyd; b.MaxMaintenanceRequests != 6 || b.MaintenanceWorkers != 16 || len(cfg.unknownKeys) != 0 {
		t.Fatalf("configured requests=%d workers=%d unknown=%v", b.MaxMaintenanceRequests, b.MaintenanceWorkers, cfg.unknownKeys)
	}
}
