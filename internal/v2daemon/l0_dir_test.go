package v2daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestL0DirOutsideRoot covers an L0 directory outside root (v2.l0-dir on
// tmpfs): a clean stop removes it, a missing one (after a reboot) opens
// exactly, two stores never share one, and a new store at a wiped root drops
// run files the old store left.
func TestL0DirOutsideRoot(t *testing.T) {
	root, shm := t.TempDir(), t.TempDir()
	cfg := l0Config()
	cfg.L0Dir = filepath.Join(shm, "go-carbon-v2-l0", "store")
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].AppendApplications(restartApps(t, 0, 6)); err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].Drain(); err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(cfg.L0Dir, "owner-000", "l0-*")); len(files) == 0 {
		t.Fatal("published rows wrote no L0 file under L0Dir")
	}
	if files, _ := filepath.Glob(filepath.Join(root, "*", "core", "*", "l0", "*")); len(files) != 0 {
		t.Fatalf("L0 files under root: %v", files)
	}
	// Another store may not claim it while (or after) this one owns it.
	if _, err = Open(t.TempDir(), cfg); err == nil || !strings.Contains(err.Error(), "belongs to store") {
		t.Fatalf("second store on one L0 dir: %v", err)
	}
	// A clean stop seals every run file: nothing in L0 is needed after it.
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(cfg.L0Dir); !os.IsNotExist(err) {
		t.Fatalf("clean stop kept the L0 dir: %v", err)
	}
	// After a reboot the directory is gone: open recreates it and is exact.
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkRestartOracle(t, s, restartApps(t, 0, 6), 3)
	if err = s.Producers[0].AppendApplications(restartApps(t, 6, 12)); err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].Drain(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkRestartOracle(t, s, restartApps(t, 0, 12), 3)
	// Leave a run file behind, as a kill -9 would, then wipe the store: the
	// new store at that root must not load the old store's rows.
	stale := filepath.Join(cfg.L0Dir, "owner-000", "l0-00000000000000000099")
	if err = os.WriteFile(stale, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(stale); err != nil {
		t.Fatalf("close removed an unsealed run file: %v", err)
	}
	if err = os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatalf("new store over stale L0 dir: %v", err)
	}
	if _, err = os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("new store kept the wiped store's run file: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
