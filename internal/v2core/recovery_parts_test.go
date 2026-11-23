package v2core

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Sealed envelopes are the only source of committed owner scalars. An obsolete
// checkpoint must neither affect recovery nor be rewritten by a later seal.
func TestPartsOnlyRecoveryScalars(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg, policies := recoveryTestSetup(t)
	root := t.TempDir()
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794701400, 0) }}
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	recoveryFixture(t, e, policies, 0, 32)
	want := ownerStateDigest(t, e)
	renders := rollupScanDigest(t, e)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	obsolete := filepath.Join(root, "owner-000", "rollup-state.ckpt")
	if _, err = os.Stat(obsolete); !os.IsNotExist(err) {
		t.Fatalf("checkpoint created: %v", err)
	}
	poison := []byte("obsolete checkpoint must be ignored")
	if err = os.WriteFile(obsolete, poison, 0600); err != nil {
		t.Fatal(err)
	}
	e, err = Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if got := ownerStateDigest(t, e); got != want {
		t.Fatalf("recovery scalars differ:\n%s\nwant:\n%s", got, want)
	}
	if got := rollupScanDigest(t, e); got != renders {
		t.Fatal("recovery changed renders")
	}
	recoveryFixture(t, e, policies, 32, 7)
	want = ownerStateDigest(t, e)
	mergeDrain(t, e.owners[0])
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := ownerStateDigest(t, e); got != want {
		t.Fatalf("merged recovery scalars differ:\n%s\nwant:\n%s", got, want)
	}
	if got, err := os.ReadFile(obsolete); err != nil || string(got) != string(poison) {
		t.Fatalf("obsolete checkpoint rewritten: %q %v", got, err)
	}
}
