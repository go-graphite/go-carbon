package v2dict

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPolicyTable: indexes are dense from 1 and stable, survive a flush, a
// reopen and a torn tail, and an old dictionary (no table) reads policy 0.
func TestPolicyTable(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{Shards: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.Append("g", 1, []byte("old.name"), 0); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	// A dictionary from before the table: its file does not exist.
	if err = os.Remove(filepath.Join(dir, policyFile)); err != nil {
		t.Fatal(err)
	}
	if d, err = Open(dir, Options{Shards: 2}); err != nil {
		t.Fatal(err)
	}
	if e, ok := d.Lookup("g", 1, []byte("old.name")); !ok || e.Policy != 0 {
		t.Fatalf("old entry %+v ok=%v", e, ok)
	}
	if _, ok := d.Policy(0); ok {
		t.Fatal("policy 0 has a suffix")
	}
	a, err := d.InternPolicy([]byte(`,"a":1}`))
	if err != nil || a != 1 {
		t.Fatalf("first policy %d %v", a, err)
	}
	if again, _ := d.InternPolicy([]byte(`,"a":1}`)); again != a {
		t.Fatalf("policy index changed %d -> %d", a, again)
	}
	b, _ := d.InternPolicy([]byte(`,"b":2}`))
	if b != 2 {
		t.Fatalf("second policy %d", b)
	}
	if _, _, err = d.Append("g", 1, []byte("new.name"), b); err != nil {
		t.Fatal(err)
	}
	if err = d.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = d.Flush(); err != nil { // the entry moves to a level file
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	// A torn record after the last synced one.
	f, err := os.OpenFile(filepath.Join(dir, policyFile), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{20, 0, 0, 0, 9, 9, 9, 9, 3, 0, ','}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if d, err = Open(dir, Options{Shards: 2}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if s, ok := d.Policy(b); !ok || string(s) != `,"b":2}` {
		t.Fatalf("policy %d after reopen: %q %v", b, s, ok)
	}
	if e, ok := d.Lookup("g", 1, []byte("new.name")); !ok || e.Policy != b {
		t.Fatalf("entry %+v ok=%v", e, ok)
	}
	if c, err := d.InternPolicy([]byte(`,"c":3}`)); err != nil || c != 3 {
		t.Fatalf("policy after torn tail %d %v", c, err)
	}
	if _, ok := d.Policy(4); ok {
		t.Fatal("an index never interned resolved")
	}
}

// TestSyncSkipsCoveredFsync: a Sync with no record since the last durable
// point does not fsync, so drains that admit only known names cost nothing.
func TestSyncSkipsCoveredFsync(t *testing.T) {
	d, err := Open(t.TempDir(), Options{Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	syncs := 0
	testHook = func(p string) error {
		if p == "log-sync" {
			syncs++
		}
		return nil
	}
	defer func() { testHook = nil }()
	for i := range 3 {
		if err = d.Sync(); err != nil {
			t.Fatal(err)
		}
		if syncs != 0 {
			t.Fatalf("sync %d of an unchanged log fsynced", i)
		}
	}
	if _, _, err = d.Append("g", 0, []byte("n"), 0); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = d.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if syncs != 1 {
		t.Fatalf("%d fsyncs for one appended record, want 1", syncs)
	}
}
