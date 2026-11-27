package v2dict

import (
	"os"
	"path/filepath"
	"testing"
)

// TestGroupTable: group indexes are dense from 1, separate from policy
// indexes, durable across a reopen, and a torn last record is cut.
func TestGroupTable(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{Shards: 2})
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := d.InternPolicy([]byte(`,"x":1}`)); p != 1 {
		t.Fatalf("policy index %d", p)
	}
	a, err := d.InternGroup("alpha")
	b, err2 := d.InternGroup("")
	if err != nil || err2 != nil || a != 1 || b != 2 {
		t.Fatalf("group indexes %d %d %v %v", a, b, err, err2)
	}
	if again, _ := d.InternGroup("alpha"); again != a {
		t.Fatal("group index not stable")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	// A torn third record is cut on open.
	f, err := os.OpenFile(filepath.Join(dir, groupFile), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{9, 0, 0, 0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if d, err = Open(dir, Options{Shards: 2}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if g, ok := d.Group(1); !ok || g != "alpha" {
		t.Fatalf("group 1 = %q %v", g, ok)
	}
	if g, ok := d.Group(2); !ok || g != "" {
		t.Fatalf("group 2 = %q %v", g, ok)
	}
	if _, ok := d.Group(3); ok {
		t.Fatal("torn group record survived")
	}
	if c, _ := d.InternGroup("gamma"); c != 3 {
		t.Fatalf("next group index %d", c)
	}
	if s, ok := d.Policy(1); !ok || string(s) != `,"x":1}` {
		t.Fatal("policy table changed by groups")
	}
}
