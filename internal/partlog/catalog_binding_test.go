package partlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// Test bindings model an independently owned immutable catalog epoch.
type testCatalogBinding struct{ entries map[uint64]catalogEntry }

func (b testCatalogBinding) Bind(id uint64, key string, definition []byte) error {
	if old, ok := b.entries[id]; ok {
		if old.key != key || !old.definitionEqual(definition) {
			return errors.New("changed identity")
		}
		return nil
	}
	b.entries[id] = catalogEntry{key: key, definition: bytes.Clone(definition)}
	return nil
}
func (b testCatalogBinding) Key(id uint64) string        { return b.entries[id].key }
func (b testCatalogBinding) Definition(id uint64) []byte { return b.entries[id].definitionBytes() }
func (b testCatalogBinding) Matches(id uint64, name string, definition []byte) bool {
	e, ok := b.entries[id]
	return ok && e.key == name && e.definitionEqual(definition)
}

func TestCatalogBindingOwnsNoNameEntries(t *testing.T) {
	dir := t.TempDir()
	var c *NameCatalog
	defer func() {
		if c != nil {
			_ = c.Close()
		}
	}()
	for round := range 3 {
		// Recovery must bind every ID into this epoch, rather than retaining a
		// resolver or an ID's strings from the preceding open.
		binding := testCatalogBinding{entries: map[uint64]catalogEntry{}}
		var err error
		c, err = OpenNameCatalog(dir, binding)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 4096 {
			name := fmt.Sprintf("bound.%04d.é.\"metric", i)
			definition := []byte(fmt.Sprintf("opaque definition %d", i))
			id, err := c.Intern(name, definition)
			if err != nil || id != uint64(i+1) {
				t.Fatalf("intern %d: %d %v", i, id, err)
			}
			clear(definition)
			key, raw, ok := c.Lookup(id)
			if !ok || key != name || !bytes.Equal(raw, []byte(fmt.Sprintf("opaque definition %d", i))) {
				t.Fatal("binding lost canonical bytes")
			}
			if _, ok := binding.entries[id]; !ok {
				t.Fatal("stale catalog epoch")
			}
		}
		if c.Count() != 4096 || len(binding.entries) != 4096 {
			t.Fatal("wrong bound cardinality")
		}
		if cap(c.entries) != 0 || len(c.definitionArena) != 0 || len(c.definitionSuffixes) != 0 {
			t.Fatal("bound catalog retains per-name headers or definition storage")
		}
		if _, _, ok := c.Lookup(0); ok {
			t.Fatal("ID zero exists")
		}
		if _, ok := c.Key(4097); ok {
			t.Fatal("ID beyond catalog exists")
		}
		if c.Matches(4097, "absent", nil) {
			t.Fatal("unknown identity matches")
		}
		if round == 1 {
			err = c.Checkpoint()
		} else {
			err = c.Sync()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		c = nil
	}
}

func TestCatalogBindingPreservesDiskBytes(t *testing.T) {
	plainDir, boundDir := t.TempDir(), t.TempDir()
	plain, err := OpenNameCatalog(plainDir)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	bound, err := OpenNameCatalog(boundDir, testCatalogBinding{entries: map[uint64]catalogEntry{}})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	// Epoch is the only intentionally random part of the durable catalog.
	bound.epoch = plain.epoch
	if err := bound.wal.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err := bound.wal.WriteAt(catalogHeader(plain.epoch), 0); err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		name := fmt.Sprintf("metric.%04d", 1000-i)
		raw := []byte(fmt.Sprintf(`{"Name":%q,"policy":"last"}`, name))
		for _, c := range []*NameCatalog{plain, bound} {
			if _, err := c.Intern(name, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, checkpoint := range []bool{false, true} {
		file := "catalog.wal"
		for _, c := range []*NameCatalog{plain, bound} {
			if checkpoint {
				err = c.Checkpoint()
				file = "catalog.checkpoint"
			} else {
				err = c.Sync()
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		a, err := os.ReadFile(filepath.Join(plainDir, file))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(boundDir, file))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(a, b) {
			t.Fatalf("bound catalog changed %s bytes", file)
		}
	}
	if _, err := bound.Intern("metric.1000", []byte("changed")); err == nil {
		t.Fatal("accepted changed definition")
	}
}
