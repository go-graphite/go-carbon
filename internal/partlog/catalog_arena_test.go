package partlog

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCatalogDefinitionArenaOwnership(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenNameCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Cross page boundaries and include a dedicated large definition.
	sizes := []int{3, (64 << 10) - 2, 17, (64 << 10) + 1, 7}
	for i, size := range sizes {
		definition := bytes.Repeat([]byte{byte(i + 1)}, size)
		if _, err := c.Intern(string(rune('a'+i)), definition); err != nil {
			t.Fatal(err)
		}
		clear(definition) // the catalog must own detached immutable bytes
	}
	for phase := 0; phase < 3; phase++ {
		for i, size := range sizes {
			_, definition, ok := c.Lookup(uint64(i + 1))
			if !ok || !bytes.Equal(definition, bytes.Repeat([]byte{byte(i + 1)}, size)) {
				t.Fatalf("phase=%d definition=%d changed", phase, i)
			}
			// Appending to a borrowed definition cannot damage its neighbor.
			_ = append(definition, 255)
		}
		if phase == 0 {
			if err := c.Sync(); err != nil {
				t.Fatal(err)
			}
		} else if err := c.Sync(); err != nil {
			t.Fatal(err)
		}
		if phase == 1 {
			if err := c.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		} else {
			break
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		c, err = OpenNameCatalog(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogSharedDefinitionSuffixRecovery(t *testing.T) {
	root := t.TempDir()
	var first []byte
	for phase := 0; phase < 3; phase++ {
		c, err := OpenNameCatalog(root)
		if err != nil {
			t.Fatal(err)
		}
		for i, name := range []string{"metric.first", "metric.second", `escaped.\"metric`, "unicode.é.metric"} {
			quoted, _ := json.Marshal(name)
			raw := append([]byte(`{"Name":`), quoted...)
			raw = append(raw, []byte(`,"Group":"same","Config":{"value":1}}`)...)
			if phase == 0 {
				id, err := c.Intern(name, raw)
				if err != nil || id != uint64(i+1) {
					t.Fatal(id, err)
				}
			}
			key, got, ok := c.Lookup(uint64(i + 1))
			if !ok || key != name || !bytes.Equal(got, raw) {
				t.Fatalf("recovered encoding changed phase=%d name=%q key=%q got=%q want=%q ok=%v", phase, name, key, got, raw, ok)
			}
			if _, err := c.Intern(name, append(bytes.Clone(raw), ' ')); err == nil {
				t.Fatal("accepted changed canonical bytes")
			}
		}
		if &c.entries[1].definition[0] != &c.entries[2].definition[0] {
			t.Fatal("repeated suffix was copied")
		}
		if phase == 0 {
			first = bytes.Clone(c.entries[1].definition)
		} else if !bytes.Equal(first, c.entries[1].definition) {
			t.Fatal("suffix changed")
		}
		if err := c.Sync(); err != nil {
			t.Fatal(err)
		}
		if phase == 1 {
			if err := c.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
