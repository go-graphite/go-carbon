package partlog

import (
	"bytes"
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
		} else if phase == 1 {
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
