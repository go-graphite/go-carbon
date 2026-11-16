//go:build darwin || linux

package v2core

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestSealedGCAllocatedDiskBytes(t *testing.T) {
	db, g := gcFixture(t, t.TempDir())
	defer db.Close()
	allocated := func() uint64 {
		paths, err := filepath.Glob(filepath.Join(g.dir, "*"))
		if err != nil {
			t.Fatal(err)
		}
		var total uint64
		for _, path := range paths {
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			total += uint64(st.Sys().(*syscall.Stat_t).Blocks) * 512
		}
		return total
	}
	var ids []v2part.Digest
	for i := byte(1); i <= 16; i++ {
		d := gcPart(t, g, "fixture", i, 100)
		ids = append(ids, d.SHA256)
	}
	before := allocated()
	pinned := db.NewSnapshot()
	if err := g.retire(ids, gcAuthorize); err != nil {
		t.Fatal(err)
	}
	gcAdvance(g)
	if _, err := g.sweep(); err != nil {
		t.Fatal(err)
	}
	held := allocated()
	if held != before {
		t.Fatalf("allocated bytes changed under reader: %d -> %d", before, held)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.sweep(); err != nil {
		t.Fatal(err)
	}
	after := allocated()
	if after != 0 || before == 0 {
		t.Fatalf("disk not reclaimed: %d -> %d", before, after)
	}
	t.Logf("allocated fixture parts=16 before=%d reader_held=%d after=%d reclaimed=%d bytes", before, held, after, before-after)
}
