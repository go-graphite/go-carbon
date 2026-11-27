package v2core

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A long read-only oracle must neither reclaim original links nor write GC
// tombstones when the retired-part grace period elapses.
func TestReadOnlyRetiredPartsSurviveIdleGC(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	root := t.TempDir()
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	g := e.owners[0].gc
	d := gcPart(t, g, "fixture", 1, 100)
	if err := g.retire([]v2part.Digest{d.SHA256}, gcAuthorize); err != nil {
		t.Fatal(err)
	}
	path := gcPath(g, d)
	marker := partBinding(retiredPartPrefix, d.SHA256)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = OpenReadOnly(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	g = e.owners[0].gc
	gcAdvance(g)
	time.Sleep(2100 * time.Millisecond) // cross two real maintenance ticks
	closeErr := e.Close()
	if closeErr != nil {
		t.Errorf("read-only close after grace: %v", closeErr)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("read-only reader removed retired original: %v", err)
	}
	db, err := pebble.Open(filepath.Dir(g.dir), &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, closer, err := db.Get(marker)
	if err != nil {
		t.Fatalf("read-only reader changed retirement marker: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}
