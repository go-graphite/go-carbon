package carbonserver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Opt-in: inserts captured names (made absent) into a trie backed by the
// captured snapshot, as startup does for checkpoint-only names.
func TestCapturedPendingNameInsert(t *testing.T) {
	dir := os.Getenv("GO_CARBON_SNAPSHOT_DIR")
	if dir == "" {
		t.Skip("set GO_CARBON_SNAPSHOT_DIR")
	}
	cache := filepath.Join(dir, "files.gzip")
	s, err := openIndexSnapshot(cache, filepath.Join(dir, "data-root"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	reader, err := NewFileListCache(cache, FLCVersionUnspecified, 'r')
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var names []string
	for len(names) < 281511 {
		e, err := reader.Read()
		if err != nil {
			t.Fatal(err)
		}
		if len(names) < 281511 && e != nil && strings.HasSuffix(e.Path, ".wsp") && len(names)%1 == 0 {
			names = append(names, strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(e.Path, "/"), ".wsp"), "/", ".")+".unseen")
		}
	}
	for _, mode := range []string{"pending", "insert"} {
		ti := newTrie(".wsp", 0, nil)
		ti.snapshot = s
		started := time.Now()
		for _, name := range names {
			if mode == "pending" {
				err = ti.insertPendingMetric(name)
			} else {
				_, err = ti.insert("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		el := time.Since(started)
		t.Logf("%s: %d names in %v (%v/name)", mode, len(names), el, el/time.Duration(len(names)))
		for _, name := range names[:1000] {
			if _, isNew := ti.metricPath(name, nil); isNew {
				t.Fatal("inserted name not found", name)
			}
		}
	}
}
