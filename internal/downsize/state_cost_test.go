package downsize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Optional measurement against a private, authenticated prefix of a real
// catalog. This is not whole-unit adoption or executable-swap qualification.
func TestTypedStateCatalogCost(t *testing.T) {
	input := os.Getenv("STORECTL_TYPED_CATALOG")
	if input == "" {
		t.Skip("set STORECTL_TYPED_CATALOG and STORECTL_TYPED_NAMES for a frozen union.tsv sample")
	}
	names, err := strconv.ParseInt(os.Getenv("STORECTL_TYPED_NAMES"), 10, 64)
	if err != nil || names < 1 {
		t.Fatal("invalid STORECTL_TYPED_NAMES")
	}
	s := newTypedState(t, names)
	// Link only a caller-owned frozen sample. The importer never writes it.
	if err := os.Link(input, filepath.Join(filepath.Dir(s.path), "union.tsv")); err != nil {
		t.Fatal(err)
	}
	stats, err := s.adoptCatalog("union.tsv")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	k := stateMemberKey{Scope: "root", Phase: "window retry", Kind: "done", Holder: "source", Window: "sample"}
	for first := int64(1); first <= names; first += 8192 {
		ids := make([]int64, min(int64(8192), names-first+1))
		for i := range ids {
			ids[i] = first + int64(i)
		}
		if n, err := s.completeMembers(k, strconv.FormatInt(first, 10), ids, nil); err != nil || n != int64(len(ids)) {
			t.Fatalf("unique completion: %d %v", n, err)
		}
	}
	seconds := time.Since(started).Seconds()
	if typedPhase(t, s).Done != names {
		t.Fatal("unique completion counter mismatch")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(s.path)
	if err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	ro, err := openStateStore(s.path, s.unit, true)
	if err != nil {
		t.Fatal(err)
	}
	if typedPhase(t, ro).Done != names {
		t.Fatal("reopened completion counter mismatch")
	}
	openSeconds := time.Since(started).Seconds()
	if err := ro.close(); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(map[string]any{"adoption": stats, "catalog_names": names, "database_bytes_after_close": st.Size(), "unique_names_per_second": float64(names) / seconds, "update_seconds": seconds, "open_and_phase_seconds": openSeconds})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
}
