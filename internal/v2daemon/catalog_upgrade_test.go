package v2daemon

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// upgradeFixture writes the state an older build leaves: dictionary entries
// without a policy (written before the policy table existed) and a producer
// catalog that alone holds each name's admitted definition. It returns the
// dictionary ID of every name.
func upgradeFixture(t *testing.T, root string, names []string) map[string]uint32 {
	t.Helper()
	d, err := v2dict.Open(dictDir(root), v2dict.Options{Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]uint32{}
	for _, name := range names {
		e, _, err := d.Append("test", v2part.Route(v2part.ID(name)), []byte(name), 0)
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = e.ID
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dictDir(root), "policies")); err != nil {
		t.Fatal(err)
	}
	c, err := partlog.OpenNameCatalog(catalogDir(root))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err = c.Intern(name, admitPolicy(t, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func catalogDir(root string) string {
	return filepath.Join(root, "placement-group-00000001", "journal", "producer-000", "catalog")
}

// refusingOwner is a receiver whose resolver fails the test: a name the store
// knows must keep the policy it was first admitted with, never today's.
func refusingOwner(t *testing.T, p *Producer) *applicationOwner {
	return &applicationOwner{p: p, resolve: func(name string) ([]byte, error) {
		t.Errorf("%s: known name repeated schema resolution after the upgrade", name)
		return nil, errors.New("refused")
	}}
}

// TestUpgradeRetiresCatalog opens an older build's store: the restart maps
// every catalog name into the dictionary under its admitted policy, removes
// the catalog, and from then on a known name is admitted from the
// dictionary alone, on its old ID, with renders and find equal across a
// second restart. A kill after the mapping and before the removal (the
// catalog restored beside the mapped dictionary) and a kill after the
// rename (a leftover catalog.retired) both open to the same state.
func TestUpgradeRetiresCatalog(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	root := t.TempDir()
	names := []string{"up.a", "up.b", "up.c"}
	ids := upgradeFixture(t, root, names)
	saved := filepath.Join(t.TempDir(), "catalog")
	if err := exec.Command("cp", "-R", catalogDir(root), saved).Run(); err != nil {
		t.Fatal(err)
	}

	open := func() *Store {
		t.Helper()
		cfg := admitConfig()
		s, err := Open(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	check := func(s *Store) {
		t.Helper()
		if _, err := os.Stat(catalogDir(root)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("catalog still present after the upgrade: %v", err)
		}
		if _, err := os.Stat(catalogDir(root) + ".retired"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired catalog left behind: %v", err)
		}
		for _, name := range names {
			h, err := s.Core.FindIdentity(name)
			if err != nil || h == nil {
				t.Fatalf("%s: dictionary does not serve the upgraded name: %v", name, err)
			}
			if id, ok := h.DictID(); !ok || id != ids[name] {
				t.Fatalf("%s: dictionary ID %d ok=%v, want %d", name, id, ok, ids[name])
			}
			def, ok := h.DictionaryDefinition()
			if !ok || !bytes.Equal(def, admitPolicy(t, name)) || !bytes.Equal(h.Definition(), def) {
				t.Fatalf("%s: dictionary definition %q ok=%v", name, def, ok)
			}
		}
	}

	// The upgrade restart, then a second open of the upgraded store.
	s := open()
	check(s)
	p := s.Producers[0]
	o := refusingOwner(t, p)
	for i, name := range names {
		if err := o.Append([]string{name}, nil, probePoints(1), nil, int64(1000+i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	render := func(s *Store) string {
		t.Helper()
		pin := s.Core.DBs()[0].NewSnapshot()
		defer pin.Close()
		snapshot, err := v2core.NewRollupSnapshot(context.Background(), pin, nil, s.cfg.Hooks.Identities, nil)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		for _, name := range names {
			_, meta, ok := snapshot.LookupMetadata(name)
			records, err := snapshot.Records(context.Background(), name)
			if !ok || err != nil || len(records) == 0 {
				t.Fatalf("%s: find ok=%v records=%d err=%v", name, ok, len(records), err)
			}
			fmt.Fprintf(&out, "%s %#x %+v\n", name, meta.ID, records)
		}
		return out.String()
	}
	want := render(s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	check(s)
	if got := render(s); got != want {
		t.Fatalf("restart changed find/render:\n%s\nwant\n%s", got, want)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Kill after the mapping, before the rename: the catalog is back beside
	// the mapped dictionary. Kill after the rename: catalog.retired is left.
	if err := exec.Command("cp", "-R", saved, catalogDir(root)).Run(); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("cp", "-R", saved, catalogDir(root)+".retired").Run(); err != nil {
		t.Fatal(err)
	}
	s = open()
	defer func() { _ = s.Close() }()
	check(s)
	if got := render(s); got != want {
		t.Fatalf("interrupted upgrade changed find/render:\n%s\nwant\n%s", got, want)
	}

	// An expired name (deleted from the dictionary) is new again after the
	// upgrade: it resolves today's policy once and revives under a new ID.
	const gone = "up.c"
	if ok, err := s.Core.Dictionary().Delete("test", v2part.Route(v2part.ID(gone)), []byte(gone)); !ok || err != nil {
		t.Fatalf("expire %s: ok=%v err=%v", gone, ok, err)
	}
	if h, err := s.Core.FindIdentity(gone); h != nil || err != nil {
		t.Fatalf("an expired name was found: %v", err)
	}
	calls := 0
	o = &applicationOwner{p: s.Producers[0], resolve: func(name string) ([]byte, error) {
		calls++
		return admitPolicy(t, name), nil
	}}
	if err := o.Append([]string{gone}, nil, probePoints(1), nil, 2000); err != nil {
		t.Fatal(err)
	}
	h, err := s.Core.FindIdentity(gone)
	if err != nil || h == nil {
		t.Fatalf("revived name not found: %v", err)
	}
	if id, ok := h.DictID(); calls != 1 || !ok || id == ids[gone] {
		t.Fatalf("revival: resolver calls=%d ID %d ok=%v, old ID %d", calls, id, ok, ids[gone])
	}
}

// writeV1L0 writes an older build's version-1 L0 run file: one application
// per name, keyed by the catalog NumericID of its local ID (catalog order).
func writeV1L0(t *testing.T, root string, names []string) {
	t.Helper()
	b := append([]byte(nil), "V2L0\x01"...)
	b = binary.AppendUvarint(b, uint64(len(names)))
	for i := range names {
		order := uint64(i + 1)
		b = binary.AppendUvarint(b, NumericID(order, 0, 1)) // local IDs are 1..n
		b = binary.AppendUvarint(b, order)
		b = binary.AppendVarint(b, 10020)
		b = binary.AppendUvarint(b, 0) // no policy bytes
		b = binary.AppendUvarint(b, 1)
		b = binary.AppendVarint(b, 0)
		b = binary.AppendVarint(b, 10000+int64(order))
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(order)))
		b = binary.AppendVarint(b, 0)
	}
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	dir := filepath.Join(root, "placement-group-00000001", "core", "owner-000", "l0")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("l0-%020d", 1)), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestUpgradeV1L0 opens an older build's store that was killed with rows in
// a version-1 L0 file: the upgrade maps the file's NumericIDs through the
// catalog once, seals them before it removes the catalog, and the rows read
// back under the names' dictionary IDs, equal across a second restart.
func TestUpgradeV1L0(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	root := t.TempDir()
	names := []string{"v1.a", "v1.b"}
	ids := upgradeFixture(t, root, names)
	writeV1L0(t, root, names)
	render := func() string {
		t.Helper()
		s, err := Open(root, admitConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		if _, err := os.Stat(catalogDir(root)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("catalog still present: %v", err)
		}
		if left, _ := filepath.Glob(filepath.Join(root, "placement-group-00000001", "core", "owner-000", "l0", "*")); len(left) != 0 {
			t.Fatalf("the upgrade removed the catalog before a seal covered %v", left)
		}
		pin := s.Core.DBs()[0].NewSnapshot()
		defer pin.Close()
		snapshot, err := v2core.NewRollupSnapshot(context.Background(), pin, nil, s.cfg.Hooks.Identities, nil)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		for i, name := range names {
			_, meta, ok := snapshot.LookupMetadata(name)
			records, err := snapshot.Records(context.Background(), name)
			if !ok || err != nil || len(records) != 1 || records[0].Sample.Time != 10001+int64(i) || records[0].Sample.Bits != math.Float64bits(float64(i+1)) {
				t.Fatalf("%s: ok=%v records=%+v err=%v", name, ok, records, err)
			}
			if g, vs, id := v2core.UnpackID(meta.ID); !v2core.IsPackedID(meta.ID) || vs != v2part.Route(v2part.ID(name)) || id != ids[name] || g == 0 {
				t.Fatalf("%s sealed under %#x, want dictionary ID %d", name, meta.ID, ids[name])
			}
			fmt.Fprintf(&out, "%s %#x %+v\n", name, meta.ID, records)
		}
		return out.String()
	}
	if first, second := render(), render(); first != second {
		t.Fatalf("restart changed find/render:\n%s\nwant\n%s", second, first)
	}
}
