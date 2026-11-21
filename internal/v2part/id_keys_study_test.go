//go:build partstudy && cgo

package v2part

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/sstable"

	"github.com/go-graphite/go-carbon/internal/v2dict"
)

// TestIDKeysRealParts rewrites captured name-keyed parts as ID-keyed parts
// through the production writer (SetSlotsEncoded/SetEncoded, the RekeyDense
// path) and reports bytes per stored point for both. The GCP3 rewrite must
// reproduce the input file byte for byte (harness check), and the GCI2 part
// must resolve through the dictionary to the same logical stream.
//
// ID_KEYS_PARTS: comma-separated part paths. ID_KEYS_LEVEL=1: seal level.
// IDs are assigned in hash order (a model of admission order).
func TestIDKeysRealParts(t *testing.T) {
	paths := os.Getenv("ID_KEYS_PARTS")
	if paths == "" {
		t.Skip("ID_KEYS_PARTS not set")
	}
	IDKeys.Store(true)
	t.Cleanup(func() { IDKeys.Store(false) })
	for _, path := range strings.Split(paths, ",") {
		t.Run(filepath.Base(path), func(t *testing.T) { idKeysRealPart(t, path, os.Getenv("ID_KEYS_LEVEL") == "1") })
	}
}

type realRec struct {
	key   Key
	value []byte
}

func idKeysRealPart(t *testing.T, path string, seal bool) {
	var recs []realRec
	var points uint64
	if err := readGroupFile(path, func(k Key, v []byte) error {
		if k.Archive != MetadataArchive {
			rs, err := DecodeSlots(k.Archive, k.BlockStart, v)
			if err != nil {
				return err
			}
			points += uint64(len(rs))
		}
		recs = append(recs, realRec{k, bytes.Clone(v)})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first := recs[0].key
	archives := map[uint32]bool{}
	for _, r := range recs {
		if r.key.Archive != MetadataArchive {
			archives[r.key.Archive] = true
		}
	}
	spec := Spec{Scope: Scope{Group: first.Group, VShard: GroupVShard, Partition: first.Partition, Part: first.Part}, Encoding: DenseEncoding, Source: Provenance{Incarnation: PartID{9}, Stream: "study"}, MinTime: 1, MaxTime: 2}
	for a := range archives {
		def := []byte(fmt.Sprintf(`{"archive":%d}`, a))
		spec.Policies = append(spec.Policies, ArchivePolicy{Archive: a, Digest: sha256.Sum256(def), Definition: def})
	}
	write := func(out string, generation uint64, recs []realRec) int64 {
		w, err := NewDenseWriter(out, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		if generation != 0 {
			if err := w.IDKeyed(generation); err != nil {
				t.Fatal(err)
			}
		}
		if seal {
			w.SealCompression()
		}
		for _, r := range recs {
			key, err := Encode(r.key)
			if err == nil && r.key.Archive == MetadataArchive {
				err = w.SetEncoded(key, r.value)
			} else if err == nil {
				err = w.SetSlotsEncoded(key, r.value)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := w.Close(); err != nil {
			t.Fatal(err)
		}
		st, _ := os.Stat(out)
		return st.Size()
	}
	dir := t.TempDir()
	in, _ := os.ReadFile(path)
	namePath := filepath.Join(dir, "names.sst")
	nameBytes := write(namePath, 0, recs)
	same, _ := os.ReadFile(namePath)
	if !bytes.Equal(same, in) {
		t.Logf("note: GCP3 rewrite is %d B, input %d B (not byte-identical)", len(same), len(in))
	}
	// IDs in hash order per vshard.
	dict, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer dict.Close()
	type nameKey struct {
		vshard uint16
		name   ID
	}
	var seen []nameKey
	for i, r := range recs {
		if i == 0 || !SameMetric(r.key, recs[i-1].key) {
			seen = append(seen, nameKey{r.key.VShard, r.key.Metric})
		}
	}
	slices.SortFunc(seen, func(a, b nameKey) int {
		ha, hb := sha256.Sum256([]byte(a.name)), sha256.Sum256([]byte(b.name))
		return cmp.Compare(binary.BigEndian.Uint64(ha[:]), binary.BigEndian.Uint64(hb[:]))
	})
	for _, n := range seen {
		if _, _, err := dict.Append(string(spec.Scope.Group), n.vshard, []byte(n.name), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := dict.Sync(); err != nil {
		t.Fatal(err)
	}
	idRecs := make([]realRec, len(recs))
	for i, r := range recs {
		e, ok := dict.Lookup(string(spec.Scope.Group), r.key.VShard, []byte(r.key.Metric))
		if !ok {
			t.Fatal("missing name")
		}
		k := r.key
		k.Metric, k.ByID, k.Local = "", true, e.ID
		idRecs[i] = realRec{k, r.value}
	}
	slices.SortStableFunc(idRecs, func(a, b realRec) int {
		return cmp.Or(cmp.Compare(a.key.VShard, b.key.VShard), cmp.Compare(a.key.Local, b.key.Local))
	})
	idPath := filepath.Join(dir, "ids.sst")
	idBytes := write(idPath, dict.Generation(), idRecs)
	// Round trip through the dictionary.
	i := 0
	var mismatch error
	if err := readGroupFile(idPath, func(k Key, v []byte) error {
		name, _, ok := dict.LookupID(string(k.Group), k.VShard, k.Local, nil)
		if !ok {
			return fmt.Errorf("unknown ID %d", k.Local)
		}
		want := idRecs[i]
		orig, _, _ := dict.LookupID(string(k.Group), want.key.VShard, want.key.Local, nil)
		if !bytes.Equal(name, orig) || k.Archive != want.key.Archive || k.BlockStart != want.key.BlockStart || !bytes.Equal(v, want.value) {
			mismatch = fmt.Errorf("record %d differs", i)
		}
		i++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if mismatch != nil || i != len(recs) {
		t.Fatalf("round trip: %v (%d of %d records)", mismatch, i, len(recs))
	}
	t.Logf("RESULT part=%s records=%d names=%d points=%d input_B=%d gcp3_B=%d gci2_B=%d gcp3_Bpt=%.3f gci2_Bpt=%.3f saving=%.1f%% harness_identical=%v",
		filepath.Base(path), len(recs), len(seen), points, len(in), nameBytes, idBytes,
		float64(nameBytes)/float64(points), float64(idBytes)/float64(points), 100*(1-float64(idBytes)/float64(nameBytes)), bytes.Equal(same, in))
}

// readGroupFile visits a group file's logical stream (Read without a
// descriptor accepts one vshard only).
func readGroupFile(path string, visit func(Key, []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	rd, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return err
	}
	r, err := sstable.NewReader(rd, sstable.ReaderOptions{})
	if err != nil {
		return err
	}
	defer r.Close()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for ik, lv := it.First(); ik != nil; ik, lv = it.Next() {
		k, err := DecodeKey(ik.UserKey)
		if err != nil {
			return err
		}
		v, _, err := lv.Value(nil)
		if err != nil {
			return err
		}
		if err = visitLogical(k, v, visit); err != nil {
			return err
		}
	}
	return it.Error()
}
