package v2dict

import (
	"bytes"
	"fmt"
	"testing"
)

func TestTransientFilterLegacyRoundtrip(t *testing.T) {
	path := levelPath(t.TempDir(), 1, 1)
	l, err := writeLevelKind(path, 1, 1, false, func(w *levelWriter) {
		for i := range 1000 {
			w.add(append(scopeKey(nil, "g", 0), fmt.Appendf(nil, "n%04d", i)...), (Entry{ID: uint32(i)}).put(nil))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if l.blockN != blockEntries {
		t.Fatalf("L1 block size %d, want legacy %d", l.blockN, blockEntries)
	}
	if l.compact || len(l.filter) == 0 {
		t.Fatal("transient L1 has no miss filter")
	}
	reopened, err := openLevel(path, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.compact || len(reopened.filter) != 0 || !bytes.Equal(l.m, reopened.m) {
		t.Fatal("transient filter changed L1 format")
	}
	misses := 0
	for i := range 2000 {
		key := append(scopeKey(nil, "g", 0), fmt.Appendf(nil, "n%04d", i)...)
		for _, run := range []*level{l, reopened} {
			e, ok := run.get(key)
			if ok != (i < 1000) || (ok && e.ID != uint32(i)) {
				t.Fatal(i, e, ok)
			}
		}
		if i >= 1000 && !l.mayContain(key) {
			misses++
		}
	}
	if misses < 900 {
		t.Fatal("filter did not reject absent names", misses)
	}
}
