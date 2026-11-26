package v2dict

import (
	"encoding/binary"
	"testing"
)

func TestCheckNamesHashFallback(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	e, _, err := d.Append("g", 2, []byte("metric"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = d.merge(d.snap.Load().levels); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	l := d.snap.Load().levels[0]
	if l.idOff-l.hashOff != 8*l.n {
		t.Fatal("hash column is not 8 bytes/name")
	}
	names := []NameID{{"g", 2, "metric", e.ID}}
	decodes := 0
	testHook = func(p string) error {
		if p == "block-decode" {
			decodes++
		}
		return nil
	}
	defer func() { testHook = nil }()
	if err = d.CheckNames(names); err != nil {
		t.Fatal(err)
	}
	if decodes != 0 {
		t.Fatal("matching persisted hash decompressed", decodes)
	}
	// Substitute a private mapping copy: force a mismatch without corrupting the
	// read-only mapped file. Correct names must still pass the exact fallback.
	mapped := l.m
	l.m = append([]byte(nil), mapped...)
	defer func() { l.m = mapped }()
	h := binary.LittleEndian.Uint64(l.m[l.hashOff:])
	binary.LittleEndian.PutUint64(l.m[l.hashOff:], h^1)
	if err = d.CheckNames(names); err != nil {
		t.Fatal("mismatch did not fall back", err)
	}
	if decodes != 1 {
		t.Fatal("mismatch must decode exactly one frame", decodes)
	}
	names[0].Name = "wrong"
	if err = d.CheckNames(names); err == nil {
		t.Fatal("fallback accepted wrong name")
	}
	if decodes != 2 {
		t.Fatal("wrong name did not use exact fallback", decodes)
	}
}
