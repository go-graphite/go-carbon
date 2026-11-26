package v2dict

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

func TestFilterNoFalseNegatives(t *testing.T) {
	rng := rand.New(rand.NewPCG(123, 456))
	for _, n := range []int{0, 1, 31, 32, 33, 511, 4097, 50000} {
		filter := make([]byte, filterBytes(n))
		hashes := make([]uint64, n)
		for i := range hashes {
			hashes[i] = rng.Uint64()
			bloomAdd(filter, hashes[i])
		}
		// Independently check all seven construction probes, including overflow.
		for _, h := range hashes {
			base := int((h>>32)%uint64(len(filter)/64)) * 64
			x, delta := ((h << 31) | (h >> 33)), h|1
			for range 7 {
				p := x & 511
				if filter[base+int(p/8)]&(1<<(p%8)) == 0 {
					t.Fatalf("false negative n=%d", n)
				}
				x += delta
			}
		}
	}
	// Persist/reopen exercises the reader's independent membership path.
	dir := t.TempDir()
	path := levelPath(dir, 1, 1)
	l, err := writeLevel(path, 1, 1, func(w *levelWriter) {
		for i := range 10000 {
			w.add(append(scopeKey(nil, "g", uint16(i/1000)), fmt.Appendf(nil, "metric.%08d", i)...), (Entry{ID: uint32(i % 1000)}).put(nil))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10000 {
		key := append(scopeKey(nil, "g", uint16(i/1000)), fmt.Appendf(nil, "metric.%08d", i)...)
		if !l.mayContain(key) {
			t.Fatalf("persisted false negative %d", i)
		}
	}
	// A filter bit error must reject the file, never silently report misses.
	data := append([]byte(nil), l.m...)
	data[len(data)-footerSize-5] ^= 1
	bad := filepath.Join(dir, "bad.lvl")
	if err := os.WriteFile(bad, data, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := openLevel(bad, 1, 1); err == nil {
		t.Fatal("accepted corrupt filter")
	}
}

func TestCheckNamesBlockBatch(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoMerge: true, BufferBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var names []NameID
	for i := range 1000 {
		name := fmt.Sprintf("prefix.metric.%06d", i)
		e, _, err := d.Append("g", 2, []byte(name), 0)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, NameID{"g", 2, name, e.ID})
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if d.snap.Load().levels[0].compact {
		t.Fatal("flush compressed a transient level")
	}
	if _, _, err := d.Append("g", 2, []byte("z.last"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.MergeAll(); err != nil {
		t.Fatal(err)
	}
	if !d.snap.Load().levels[0].compact {
		t.Fatal("merge did not compress")
	}
	// Reverse request order proves the operation sorts before decoding.
	for i, j := 0, len(names)-1; i < j; i, j = i+1, j-1 {
		names[i], names[j] = names[j], names[i]
	}
	decodes := 0
	testHook = func(p string) error {
		if p == "block-decode" {
			decodes++
		}
		return nil
	}
	defer func() { testHook = nil }()
	if err := d.CheckNames(names); err != nil {
		t.Fatal(err)
	}
	if decodes != 0 {
		t.Fatalf("decoded %d blocks for %d names", decodes, len(names))
	}
	// Newest tombstone/revival, absent IDs, wrong names and scope all fail.
	for _, mutation := range []string{"id", "name", "scope", "deleted", "revived"} {
		t.Run(mutation, func(t *testing.T) {
			bad := append([]NameID(nil), names...)
			switch mutation {
			case "id":
				bad[0].ID++
			case "name":
				bad[0].Name += ".wrong"
			case "scope":
				bad[0].VShard++
			case "deleted", "revived":
				if _, err := d.Delete("g", 2, []byte(bad[0].Name)); err != nil {
					t.Fatal(err)
				}
				if mutation == "revived" {
					d.Append("g", 2, []byte(bad[0].Name), 0)
					if err := d.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := d.CheckNames(bad); err == nil {
				t.Fatal("accepted changed name/ID")
			}
		})
	}
	// A negative filter probe cannot decode even if the candidate block is corrupt.
	l := d.snap.Load().levels[0]
	key := append(scopeKey(nil, "g", 2), []byte("prefix.metric.never")...)
	for l.mayContain(key) {
		key = append(key, 'x')
	}
	before := decodes
	if _, ok := l.get(key); ok {
		t.Fatal("found miss")
	}
	if decodes != before {
		t.Fatal("miss decoded a block")
	}
	// Assert little-endian Bloom extent remains bounded by the mapped file.
	if binary.LittleEndian.Uint64(l.m[len(l.m)-footerSize+16:]) != uint64(l.n) {
		t.Fatal("entry footer changed")
	}
}
