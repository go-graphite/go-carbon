package v2dict

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// legacyLevelBytes is an independent L1 encoder, kept as a compatibility oracle.
func legacyLevelBytes(keys [][]byte, entries []Entry) []byte {
	var data, idcol, stab, prev []byte
	var blocks []uint64
	for i, key := range keys {
		shared := 0
		if i%blockEntries == 0 {
			blocks = append(blocks, uint64(len(data)))
		} else {
			shared = sharedPrefix(prev, key)
		}
		data = binary.AppendUvarint(data, uint64(shared))
		data = binary.AppendUvarint(data, uint64(len(key)-shared))
		data = append(data, key[shared:]...)
		data = entries[i].put(data)
		prev = key
	}
	for i := 0; i < len(keys); {
		j := i + 1
		sk := keys[i][:scopeLen(keys[i])]
		for j < len(keys) && bytes.HasPrefix(keys[j], sk) {
			j++
		}
		var pairs []uint64
		var maxID uint32
		for k := i; k < j; k++ {
			pairs = append(pairs, uint64(entries[k].ID)<<32|uint64(k))
			maxID = max(maxID, entries[k].ID)
		}
		slices.Sort(pairs)
		for _, p := range pairs {
			idcol = binary.LittleEndian.AppendUint32(idcol, uint32(p>>32))
			idcol = binary.LittleEndian.AppendUint32(idcol, uint32(p))
		}
		stab = binary.LittleEndian.AppendUint16(stab, uint16(len(sk)))
		stab = append(stab, sk...)
		for _, v := range []uint32{uint32(i), uint32(j - i), maxID} {
			stab = binary.LittleEndian.AppendUint32(stab, v)
		}
		i = j
	}
	dataLen := len(data)
	for _, b := range blocks {
		data = binary.LittleEndian.AppendUint64(data, b)
	}
	data = append(data, idcol...)
	data = append(data, stab...)
	for _, v := range []int{dataLen, len(blocks), len(keys), len(stab)} {
		data = binary.LittleEndian.AppendUint64(data, uint64(v))
	}
	return append(data, legacyLevelMagic...)
}

// Both codecs must return the same full entries and name bytes, including sparse
// IDs, nonzero metadata and block boundaries; reopening and merging uses L1+L2.
func TestCompactLevelDifferential(t *testing.T) {
	var keys [][]byte
	var entries []Entry
	for vs := uint16(0); vs < 4; vs++ {
		for i := 0; i < 129; i++ {
			keys = append(keys, append(scopeKey(nil, "group", vs), []byte(fmt.Sprintf("prefix.metric.%04d", i))...))
			id := uint32(1024 + (128 - i))
			if vs&1 != 0 {
				id *= 3
			}
			e := Entry{ID: id}
			if i%3 == 0 {
				e.Policy = uint16(i * 511)
				e.Flags = uint8(i)
			}
			if i == 128 && vs == 3 {
				e = Entry{ID: NoID - 1, Policy: 65535, Flags: 255}
			}
			entries = append(entries, e)
		}
	}
	legacy := legacyLevelBytes(keys, entries)
	dir := t.TempDir()
	if err := os.WriteFile(levelPath(dir, 1, 1), legacy, 0644); err != nil {
		t.Fatal(err)
	}
	l1, err := openLevel(levelPath(dir, 1, 1), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := writeLevel(filepath.Join(t.TempDir(), "compact.lvl"), 1, 1, func(w *levelWriter) {
		for i, key := range keys {
			w.add(key, entries[i].put(nil))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !l2.compact || l1.compact || len(l2.m)-8*l2.n >= len(l1.m) {
		t.Fatalf("encoding/size (L2 includes 8B/name validation column): L1=%d L2=%d", len(l1.m), len(l2.m))
	}
	for _, l := range []*level{l1, l2} {
		for i, key := range keys {
			if e, ok := l.get(key); !ok || e != entries[i] {
				t.Fatalf("get %d: %+v %v want %+v", i, e, ok, entries[i])
			}
			pos := l.position(key[:scopeLen(key)], entries[i].ID)
			if pos != i {
				t.Fatalf("reverse position %d: got %d", i, pos)
			}
			k, e := l.at(pos, nil)
			if !bytes.Equal(k, key) || e != entries[i] {
				t.Fatalf("reverse bytes %d", i)
			}
		}
		it := levelIter{l: l}
		for i := 0; it.next(); i++ {
			if !bytes.Equal(it.key, keys[i]) || decode(it.val) != entries[i] {
				t.Fatalf("iterator %d", i)
			}
		}
	}
	d, err := Open(dir, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.Append("group", 0, []byte("new.metric"), 0); err != nil {
		t.Fatal(err)
	}
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = d.MergeAll(); err != nil {
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
	// Prefix expansion must return exactly the same sorted names and entries
	// after mixed-version compaction and restart, including nonzero metadata.
	for vs := uint16(0); vs < 4; vs++ {
		var got []string
		d.Scan("group", vs, []byte("prefix.metric.00"), func(name []byte, e Entry) bool {
			got = append(got, string(name))
			return true
		})
		var want []string
		for _, key := range keys {
			if bytes.HasPrefix(key, scopeKey(nil, "group", vs)) && bytes.HasPrefix(key[scopeLen(key):], []byte("prefix.metric.00")) {
				want = append(want, string(key[scopeLen(key):]))
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("prefix expansion vshard %d differs", vs)
		}
	}
	for i, key := range keys {
		vs := binary.BigEndian.Uint16(key[scopeLen(key)-2:])
		name := key[scopeLen(key):]
		if e, ok := d.Lookup("group", vs, name); !ok || e != entries[i] {
			t.Fatalf("reopen forward %d", i)
		}
		got, e, ok := d.LookupID("group", vs, entries[i].ID, nil)
		if !ok || !bytes.Equal(got, name) || e != entries[i] {
			t.Fatalf("reopen reverse %d", i)
		}
	}
}

func TestCompactDenseIDBoundaries(t *testing.T) {
	l, err := writeLevel(filepath.Join(t.TempDir(), "dense.lvl"), 1, 1, func(w *levelWriter) {
		for i := 0; i < 64; i++ {
			key := append(scopeKey(nil, "g", 2), []byte(fmt.Sprintf("n%02d", i))...)
			w.add(key, (Entry{ID: uint32(100 + 63 - i)}).put(nil))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(l.scopes) != 1 || l.scopes[0].baseID != 100 {
		t.Fatal("dense IDs not represented by one base")
	}
	sk := scopeKey(nil, "g", 2)
	for _, id := range []uint32{0, 99, 164, NoID} {
		if l.position(sk, id) != -1 {
			t.Fatalf("found out-of-range ID %d", id)
		}
	}
}
