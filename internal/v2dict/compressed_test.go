package v2dict

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Exercise unequal scope and block boundaries, misses between blocks, and seeks
// that advance into the next block. A separate L1 encoder is the merge oracle in
// TestCompactLevelDifferential.
func TestCompressedBlocks(t *testing.T) {
	for _, count := range []int{32, 128} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var keys [][]byte
			for vs := uint16(0); vs < 3; vs++ {
				for i := 0; i < 257; i++ {
					key := scopeKey(nil, "group", vs)
					key = fmt.Appendf(key, "prefix.%05d.%s", i*2, bytes.Repeat([]byte("suffix."), 40))
					keys = append(keys, key)
				}
			}
			l, err := writeLevel(filepath.Join(t.TempDir(), "blocks.lvl"), 1, 1, func(w *levelWriter) {
				w.blockN = count
				for i, k := range keys {
					w.add(k, (Entry{ID: uint32(i), Policy: uint16(i), Flags: uint8(i)}).put(nil))
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if l.blockN != count || l.nb != (len(keys)+count-1)/count {
				t.Fatal("block count not persisted")
			}
			for i, k := range keys {
				want := Entry{ID: uint32(i), Policy: uint16(i), Flags: uint8(i)}
				if got, ok := l.get(k); !ok || got != want {
					t.Fatalf("forward %d: %+v %v", i, got, ok)
				}
				gotKey, got := l.at(i, nil)
				if !bytes.Equal(gotKey, k) || got != want {
					t.Fatalf("reverse %d", i)
				}
				it := levelIter{l: l}
				if !it.seekGE(k) || !bytes.Equal(it.key, k) || decode(it.val) != want {
					t.Fatalf("seek %d", i)
				}
				missing := bytes.Clone(k)
				missing[len(scopeKey(nil, "group", 0))+len("prefix.")+4]++
				if _, ok := l.get(missing); ok {
					t.Fatalf("missing key %d found", i)
				}
				if i%257 != 256 {
					it = levelIter{l: l}
					if !it.seekGE(missing) || !bytes.Equal(it.key, keys[i+1]) {
						t.Fatalf("gap seek %d", i)
					}
				}
			}
			// Destroy a different block's frame. The mapped first-key index must still
			// allow a lookup of the final block without decoding the destroyed frame.
			f, err := os.OpenFile(l.path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			offset := l.block(0) + 1
			// The frame is immediately after first key + its two framing varints.
			offset += len(l.first(0))
			// Find it by its standard Zstandard magic (not an implementation decoder).
			frame := bytes.Index(l.m[offset:l.block(1)], []byte{0x28, 0xb5, 0x2f, 0xfd})
			if frame < 0 {
				t.Fatal("missing compressed frame")
			}
			if _, err = f.WriteAt([]byte{0}, int64(offset+frame)); err != nil {
				t.Fatal(err)
			}
			f.Close()
			if _, ok := l.get(keys[len(keys)-1]); !ok {
				t.Fatal("lookup decoded unrelated block")
			}
		})
	}
}

func TestCompressedIndexValidation(t *testing.T) {
	l, err := writeLevel(filepath.Join(t.TempDir(), "valid.lvl"), 1, 1, func(w *levelWriter) {
		for i := 0; i < 100; i++ {
			w.add(append(scopeKey(nil, "g", 1), fmt.Appendf(nil, "n%03d", i)...), (Entry{ID: uint32(i)}).put(nil))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"zero block count", "oversize block count", "first offset", "last offset"} {
		t.Run(mutation, func(t *testing.T) {
			data := bytes.Clone(l.m)
			switch mutation {
			case "zero block count":
				binary.LittleEndian.PutUint32(data, 0)
			case "oversize block count":
				binary.LittleEndian.PutUint32(data, 4097)
			case "first offset":
				binary.LittleEndian.PutUint64(data[l.dataLen:], 0)
			case "last offset":
				binary.LittleEndian.PutUint64(data[l.dataLen+8*(l.nb-1):], uint64(len(data)))
			}
			path := filepath.Join(t.TempDir(), "bad.lvl")
			if err := os.WriteFile(path, data, 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := openLevel(path, 1, 1); !errors.Is(err, errCorrupt) {
				t.Fatalf("accepted %s: %v", mutation, err)
			}
		})
	}
}

// Compressed frame headers are validated only when read. Opening a level must
// not scan the compressed data pages just to validate otherwise cold names.
func TestCompressedHeaderValidatedOnAccess(t *testing.T) {
	l, err := writeLevel(filepath.Join(t.TempDir(), "header.lvl"), 1, 1, func(w *levelWriter) { w.add(append(scopeKey(nil, "g", 1), "name"...), (Entry{ID: 1}).put(nil)) })
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Clone(l.m)
	data[l.block(0)] = 1
	path := filepath.Join(t.TempDir(), "bad-header.lvl")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	bad, err := openLevel(path, 1, 1)
	if err != nil {
		t.Fatalf("read cold data during open: %v", err)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("accepted corrupt accessed header")
		} else if e, ok := r.(error); !ok || !errors.Is(e, errCorrupt) {
			t.Fatalf("wrong panic: %v", r)
		}
	}()
	bad.at(0, nil)
}
