package v2dict

import (
	"bytes"
	"encoding/binary"
	"hash/maphash"
	"slices"
)

// memtable is the append level: the newest entries since the last flush, in
// one byte arena (u16 key length, key, value) with two open-addressing
// tables of record numbers, by key and by (scope, ID). It holds no per-name
// Go objects; its size is bounded by Options.BufferBytes. A key appears
// once: a later put of the same key (a flag change, a revival) rewrites the
// value in place.
type memtable struct {
	arena  []byte
	recs   []uint32 // arena offset of each record
	byKey  []uint32 // record number + 1; 0 is empty
	byID   []uint32
	idUsed int
	sorted []uint32 // record numbers in key order; valid unless dirty
	dirty  bool
}

var seed = maphash.MakeSeed()

func idHash(sk []byte, id uint32) uint64 {
	h := maphash.Bytes(seed, sk) ^ uint64(id)*0x9e3779b97f4a7c15
	return h ^ h>>29
}

func (t *memtable) rec(r uint32) (key, val []byte) {
	off := t.recs[r]
	kl := int(binary.LittleEndian.Uint16(t.arena[off:]))
	key = t.arena[off+2 : int(off)+2+kl]
	return key, t.arena[int(off)+2+kl : int(off)+2+kl+valueSize]
}

// bytes is the heap the append level holds.
func (t *memtable) bytes() int {
	return cap(t.arena) + 4*(cap(t.recs)+len(t.byKey)+len(t.byID)+cap(t.sorted))
}

// size is the flush measure: record bytes plus their share of the tables
// (four table slots of 4 bytes at the 1/2 load bound, and the record offset).
func (t *memtable) size() int { return len(t.arena) + 20*len(t.recs) }

// slot returns the byKey slot of key: its record, or the empty slot to use.
func (t *memtable) slot(key []byte) int {
	if len(t.byKey) == 0 {
		return -1
	}
	mask := len(t.byKey) - 1
	for i := int(maphash.Bytes(seed, key)) & mask; ; i = (i + 1) & mask {
		r := t.byKey[i]
		if r == 0 {
			return i
		}
		if k, _ := t.rec(r - 1); bytes.Equal(k, key) {
			return i
		}
	}
}

func (t *memtable) get(key []byte) []byte {
	if i := t.slot(key); i >= 0 && t.byKey[i] != 0 {
		_, v := t.rec(t.byKey[i] - 1)
		return v
	}
	return nil
}

// getID returns the key and value of the record holding (scope, id).
func (t *memtable) getID(sk []byte, id uint32) (key, val []byte) {
	if len(t.byID) == 0 {
		return nil, nil
	}
	mask := len(t.byID) - 1
	for i := int(idHash(sk, id)) & mask; ; i = (i + 1) & mask {
		r := t.byID[i]
		if r == 0 {
			return nil, nil
		}
		// A revival rewrites a record's ID in place; its old slot stays
		// and no longer matches.
		if k, v := t.rec(r - 1); binary.LittleEndian.Uint32(v) == id && bytes.Equal(k[:scopeLen(k)], sk) {
			return k, v
		}
	}
}

func (t *memtable) put(key, val []byte) {
	if 2*(len(t.recs)+1) > len(t.byKey) {
		t.byKey = t.rehash(t.byKey, 2*len(t.byKey), func(r uint32) uint64 { k, _ := t.rec(r); return maphash.Bytes(seed, k) })
	}
	if 2*(t.idUsed+1) > len(t.byID) {
		t.byID = t.rehash(t.byID, 2*len(t.byID), func(r uint32) uint64 {
			k, v := t.rec(r)
			return idHash(k[:scopeLen(k)], binary.LittleEndian.Uint32(v))
		})
		t.idUsed = len(t.recs)
	}
	i := t.slot(key)
	newID := true
	if r := t.byKey[i]; r != 0 {
		_, v := t.rec(r - 1)
		newID = binary.LittleEndian.Uint32(v) != binary.LittleEndian.Uint32(val)
		copy(v, val)
		if !newID {
			return
		}
		t.insertID(r)
		return
	}
	t.recs = append(t.recs, uint32(len(t.arena)))
	t.arena = binary.LittleEndian.AppendUint16(t.arena, uint16(len(key)))
	t.arena = append(t.arena, key...)
	t.arena = append(t.arena, val[:valueSize]...)
	r := uint32(len(t.recs))
	t.byKey[i] = r
	t.insertID(r)
	t.dirty = true
}

func (t *memtable) insertID(r uint32) {
	k, v := t.rec(r - 1)
	mask := len(t.byID) - 1
	i := int(idHash(k[:scopeLen(k)], binary.LittleEndian.Uint32(v))) & mask
	for t.byID[i] != 0 {
		i = (i + 1) & mask
	}
	t.byID[i] = r
	t.idUsed++
}

// rehash rebuilds a table at size n (at least 1024) from the current
// records, dropping stale ID slots.
func (t *memtable) rehash(_ []uint32, n int, hash func(r uint32) uint64) []uint32 {
	n = max(n, 1024)
	tab := make([]uint32, n)
	for r := range t.recs {
		i := int(hash(uint32(r))) & (n - 1)
		for tab[i] != 0 {
			i = (i + 1) & (n - 1)
		}
		tab[i] = uint32(r) + 1
	}
	return tab
}

// order returns the record numbers in key order.
func (t *memtable) order() []uint32 {
	if t.dirty || len(t.sorted) != len(t.recs) {
		t.sorted = t.sorted[:0]
		for r := range t.recs {
			t.sorted = append(t.sorted, uint32(r))
		}
		slices.SortFunc(t.sorted, func(a, b uint32) int {
			ka, _ := t.rec(a)
			kb, _ := t.rec(b)
			return bytes.Compare(ka, kb)
		})
		t.dirty = false
	}
	return t.sorted
}

// sortedRecs returns, in key order, the records of a memtable no one writes
// any more (a frozen append level) whose keys start with prefix, in a new
// slice: concurrent readers never share its sort state.
func (t *memtable) sortedRecs(prefix []byte) []uint32 {
	var out []uint32
	for r := range t.recs {
		if k, _ := t.rec(uint32(r)); bytes.HasPrefix(k, prefix) {
			out = append(out, uint32(r))
		}
	}
	slices.SortFunc(out, func(a, b uint32) int {
		ka, _ := t.rec(a)
		kb, _ := t.rec(b)
		return bytes.Compare(ka, kb)
	})
	return out
}
