package v2dict

import (
	"bytes"
	"encoding/binary"
	"hash/maphash"
	"slices"
	"syscall"
)

// memtable is the append level: the newest entries since the last flush, in
// a byte arena (u16 key length, key, value) with two open-addressing tables
// of record numbers, by key and by (scope, ID). It holds no per-name Go
// objects; its size is bounded by Options.BufferBytes, except that it keeps
// filling while the previous one is flushed. The arena is anonymous mmap-backed 64 MiB
// chunks that never move, so it grows without copying itself. A key appears
// once: a later put of the same key (a flag change, a revival) rewrites the
// value in place.
type memtable struct {
	chunks [][]byte
	used   int      // record bytes in chunks
	recs   []uint32 // chunk<<chunkBits | offset of each record
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

// chunkBits sizes mmap chunks; a record (at most 2+65535+7 bytes) never spans two.
const chunkBits = 26

func (t *memtable) rec(r uint32) (key, val []byte) {
	off := t.recs[r]
	c := t.chunks[off>>chunkBits][off&(1<<chunkBits-1):]
	kl := int(binary.LittleEndian.Uint16(c))
	return c[2 : 2+kl], c[2+kl : 2+kl+valueSize]
}

// bytes is the memory reserved by the append level (heap and mmap).
func (t *memtable) bytes() int {
	n := 4 * (cap(t.recs) + len(t.byKey) + len(t.byID) + cap(t.sorted))
	for _, c := range t.chunks {
		n += cap(c)
	}
	return n
}

// size is the flush measure: record bytes plus their share of the tables
// (four table slots of 4 bytes at the 1/2 load bound, and the record offset).
func (t *memtable) size() int { return t.used + 20*len(t.recs) }

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

// next returns an empty append level to follow t, with tables and record
// offsets for as many records as t holds: one that fills as t did
// allocates only its arena chunks. t itself is never refilled: lock-free
// readers may still hold it once frozen.
func (t *memtable) next() memtable {
	slots := 1024
	for slots < 2*(len(t.recs)+1) {
		slots <<= 1
	}
	return memtable{chunks: make([][]byte, 0, len(t.chunks)), recs: make([]uint32, 0, len(t.recs)), byKey: make([]uint32, slots), byID: make([]uint32, slots)}
}

func (t *memtable) put(key, val []byte) error {
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
			return nil
		}
		t.insertID(r)
		return nil
	}
	n, last := 2+len(key)+valueSize, len(t.chunks)-1
	if last < 0 || len(t.chunks[last])+n > cap(t.chunks[last]) {
		c, err := syscall.Mmap(-1, 0, 1<<chunkBits, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			return err
		}
		t.chunks = append(t.chunks, c[:0])
		last++
	}
	c := t.chunks[last]
	t.recs = append(t.recs, uint32(last)<<chunkBits|uint32(len(c)))
	c = binary.LittleEndian.AppendUint16(c, uint16(len(key)))
	c = append(c, key...)
	t.chunks[last] = append(c, val[:valueSize]...)
	t.used += n
	r := uint32(len(t.recs))
	t.byKey[i] = r
	t.insertID(r)
	t.dirty = true
	return nil
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

// release runs only after snapshot replacement under d.mu, excluding every
// frozen reader, or under the shard lock for the active append level on Close.
func (t *memtable) release() error {
	var first error
	if len(t.chunks) > 0 {
		_ = hook("arena-unmap")
	}
	for _, c := range t.chunks {
		if err := syscall.Munmap(c[:cap(c)]); err != nil && first == nil {
			first = err
		}
	}
	*t = memtable{}
	return first
}
