// Package namefp is the 96-bit name fingerprint and the pointer-free table that
// owners and receivers use instead of name-keyed maps. A table maps a
// fingerprint to a 32-bit local number and never reads name bytes, so it
// retains no name.
//
// Identity is the fingerprint: a process-seeded maphash64 plus the high 32 bits
// of the FNV-1a64 routing hash. At 263M names the chance of any two names
// sharing a fingerprint is about n²/2^97 ≈ 4e-13 per process lifetime; 64 bits
// alone would be 2e-3, which is why both halves are kept.
package namefp

import (
	"hash/maphash"
	"math/bits"
)

var seed = maphash.MakeSeed()

// Hash is the process-seeded 64-bit half of a fingerprint.
func Hash(name string) uint64 { return maphash.String(seed, name) }

// Route is FNV-1a64 over the name bytes, the hash TCP intake routes by. Its high
// 32 bits are the second half of a fingerprint.
func Route[T ~string | ~[]byte](name T) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= 1099511628211
	}
	return h
}

// Fingerprint identifies a name within one process.
type Fingerprint struct {
	Hash  uint64 // Hash(name)
	Route uint32 // Route(name) >> 32
}

// Of builds a fingerprint from the two hashes a caller already holds.
func Of(hash, route uint64) Fingerprint { return Fingerprint{Hash: hash, Route: uint32(route >> 32)} }

// Name fingerprints name bytes directly.
func Name(name string) Fingerprint { return Of(Hash(name), Route(name)) }

// slot is 16 B with no pointers. key is Hash|1, so 0 marks an empty slot; val
// is Route<<32 | local. The bit forced into key leaves 95 independent bits.
type slot struct {
	key, val uint64
}

// Table is an open-addressing map from fingerprint to a 32-bit local number,
// linear probing at load at most 3/4. It is not safe for concurrent use.
type Table struct {
	slots []slot
	used  int
}

func (t *Table) home(key uint64) uint64 {
	// Callers may shard names by low hash bits, so index by the high bits of a
	// Fibonacci multiply.
	return (key * 0x9e3779b97f4a7c15) >> (64 - bits.Len64(uint64(len(t.slots)-1)))
}

// Get returns fp's local number.
func (t *Table) Get(fp Fingerprint) (uint32, bool) {
	if t.used == 0 {
		return 0, false
	}
	key, route := fp.Hash|1, uint64(fp.Route)<<32
	mask := uint64(len(t.slots) - 1)
	for i := t.home(key); ; i = (i + 1) & mask {
		s := t.slots[i]
		if s.key == 0 {
			return 0, false
		}
		if s.key == key && s.val&^0xffffffff == route {
			return uint32(s.val), true
		}
	}
}

// Put sets fp's local number, replacing any previous one.
func (t *Table) Put(fp Fingerprint, local uint32) {
	if (t.used+1)*4 > len(t.slots)*3 {
		t.grow()
	}
	key, val := fp.Hash|1, uint64(fp.Route)<<32|uint64(local)
	mask := uint64(len(t.slots) - 1)
	for i := t.home(key); ; i = (i + 1) & mask {
		s := &t.slots[i]
		if s.key == 0 {
			*s = slot{key, val}
			t.used++
			return
		}
		if s.key == key && s.val>>32 == val>>32 {
			s.val = val
			return
		}
	}
}

func (t *Table) grow() {
	old := t.slots
	t.slots = make([]slot, max(64, 2*len(old)))
	mask := uint64(len(t.slots) - 1)
	for _, s := range old {
		if s.key == 0 {
			continue
		}
		i := t.home(s.key)
		for t.slots[i].key != 0 {
			i = (i + 1) & mask
		}
		t.slots[i] = s
	}
}

// Len is the number of fingerprints held.
func (t *Table) Len() int { return t.used }

// Reset empties the table and keeps its storage.
func (t *Table) Reset() {
	clear(t.slots)
	t.used = 0
}
