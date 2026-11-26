package v2core

import (
	"encoding/binary"
	"testing"
)

// TestL0WireIDCompact: the L0 wire form round-trips NumericIDs and packed
// IDs at every field edge, takes 5 uvarint bytes for a packed ID in the
// first 8 groups with up to 2^15 names per vshard (the packed ID takes 8),
// and l0ID refuses values l0WireID never writes.
func TestL0WireIDCompact(t *testing.T) {
	ids := []uint64{1, 2, 1 << 31, 1<<62 + 5}
	for _, g := range []uint16{0, 1, 7, 8, maxPackedGroups - 1} {
		for _, v := range []uint16{0, 1, 4095} {
			for _, d := range []uint32{0, 1, 255, 1<<15 - 1, 1 << 31, 1<<32 - 1} {
				ids = append(ids, PackID(g, v, d))
			}
		}
	}
	for _, id := range ids {
		got, ok := l0ID(l0WireID(id))
		if !ok || got != id {
			t.Fatalf("%#x -> wire %#x -> %#x ok=%v", id, l0WireID(id), got, ok)
		}
	}
	var buf [binary.MaxVarintLen64]byte
	for _, g := range []uint16{1, 7} {
		if n := binary.PutUvarint(buf[:], l0WireID(PackID(g, 4095, 1<<15-1))); n != 5 {
			t.Fatalf("group %d: packed ID takes %d wire bytes, want 5", g, n)
		}
	}
	for _, w := range []uint64{1 << 49, 1<<13 | 1<<61 | 1} {
		if _, ok := l0ID(w | 1); ok {
			t.Fatalf("wire %#x accepted", w|1)
		}
	}
}
