package v2core

import (
	"testing"

	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// rowSlotCatalog places n packed identities the way publish does (page,
// slot, entry), spread over every vshard. ids[i] is name i's durable ID.
func rowSlotCatalog(tb testing.TB, n int) (*PolicyCatalog, []*PolicyIdentity) {
	c := packedTestCatalog(tb, tb.TempDir())
	g, err := c.GroupIndex("test")
	if err != nil {
		tb.Fatal(err)
	}
	hs := make([]*PolicyIdentity, n)
	for i := range hs {
		id := PackID(g, uint16(i%partsformat.VirtualShards), uint32(i/partsformat.VirtualShards))
		p := c.packedPage(id, true)
		h := &PolicyIdentity{id: id, slot: p.slot | uint32(id&255), catalog: c}
		p.ids[id&255].Store(h)
		hs[i] = h
	}
	return c, hs
}

// TestRowSlotDurable checks that page arithmetic gives every slot its
// identity's durable ID, as the identity-reading durableID does, and that
// IDs that are not slots pass through.
func TestRowSlotDurable(t *testing.T) {
	c, hs := rowSlotCatalog(t, 3*partsformat.VirtualShards*256+17)
	seen := map[uint32]bool{}
	for _, h := range hs {
		if h.slot < 256 || seen[h.slot] {
			t.Fatalf("slot %d of %#x: below 256 or repeated", h.slot, h.id)
		}
		seen[h.slot] = true
		if got := c.durableRow(uint64(h.slot)); got != h.id {
			t.Fatalf("durableRow(%d) = %#x, want %#x", h.slot, got, h.id)
		}
		if got := durableID(uint64(h.slot), c.rowLookup); got != h.id {
			t.Fatalf("durableID(%d) = %#x, want %#x", h.slot, got, h.id)
		}
		if c.rowLookup(uint64(h.slot)) != h || c.rowLookup(h.id) != h {
			t.Fatalf("rowLookup of %#x", h.id)
		}
	}
	for _, id := range []uint64{0, 1, 255, hs[0].id, 1 << 40} {
		if got := c.durableRow(id); got != id {
			t.Fatalf("durableRow(%#x) = %#x, want itself", id, got)
		}
	}
	if got := (&PolicyCatalog{}).durableRow(300); got != 300 {
		t.Fatalf("NumericID catalog durableRow(300) = %d", got)
	}
}

// BenchmarkRowSlotL0 is the owner's L0 encode per head row of one-point
// applications in random name order (the six-arm shape): numeric IDs (no
// translation, head), slots translated through the identity (durableID,
// box 4b) and slots translated by page (durableRow).
func BenchmarkRowSlotL0(b *testing.B) {
	const names, rowsPer = 1 << 21, 1 << 18
	c, hs := rowSlotCatalog(b, names)
	for _, arm := range []string{"numeric", "identity", "page"} {
		b.Run("ids="+arm, func(b *testing.B) {
			var h pendingHead
			var p pendingRows
			for i := range rowsPer {
				n := uint64(i) * 2654435761 % names
				id := n + 1
				if arm != "numeric" {
					id = uint64(hs[n].slot)
				}
				p.startApplication(uint64(i+1), 1, 1000, 1)
				p.addRow(id, 1000+int64(i%60), uint64(i), uint64(i+1))
			}
			p.closePacket()
			h.addRows(&p)
			lookup := c.rowLookup
			if arm == "numeric" {
				lookup = nil // a NumericID head row is its own durable ID
			}
			v := h.rawView(lookup)
			if arm == "page" {
				v.rows = c
			}
			var buf []byte
			for b.Loop() {
				buf = encodeL0(buf, v)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rowsPer), "ns/row")
			b.ReportMetric(float64(len(buf))/rowsPer, "L0B/row")
		})
	}
}
