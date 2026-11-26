package v2core

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

func TestPackedIDRoundTrip(t *testing.T) {
	for _, c := range []struct {
		g, v uint16
		d    uint32
	}{{0, 0, 0}, {1, 4095, 1<<32 - 1}, {maxPackedGroups - 1, 17, 12345}} {
		id := PackID(c.g, c.v, c.d)
		g, v, d := UnpackID(id)
		if !IsPackedID(id) || g != c.g || v != c.v || d != c.d || id == deadSlot {
			t.Fatalf("%+v -> %#x -> %d %d %d", c, id, g, v, d)
		}
	}
	// Every NumericID of a real store stays outside the packed space.
	if IsPackedID(1<<62) || IsPackedID(1) {
		t.Fatal("NumericID read as packed")
	}
}

func TestPolicyCatalogPackedIdentities(t *testing.T) {
	c := &PolicyCatalog{}
	g, err := c.GroupIndex("test")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := c.GroupIndex("test"); again != g {
		t.Fatal("group index not stable")
	}
	other, _ := c.GroupIndex("other")
	names := []string{"a.b.c", "a.b.d", "x.y", "deep.name.with.parts"}
	ids := make([]uint64, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		_, raw := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
		v := partsformat.Route(partsformat.ID(name))
		ids[i] = PackID(g, v, uint32(300*i+7)) // crosses 256-slot pages
		// The wrong vshard or group must be refused.
		if _, err := c.intern(PackID(g, (v+1)%partsformat.VirtualShards, 1), name, raw); err == nil {
			t.Fatal("accepted a packed ID of another vshard")
		}
		if _, err := c.intern(PackID(other, v, 1), name, raw); err == nil {
			t.Fatal("accepted a packed ID of another group")
		}
		wg.Go(func() {
			if _, err := c.intern(ids[i], name, raw); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for i, name := range names {
		h := c.lookup(ids[i])
		if h == nil || h.Name() != name || h.id != ids[i] {
			t.Fatalf("lookup %#x: %v", ids[i], h)
		}
	}
	// A NumericID and the packed space never share slots.
	_, raw := rollupTestPolicy(t, "numeric.name", "1s:1d", sealrollup.Last, .5)
	if _, err := c.intern(7, "numeric.name", raw); err != nil {
		t.Fatal(err)
	}
	if c.lookup(7).Name() != "numeric.name" || c.lookup(PackID(g, 0, 7)) != nil && c.lookup(PackID(g, 0, 7)).Name() == "numeric.name" {
		t.Fatal("packed and numeric spaces overlap")
	}
	if c.lookup(PackID(g, 1, 1<<20)) != nil || c.lookup(PackID(99, 1, 1)) != nil {
		t.Fatal("lookup of an unknown packed ID returned an identity")
	}
}

// TestPackedPagesHeap reports the page heap of N identity slots under
// NumericIDs (one dense directory) and packed IDs (dense per vshard), for
// names spread evenly over every vshard as Route spreads them.
// PACKED_PAGES_HEAP=<names> runs it.
func TestPackedPagesHeap(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("PACKED_PAGES_HEAP"))
	if n == 0 {
		t.Skip("set PACKED_PAGES_HEAP=<names>")
	}
	measure := func(fill func(c *PolicyCatalog)) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		c := &PolicyCatalog{}
		fill(c)
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(c)
		return after.HeapAlloc - before.HeapAlloc
	}
	numeric := measure(func(c *PolicyCatalog) {
		for id := uint64(1); id <= uint64(n); id++ {
			_ = &c.idPage(id>>10, true)[id&1023]
		}
	})
	packed := measure(func(c *PolicyCatalog) {
		g, _ := c.GroupIndex("test")
		for i := 0; i < n; i++ {
			v := uint16(i % partsformat.VirtualShards)
			_ = c.packedSlot(PackID(g, v, uint32(i/partsformat.VirtualShards)), true)
		}
	})
	t.Log(fmt.Sprintf("names=%d numeric=%d B (%.2f B/name) packed=%d B (%.2f B/name) delta=%+d B (%+.2f B/name)",
		n, numeric, float64(numeric)/float64(n), packed, float64(packed)/float64(n),
		int64(packed)-int64(numeric), (float64(packed)-float64(numeric))/float64(n)))
}

func BenchmarkPolicyCatalogLookup(b *testing.B) {
	const n = 1 << 20
	c := &PolicyCatalog{}
	g, _ := c.GroupIndex("test")
	h := &PolicyIdentity{}
	for id := uint64(1); id <= n; id++ {
		c.idPage(id>>10, true)[id&1023].Store(h)
		c.packedSlot(PackID(g, uint16(id%partsformat.VirtualShards), uint32(id/partsformat.VirtualShards)), true).Store(h)
	}
	// The same pseudo-random order for both: real lookups are not sequential.
	at := func(i int) uint64 { return uint64(i)*2654435761%n + 1 }
	b.Run("numeric", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			if c.lookup(at(i)) == nil {
				b.Fatal("miss")
			}
		}
	})
	b.Run("packed", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			id := at(i)
			if c.lookup(PackID(g, uint16(id%partsformat.VirtualShards), uint32(id/partsformat.VirtualShards))) == nil {
				b.Fatal("miss")
			}
		}
	})
}
