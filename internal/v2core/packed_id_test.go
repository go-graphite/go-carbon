package v2core

import (
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
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

// packedTestCatalog returns a catalog with a store dictionary at dir.
func packedTestCatalog(t testing.TB, dir string) *PolicyCatalog {
	t.Helper()
	d, err := v2dict.Open(dir, v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	c := &PolicyCatalog{}
	c.dict.Store(d)
	return c
}

func TestPolicyCatalogPackedIdentities(t *testing.T) {
	if _, err := (&PolicyCatalog{}).GroupIndex("test"); err == nil {
		t.Fatal("packed group index without a dictionary")
	}
	dir := t.TempDir()
	c := packedTestCatalog(t, dir)
	g, err := c.GroupIndex("test")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := c.GroupIndex("test"); again != g {
		t.Fatal("group index not stable")
	}
	other, _ := c.GroupIndex("other")
	if g == 0 || other == g {
		t.Fatalf("group indexes %d %d", g, other)
	}
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
	// Names of one vshard across 256-slot page boundaries keep their slots.
	var same []string
	want := partsformat.Route("same.vshard.0")
	for i := 0; len(same) < 6; i++ {
		if name := fmt.Sprintf("same.vshard.%d", i); partsformat.Route(partsformat.ID(name)) == want {
			same = append(same, name)
		}
	}
	for i, d := range []uint32{0, 255, 256, 511, 512, 70000} {
		_, raw := rollupTestPolicy(t, same[i], "1s:1d", sealrollup.Last, .5)
		if _, err := c.intern(PackID(g, want, d), same[i], raw); err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range []uint32{0, 255, 256, 511, 512, 70000} {
		if h := c.lookup(PackID(g, want, d)); h == nil || h.Name() != same[i] {
			t.Fatalf("dict ID %d: page slot aliased (%v)", d, h)
		}
	}
	// A store with a dictionary refuses NumericIDs, so the dense directory
	// holds only packed identities' row slots: each maps back to its handle.
	_, raw := rollupTestPolicy(t, "numeric.name", "1s:1d", sealrollup.Last, .5)
	if _, err := c.intern(7, "numeric.name", raw); !errors.Is(err, errNumericWithDict) {
		t.Fatalf("NumericID with a dictionary: %v, want refusal", err)
	}
	for _, d := range []uint32{0, 255, 256, 511, 512, 70000} {
		h := c.lookup(PackID(g, want, d))
		if r := h.rowID(); r == 0 || r > math.MaxUint32 || c.rowLookup(r) != h || c.lookup(r) != nil || durableID(r, c.rowLookup) != h.id {
			t.Fatalf("dict ID %d: row slot %d does not map back", d, r)
		}
	}
	if c.lookup(PackID(g, 1, 1<<20)) != nil || c.lookup(PackID(99, 1, 1)) != nil {
		t.Fatal("lookup of an unknown packed ID returned an identity")
	}
	// Group indexes are durable: a later process of the store packs the
	// same IDs, even when it meets the groups in another order.
	c.partDict().Close()
	again := packedTestCatalog(t, dir)
	if o, _ := again.GroupIndex("other"); o != other {
		t.Fatalf("reopened group index %d, want %d", o, other)
	}
	if label, ok := again.packedGroup(PackID(g, 0, 0)); !ok || label != "test" {
		t.Fatalf("reopened label of %d: %q %v", g, label, ok)
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
		c := packedTestCatalog(t, t.TempDir())
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
	c := packedTestCatalog(b, b.TempDir())
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

// TestPackedNameLifecycle seals packed names through expiry, revival and a
// restart. A name written in the expiring seal stays live (the expiry pass
// matches the owner's durable ID against row slots), the idle name expires,
// the revived name seals under the packed ID its writer admitted, and the
// reopened store finds the same names and renders the same records.
func TestPackedNameLifecycle(t *testing.T) {
	saved := partsformat.IDKeys.Swap(true)
	defer partsformat.IDKeys.Store(saved)
	root := t.TempDir()
	var clock atomic.Int64
	var expireOn atomic.Bool
	clock.Store(2000)
	hooks := Hooks{
		IDKeys:    true,
		Now:       func() time.Time { return time.Unix(clock.Load(), 0) },
		Expire:    func(RollupPolicy, RollupMetadata, time.Time) bool { return expireOn.Load() },
		ExpiryTTL: func(RollupPolicy, time.Time) (time.Duration, time.Time) { return time.Second, time.Time{} },
	}
	open := func() *Engine {
		e, err := Open(root, adoptConfig(), hooks)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	write := func(e *Engine, order uint64, names ...string) map[string]uint64 {
		ids := map[string]uint64{}
		var batches []Batch
		for i, name := range names {
			_, raw := rollupTestPolicy(t, name, "1s:120,10s:1h", sealrollup.Last, .5)
			id := writerID(t, e.identities, 0, raw)
			if !IsPackedID(id) {
				t.Fatalf("%s admitted under %#x, not a packed ID", name, id)
			}
			ids[name] = id
			row := Row{ID: id, Time: 1000 + int64(order), Bits: math.Float64bits(float64(order*10) + float64(i)), Seq: order*8 + uint64(i)}
			batches = append(batches, Batch{ID: id, Order: order*8 + uint64(i), Clock: 1010, Policy: raw, Points: []Row{row}})
		}
		if err := e.AppendBatches(0, batches, order*8+uint64(len(names)), true); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	type state struct {
		meta    RollupMetadata
		records []sealrollup.Record
	}
	read := func(e *Engine) map[string]state {
		snapshot := e.DBs()[0].NewSnapshot()
		defer snapshot.Close()
		out := map[string]state{}
		if err := scanRollups(snapshot, e.identities, func(p RollupPolicy, m RollupMetadata, r []sealrollup.Record) error {
			out[string(p.Name)] = state{m, slices.Clone(r)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	e := open()
	ids := write(e, 1, "life.idle", "life.busy")
	clock.Store(2010)
	expireOn.Store(true)
	if got := write(e, 2, "life.busy"); got["life.busy"] != ids["life.busy"] {
		t.Fatalf("life.busy re-admitted under %#x, want %#x", got["life.busy"], ids["life.busy"])
	}
	s := read(e)
	if busy, ok := s["life.busy"]; !ok || busy.meta.Expired || busy.meta.ID != ids["life.busy"] {
		t.Fatalf("written name expired or lost its ID: ok=%v %+v", ok, busy.meta)
	}
	if idle, ok := s["life.idle"]; ok && !idle.meta.Expired {
		t.Fatalf("idle name did not expire: %+v", idle.meta)
	}
	expireOn.Store(false)
	clock.Store(2020)
	revived := write(e, 3, "life.idle")
	s = read(e)
	if idle, ok := s["life.idle"]; !ok || idle.meta.Expired || idle.meta.ID != revived["life.idle"] {
		t.Fatalf("revived name: ok=%v %+v, want ID %#x", ok, idle.meta, revived["life.idle"])
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = open()
	defer e.Close()
	if after := read(e); !reflect.DeepEqual(after, s) {
		t.Fatalf("restart changed names or records:\nbefore %+v\nafter  %+v", s, after)
	}
	if got := write(e, 4, "life.idle", "life.busy"); got["life.idle"] != revived["life.idle"] || got["life.busy"] != ids["life.busy"] {
		t.Fatalf("restart re-admitted %v, want idle %#x busy %#x", got, revived["life.idle"], ids["life.busy"])
	}
}
