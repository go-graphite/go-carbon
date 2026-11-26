//go:build !race

package v2dict

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
)

// Allocation budgets describe the ordinary data path. Race instrumentation
// deliberately drops one quarter of sync.Pool buffers, so it forces allocations
// unrelated to the codec's steady-state cost. All lookup correctness and
// concurrency tests still run with -race.

// TestLookupNoAllocs pins the lookups of names held in level files,
// including names longer than 256 bytes, to zero allocations.
func TestLookupNoAllocs(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	long := bytes.Repeat([]byte("n"), 300)
	name := func(i int) []byte { return fmt.Appendf(nil, "%s.%d", long, i) }
	for i := range 3000 {
		if _, _, err := d.Append("g", 2, name(i), 0); err != nil {
			t.Fatal(err)
		}
		if i%1000 == 999 {
			if err := d.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	dst := make([]byte, 0, 512)
	i := 0
	names := make([][]byte, 3000)
	for i := range names {
		names[i] = name(i)
	}
	for _, c := range []struct {
		what string
		f    func()
	}{
		{"Append", func() { i = (i + 7) % 3000; d.Append("g", 2, names[i], 0) }},
		{"Lookup", func() { i = (i + 7) % 3000; d.Lookup("g", 2, names[i]) }},
		{"LookupID", func() { i = (i + 7) % 3000; d.LookupID("g", 2, uint32(i), dst[:0]) }},
	} {
		if n := testing.AllocsPerRun(1000, c.f); n != 0 {
			t.Errorf("%s of a stored name: %v allocations per call", c.what, n)
		}
	}
}

// TestNewNameAllocBytes bounds what a new name allocates once append levels
// cycle: its record in the arena, its share of tables sized up front and
// of the level file write (about 100 B, in the background flush), and
// nothing that grows per lookup or copies.
func TestNewNameAllocBytes(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	long := bytes.Repeat([]byte("m"), 280)
	names := make([][]byte, 150000)
	keyBytes := 0
	for i := range names {
		names[i] = fmt.Appendf(nil, "%s.%d", long, i)
		keyBytes += len(names[i]) + 4
	}
	for _, n := range names[:40000] { // the first append level grows; later ones are sized
		if _, _, err := d.Append("g", 3, n, 0); err != nil {
			t.Fatal(err)
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for _, n := range names[40000:] {
		if _, _, err := d.Append("g", 3, n, 0); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	perName := float64(after.TotalAlloc-before.TotalAlloc) / float64(len(names)-40000)
	key := float64(keyBytes) / float64(len(names))
	t.Logf("%.0f B allocated per new name (key %.0f B)", perName, key)
	if perName > key+192 {
		t.Fatalf("%.0f B allocated per new name, want at most its %.0f-byte key + 192", perName, key)
	}
}
