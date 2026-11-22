package v2dict

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"testing"
)

// TestLevelGetPrefixes checks level lookups, which compare entries through
// their shared prefixes without decoding keys, against a sorted model:
// present names, their proper prefixes and extensions, and neighbours one
// byte off, with names that share long prefixes and outgrow keyBuf.
func TestLevelGetPrefixes(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 9))
	long := bytes.Repeat([]byte("x"), keyBuf)
	var names [][]byte
	for i := range 400 {
		var n []byte
		switch i % 5 {
		case 0:
			n = fmt.Appendf(nil, "a.b%d", rng.IntN(50))
		case 1:
			n = fmt.Appendf(nil, "a.b%d.c%d", rng.IntN(50), rng.IntN(9))
		case 2:
			n = append(fmt.Appendf(nil, "%s.", long[:200+rng.IntN(200)]), byte('a'+rng.IntN(3)))
		case 3:
			n = fmt.Appendf(nil, "%s%d", long, rng.IntN(20))
		default:
			n = fmt.Appendf(nil, "z%d", rng.IntN(1000))
		}
		names = append(names, n)
	}
	d, err := Open(t.TempDir(), Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	want := map[string]uint32{}
	for i, n := range names {
		e, _, err := d.Append("g", 1, n, 0)
		if err != nil {
			t.Fatal(err)
		}
		want[string(n)] = e.ID
		if i%97 == 0 {
			if err := d.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.MergeAll(); err != nil {
		t.Fatal(err)
	}
	var probes [][]byte
	for n := range want {
		b := []byte(n)
		probes = append(probes, b, b[:len(b)-1], b[:len(b)/2], append(slices.Clone(b), 0), append(slices.Clone(b), '~'))
		up, down := slices.Clone(b), slices.Clone(b)
		up[len(up)-1]++
		down[len(down)-1]--
		probes = append(probes, up, down)
	}
	probes = append(probes, []byte{}, []byte("a"), []byte("{"))
	for _, p := range probes {
		e, ok := d.Lookup("g", 1, p)
		id, in := want[string(p)]
		if ok != in || ok && e.ID != id {
			t.Fatalf("Lookup(%.40q... len %d) = %+v %v, want %d %v", p, len(p), e, ok, id, in)
		}
	}
	if s := d.snap.Load(); len(s.levels) != 1 || s.frozen != nil || d.bufSize.Load() != 0 {
		t.Fatalf("want one level and nothing above it, have %d levels", len(s.levels))
	}
}

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
