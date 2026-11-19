package namefp

import (
	"fmt"
	"testing"
)

func TestTableRoundTripThroughGrowth(t *testing.T) {
	var tab Table
	const n = 100_000
	for i := 0; i < n; i++ {
		tab.Put(Name(fmt.Sprintf("a.b.c.host-%06d.cpu", i)), uint32(i))
	}
	if tab.Len() != n {
		t.Fatalf("Len = %d, want %d", tab.Len(), n)
	}
	for i := 0; i < n; i++ {
		if got, ok := tab.Get(Name(fmt.Sprintf("a.b.c.host-%06d.cpu", i))); !ok || got != uint32(i) {
			t.Fatalf("Get(%d) = %d, %v", i, got, ok)
		}
	}
	if _, ok := tab.Get(Name("a.b.c.host-missing.cpu")); ok {
		t.Fatal("absent name found")
	}
	tab.Put(Name("a.b.c.host-000007.cpu"), 1<<31)
	if got, _ := tab.Get(Name("a.b.c.host-000007.cpu")); got != 1<<31 || tab.Len() != n {
		t.Fatalf("replace: got %d, Len %d", got, tab.Len())
	}
	tab.Reset()
	if _, ok := tab.Get(Name("a.b.c.host-000001.cpu")); ok || tab.Len() != 0 {
		t.Fatal("Reset kept entries")
	}
}

// TestTableSeparatesEqualHashes forces names whose 64-bit halves are equal:
// only the Route half keeps them apart. Dropping the Route comparison from Get
// or Put fails it.
func TestTableSeparatesEqualHashes(t *testing.T) {
	var tab Table
	const n = 300 // enough to cross several growth steps with one probe run
	for i := 0; i < n; i++ {
		tab.Put(Fingerprint{Hash: 0xdeadbeef00, Route: uint32(i)}, uint32(1000+i))
	}
	if tab.Len() != n {
		t.Fatalf("Len = %d, want %d: equal hashes merged", tab.Len(), n)
	}
	for i := 0; i < n; i++ {
		if got, ok := tab.Get(Fingerprint{Hash: 0xdeadbeef00, Route: uint32(i)}); !ok || got != uint32(1000+i) {
			t.Fatalf("Get(route %d) = %d, %v", i, got, ok)
		}
	}
	if _, ok := tab.Get(Fingerprint{Hash: 0xdeadbeef00, Route: n + 1}); ok {
		t.Fatal("equal hash with an unseen route found")
	}
	// Equal Route, different Hash: the 64-bit half separates them.
	var other Table
	other.Put(Fingerprint{Hash: 2, Route: 7}, 1)
	if _, ok := other.Get(Fingerprint{Hash: 4, Route: 7}); ok {
		t.Fatal("equal route with another hash found")
	}
}

// TestTableKeepsAll96Bits checks that no fingerprint bit is spent on the empty
// marker: hashes that differ only in bit 0 stay apart, and the all-zero
// fingerprint holding local 0 is found. Forcing Hash|1 into the key, or
// storing local without the +1 offset, fails it.
func TestTableKeepsAll96Bits(t *testing.T) {
	var tab Table
	tab.Put(Fingerprint{Hash: 6, Route: 9}, 1)
	tab.Put(Fingerprint{Hash: 7, Route: 9}, 2)
	tab.Put(Fingerprint{}, 0)
	tab.Put(Fingerprint{Hash: 1}, MaxLocal)
	if tab.Len() != 4 {
		t.Fatalf("Len = %d, want 4", tab.Len())
	}
	for _, c := range []struct {
		fp    Fingerprint
		local uint32
	}{{Fingerprint{Hash: 6, Route: 9}, 1}, {Fingerprint{Hash: 7, Route: 9}, 2}, {Fingerprint{}, 0}, {Fingerprint{Hash: 1}, MaxLocal}} {
		if got, ok := tab.Get(c.fp); !ok || got != c.local {
			t.Fatalf("Get(%+v) = %d, %v, want %d", c.fp, got, ok, c.local)
		}
	}
}

// TestFingerprintHalvesIndependent checks that real names spread both halves:
// no two of 200k structured names share a 64-bit Hash or a full fingerprint.
func TestFingerprintHalvesIndependent(t *testing.T) {
	seen := make(map[Fingerprint]struct{}, 200_000)
	hashes := make(map[uint64]struct{}, 200_000)
	for i := 0; i < 200_000; i++ {
		fp := Name(fmt.Sprintf("containers.k8s.cluster-%02d.ns-%03d.pod-%06x.cpu", i%7, i%311, i))
		if _, dup := hashes[fp.Hash]; dup {
			t.Fatalf("64-bit hash collision at %d", i)
		}
		hashes[fp.Hash] = struct{}{}
		seen[fp] = struct{}{}
	}
	if len(seen) != 200_000 {
		t.Fatalf("fingerprints collided: %d distinct", len(seen))
	}
}

func BenchmarkTableGet(b *testing.B) {
	for _, n := range []int{1 << 20, 3 << 20} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			var tab Table
			fps := make([]Fingerprint, n)
			for i := range fps {
				fps[i] = Fingerprint{Hash: uint64(i) * 0x9e3779b97f4a7c15, Route: uint32(i * 2654435761)}
				tab.Put(fps[i], uint32(i))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := tab.Get(fps[(i*7919)%n]); !ok {
					b.Fatal("missing")
				}
			}
			b.ReportMetric(float64(len(tab.slots)*16)/float64(n), "B/name")
		})
	}
}

func TestHashBytesMatchesString(t *testing.T) {
	for _, name := range []string{"", "a", "metric.\x00.bytes", "metric.日本語", string(make([]byte, 4096))} {
		if HashBytes([]byte(name)) != Hash(name) {
			t.Fatalf("hash differs for %q", name)
		}
	}
}
