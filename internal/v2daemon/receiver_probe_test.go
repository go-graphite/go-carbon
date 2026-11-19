package v2daemon

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

// openProbeStore opens a layer-3 store whose catalog binds names to the shared
// policy identities, as the daemon does.
func openProbeStore(tb testing.TB) (*Store, PolicyResolver) {
	tb.Helper()
	r, err := v2read.New(2, 16<<20, 1<<20)
	if err != nil {
		tb.Fatal(err)
	}
	r.Identities = &v2core.PolicyCatalog{}
	s, err := Open(tb.TempDir(), Config{Layer: 3, Producers: 1, Shards: 2, MaxCoreRows: 1 << 16, GroupInterval: time.Hour, Hooks: v2core.Hooks{Identities: r.Identities, PublishBatches: r.Publish}})
	if err != nil {
		r.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		r.Close()
		if err := s.Close(); err != nil {
			tb.Error(err)
		}
	})
	resolve := func(name string) ([]byte, error) {
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 10, Count: 120}}}})
	}
	return s, resolve
}

// probeBatch splits one string the way v2recv's batch.named does: every name
// borrows the socket batch's text.
func probeBatch(names []string) (string, []string) {
	text := strings.Join(names, "")
	out := make([]string, len(names))
	start := 0
	for i, name := range names {
		out[i] = text[start : start+len(name)]
		start += len(name)
	}
	return text, out
}

// probeNames returns n distinct names of 280 bytes, the prod mean.
func probeNames(n, offset int) []string {
	out := make([]string, n)
	for i := range out {
		base := fmt.Sprintf("app.region%03d.cluster.component.host%08d.requests.duration.", (offset+i)%997, offset+i)
		out[i] = base + strings.Repeat("x", 280-len(base))
	}
	return out
}

func probePoints(n int) []points.Point {
	data := make([]points.Point, n)
	for i := range data {
		data[i] = points.Point{Timestamp: 1000, Value: float64(i)}
	}
	return data
}

// Either fingerprint half separates names. Equal routing hashes (the FNV half)
// are split by maphash end to end; equal maphash is split by the FNV half.
func TestApplicationOwnerProbeFingerprint(t *testing.T) {
	s, resolve := openProbeStore(t)
	p := s.Producers[0]
	o := &applicationOwner{p: p, resolve: resolve}
	names := []string{"collide.a", "collide.b", "collide.b", "collide.a", "collide.c", "collide.a"}
	hashes := make([]uint64, len(names))
	for i := range hashes {
		hashes[i] = 42 << 32 // forced equal FNV half
	}
	if err := o.Append(names, hashes, probePoints(len(names)), nil, 1010); err != nil {
		t.Fatal(err)
	}
	if got := p.catalog.Count(); got != 3 || o.used != 3 {
		t.Fatalf("catalog names=%d table=%d, want 3", got, o.used)
	}
	ids := p.applicationIDs[:len(names)]
	for i, name := range names {
		if got, _ := p.catalog.Key(ids[i]); got != name {
			t.Fatalf("point %d (%q) journaled as %q", i, name, got)
		}
	}
	// Forced equal maphash: only the FNV half tells these apart.
	a := namefp.Fingerprint{Hash: 7, Route: 1}
	*o.find(a) = nameSlot{fp: a, local: 1}
	if got := o.find(namefp.Fingerprint{Hash: 7, Route: 2}); got.local != 0 {
		t.Fatalf("equal maphash merged into local %d", got.local)
	}
	if got := o.find(a); got.local != 1 {
		t.Fatalf("lost local 1: %d", got.local)
	}
}

// The table grows past several resizes without losing or duplicating names.
// The owner retains no name; each handle's name is the catalog's own string,
// never a clone and never a substring of the borrowed batch text.
func TestApplicationOwnerProbeGrowthAndSharing(t *testing.T) {
	s, resolve := openProbeStore(t)
	p := s.Producers[0]
	o := &applicationOwner{p: p, resolve: resolve}
	const n = 3000 // 1024 slots grow at 769 and 1537 entries, then 3073
	raw := make([]string, n)
	for i := range raw {
		raw[i] = fmt.Sprintf("grow.host%04d.", i) + strings.Repeat("m", i%37)
	}
	var texts []string
	for start := 0; start < n; start += 512 {
		text, names := probeBatch(raw[start:min(n, start+512)])
		texts = append(texts, text)
		if err := o.Append(names, nil, probePoints(len(names)), nil, 1010); err != nil {
			t.Fatal(err)
		}
	}
	// Known names, routing hashes supplied, in a different batch layout.
	_, again := probeBatch(raw)
	hashes := make([]uint64, n)
	for i, name := range again {
		hashes[i] = v2recv.NameHash(name)
	}
	if err := o.Append(again[:2048], hashes[:2048], probePoints(2048), nil, 1020); err != nil {
		t.Fatal(err)
	}
	if o.used != n || p.catalog.Count() != n || len(o.slots) != 4096 {
		t.Fatalf("used=%d catalog=%d slots=%d", o.used, p.catalog.Count(), len(o.slots))
	}
	for _, name := range raw {
		slot := o.find(namefp.Of(namefp.Hash(name), v2recv.NameHash(name)))
		if slot.local == 0 {
			t.Fatalf("%q lost from the table", name)
		}
		registered := p.applications[slot.local]
		owned, _ := p.catalog.Key(uint64(slot.local))
		if registered == nil || registered.name != name {
			t.Fatalf("%q resolves to %v", name, registered)
		}
		at := unsafe.StringData(registered.name)
		if at != unsafe.StringData(owned) || at == unsafe.StringData(name) {
			t.Fatalf("%q is a separate copy, not the catalog's string", name)
		}
		for _, text := range texts {
			lo := uintptr(unsafe.Pointer(unsafe.StringData(text)))
			if a := uintptr(unsafe.Pointer(at)); a >= lo && a < lo+uintptr(len(text)) {
				t.Fatalf("%q pins the borrowed batch text", name)
			}
		}
	}
}

// BenchmarkApplicationOwnerProbe measures the L3 receiver adapter: "new"
// registers 1,024 new names per op and reports live heap per name; "known"
// appends 1,024 points of registered names per op (the steady-state hot path).
// Journal sync is outside the timer.
func BenchmarkApplicationOwnerProbe(b *testing.B) {
	const batch = 1024
	data := probePoints(batch)
	b.Run("new", func(b *testing.B) {
		s, resolve := openProbeStore(b)
		o := &applicationOwner{p: s.Producers[0], resolve: resolve}
		inputs := make([][]string, b.N)
		for i := range inputs {
			_, inputs[i] = probeBatch(probeNames(batch, i*batch))
		}
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if err := o.Append(inputs[i], nil, data, nil, 1010); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		inputs = nil
		runtime.GC()
		runtime.ReadMemStats(&m1)
		names := float64(b.N * batch)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/names, "ns/name")
		b.ReportMetric(float64(m1.Mallocs-m0.Mallocs)/names, "allocs/name")
		b.ReportMetric(float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/names, "live-B/name")
		if err := o.Drain(); err != nil {
			b.Fatal(err)
		}
	})
	for _, sets := range []int{16, 256} {
		b.Run(fmt.Sprintf("known%dk", sets), func(b *testing.B) {
			s, resolve := openProbeStore(b)
			o := &applicationOwner{p: s.Producers[0], resolve: resolve}
			inputs := make([][]string, sets)
			hashes := make([][]uint64, sets)
			for i := range inputs {
				_, inputs[i] = probeBatch(probeNames(batch, i*batch))
				hashes[i] = make([]uint64, batch)
				for j, name := range inputs[i] {
					hashes[i][j] = v2recv.NameHash(name)
				}
				if err := o.Append(inputs[i], hashes[i], data, nil, 1010); err != nil {
					b.Fatal(err)
				}
				if err := o.Drain(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := o.Append(inputs[i%sets], hashes[i%sets], data, nil, 1010); err != nil {
					b.Fatal(err)
				}
				if i%16 == 15 {
					b.StopTimer()
					if err := o.Drain(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/point")
		})
	}
}
