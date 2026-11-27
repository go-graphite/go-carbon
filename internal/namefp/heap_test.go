package namefp

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

func heapNow() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestOwnerIndexHeap compares an owner's name index before (a name-keyed map
// whose keys share the identity's existing strings, as coreRollups.names did)
// and after (namefp.Table), in heap bytes per name and lookup ns. Opt-in:
// NAMEFP_HEAP=1 go test -run TestOwnerIndexHeap -v ./internal/namefp
func TestOwnerIndexHeap(t *testing.T) {
	if os.Getenv("NAMEFP_HEAP") == "" {
		t.Skip("set NAMEFP_HEAP=1")
	}
	for _, n := range []int{1_000_000, 3_000_000} {
		names := make([]string, n)
		fps := make([]Fingerprint, n)
		for i := range names {
			// ≈120-B names, like the scale harness; the strings exist before
			// either index, as the identity's name does.
			names[i] = fmt.Sprintf("containers.k8s.prod.cluster-%02d.namespace-%03d.pod-%06x.container-%04d.cpu.usage.seconds.total.rate", i%13, i%251, i, i%977)
			fps[i] = Name(names[i])
		}
		base := heapNow()
		m := make(map[string]uint64)
		for i, s := range names {
			m[s] = uint64(i + 1)
		}
		mapBytes := heapNow() - base
		base = heapNow()
		var tab Table
		for i, fp := range fps {
			tab.Put(fp, uint32(i))
		}
		tabBytes := heapNow() - base

		start := time.Now()
		var sink uint64
		for i := 0; i < n; i++ {
			sink += m[names[(i*7919)%n]]
		}
		mapNS := float64(time.Since(start).Nanoseconds()) / float64(n)
		start = time.Now()
		for i := 0; i < n; i++ {
			v, _ := tab.Get(fps[(i*7919)%n])
			sink += uint64(v)
		}
		tabNS := float64(time.Since(start).Nanoseconds()) / float64(n)
		start = time.Now()
		for i := 0; i < n; i++ {
			sink += Route(names[(i*7919)%n])
		}
		routeNS := float64(time.Since(start).Nanoseconds()) / float64(n)
		t.Logf("names=%d map[name]id %.1f B/name %.1f ns/lookup | namefp.Table %.1f B/name %.1f ns/lookup | Route (XXH3-64, once per name at intern) %.1f ns | sink %d",
			n, float64(mapBytes)/float64(n), mapNS, float64(tabBytes)/float64(n), tabNS, routeNS, sink&1)
		runtime.KeepAlive(m)
		runtime.KeepAlive(&tab)
		runtime.KeepAlive(names)
	}
}
