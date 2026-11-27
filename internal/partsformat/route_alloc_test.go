package partsformat

import (
	"hash/crc32"
	"strings"
	"testing"
)

func TestRouteChecksumOracleAndAllocations(t *testing.T) {
	for _, name := range []string{"", "x", "name.é.metric", strings.Repeat("metric.", 100), string([]byte{0, 255, 1, 128})} {
		want := uint16(crc32.Checksum([]byte(name), routingTable) & (VirtualShards - 1))
		if got := Route(ID(name)); got != want {
			t.Fatalf("route %d want %d", got, want)
		}
	}
	name := ID(strings.Repeat("metric.", 100))
	if allocs := testing.AllocsPerRun(100, func() { _ = Route(name) }); allocs != 0 {
		t.Fatalf("route allocates %.1f times", allocs)
	}
}
