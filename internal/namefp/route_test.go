package namefp

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/zeebo/xxh3"
)

func TestRouteXXH3(t *testing.T) {
	type metric string
	type rawMetric []byte
	if got := Route(""); got != 0x2d06800538d394c2 {
		t.Fatalf("empty XXH3 vector: %016x", got)
	}
	// Exercise every short-input boundary and the long-input path, including
	// binary bytes and named types accepted by Route's generic contract.
	for _, n := range []int{0, 1, 2, 3, 4, 8, 9, 16, 17, 32, 64, 128, 129, 240, 241, 259, 1024, 4096} {
		raw := make([]byte, n)
		for i := range raw {
			raw[i] = byte(i*37 + 11)
		}
		want := xxh3.Hash(raw)
		for _, got := range []uint64{Route(raw), Route(string(raw)), Route(metric(raw)), Route(rawMetric(raw))} {
			if got != want {
				t.Fatalf("length %d: %016x, want %016x", n, got, want)
			}
		}
	}
}

func TestRouteNoAllocations(t *testing.T) {
	name := string(bytes.Repeat([]byte("metric."), 200))
	raw := []byte(name)
	for _, c := range []struct {
		name string
		hash func() uint64
	}{
		{"string", func() uint64 { return Route(name) }},
		{"bytes", func() uint64 { return Route(raw) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(100, func() { routeSink = c.hash() }); got != 0 {
				t.Fatal(fmt.Sprintf("%g allocations per hash", got))
			}
		})
	}
}
