//go:build (linux || darwin) && phasecpubench

package main

import (
	"strings"
	"testing"
)

func TestMetricNamesAndQuantile(t *testing.T) {
	seen := make(map[string]bool)
	for _, i := range []int{0, 1, 1023, 1024, 999999} {
		name := string(metricName(nil, i))
		if len(name) < 100 || len(name) > 180 || strings.ContainsAny(name, " /;") {
			t.Fatalf("unrealistic/invalid name %q", name)
		}
		if seen[name] {
			t.Fatal("duplicate name")
		}
		seen[name] = true
	}
	before := snapshot{Fine: make([]uint64, 65536), Count: 100}
	before.Fine[50] = 100
	after := snapshot{Fine: make([]uint64, 65536), Count: 200}
	after.Fine[50] = 100
	after.Fine[100] = 98
	after.Fine[200] = 2
	p99, lo, hi := quantile(before, after, .99)
	if p99 != 200.5/1e6 || lo != 200.0/1e6 || hi != 201.0/1e6 {
		t.Fatalf("p99 %g [%g,%g]", p99, lo, hi)
	}
}
