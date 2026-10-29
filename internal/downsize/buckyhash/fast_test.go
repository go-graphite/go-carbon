package hashing

import (
	"fmt"
	"strings"
	"testing"
)

func TestCompiledMatchesBuckytools(t *testing.T) {
	nodes := []Node{{"10.0.0.4", 2003, "003"}, {"10.0.0.1", 2003, "000"}, {"10.0.0.3", 2003, "002"}, {"10.0.0.2", 2003, "001"}}
	for _, algo := range []string{"carbon", "fnv1a", "jump_fnv1a"} {
		t.Run(algo, func(t *testing.T) {
			compiled, err := Compile(algo, nodes, 1, 100)
			if err != nil {
				t.Fatal(err)
			}
			var oracle HashRing
			switch algo {
			case "carbon":
				oracle = NewCarbonHashRing()
			case "fnv1a":
				oracle = NewFNV1aHashRing()
			case "jump_fnv1a":
				oracle = NewJumpHashRing(1)
			}
			for _, n := range nodes {
				oracle.AddNode(n)
			}
			var scratch [1]int
			for i := 0; i < 10000; i++ {
				name := fmt.Sprintf("aggregations.minutely.service.%d.host.%d.metric.%d", i%13, i%97, i)
				got := compiled.Nodes()[compiled.Lookup(name, scratch[:0])[0]]
				if want := oracle.GetNode(name); got != want {
					t.Fatalf("%s: got %v want %v", name, got, want)
				}
			}
			for _, length := range []int{0, 1, 255, 256, 257, 1024} {
				name := strings.Repeat("x", length)
				got := compiled.Nodes()[compiled.Lookup(name, scratch[:0])[0]]
				if got != oracle.GetNode(name) {
					t.Fatalf("long key length %d differs", length)
				}
			}
		})
	}
}

func TestCompiledAll16BitPositions(t *testing.T) {
	nodes := []Node{{"a", 2003, ""}, {"b", 2003, ""}, {"c", 2003, ""}}
	for _, algo := range []string{"carbon", "fnv1a"} {
		c, err := Compile(algo, nodes, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		var ring []RingEntry
		if algo == "carbon" {
			r := NewCarbonHashRing()
			for _, n := range nodes {
				r.AddNode(n)
			}
			ring = r.ring
		} else {
			r := NewFNV1aHashRing()
			for _, n := range nodes {
				r.AddNode(n)
			}
			ring = r.ring
		}
		for p := 0; p < 65536; p++ {
			i := bisectLeft(ring, RingEntry{position: p}) % len(ring)
			if got := c.nodes[c.owners[p]]; got != ring[i].node {
				t.Fatalf("%s position %d: %v != %v", algo, p, got, ring[i].node)
			}
		}
	}
}

func TestReplacementIdentity(t *testing.T) {
	for _, algo := range []string{"carbon", "fnv1a", "jump_fnv1a"} {
		old := []Node{{"old", 2003, "000"}, {"other", 2003, "001"}}
		newNodes := []Node{{"replacement", 2003, "000"}, {"other", 2003, "001"}}
		a, _ := Compile(algo, old, 1, 100)
		b, _ := Compile(algo, newNodes, 1, 100)
		changed := 0
		var scratch [1]int
		for i := 0; i < 10000; i++ {
			name := fmt.Sprint(i)
			left := a.Lookup(name, scratch[:0])[0]
			right := b.Lookup(name, scratch[:0])[0]
			if left != right {
				changed++
			}
		}
		if algo == "carbon" && changed == 0 {
			t.Fatal("carbon includes hostname")
		}
		if algo != "carbon" && changed != 0 {
			t.Fatalf("%s moved %d identities", algo, changed)
		}
	}
}

func BenchmarkLookup(b *testing.B) {
	for _, algo := range []string{"carbon", "fnv1a", "jump_fnv1a"} {
		b.Run(algo, func(b *testing.B) {
			nodes := []Node{{"a", 2003, "000"}, {"b", 2003, "001"}, {"c", 2003, "002"}, {"d", 2003, "003"}, {"e", 2003, "004"}, {"f", 2003, "005"}}
			r, err := Compile(algo, nodes, 1, 100)
			if err != nil {
				b.Fatal(err)
			}
			names := make([]string, 4096)
			for i := range names {
				names[i] = fmt.Sprintf("aggregations.minutely.by_service.service_%d.by_host.host_%d.metric_%d", i%97, i%101, i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var scratch [1]int
			for i := 0; i < b.N; i++ {
				r.Lookup(names[i%len(names)], scratch[:0])
			}
		})
	}
}

func BenchmarkBuckytoolsReference(b *testing.B) {
	for _, algo := range []string{"carbon", "fnv1a", "jump_fnv1a"} {
		b.Run(algo, func(b *testing.B) {
			var r HashRing
			switch algo {
			case "carbon":
				r = NewCarbonHashRing()
			case "fnv1a":
				r = NewFNV1aHashRing()
			case "jump_fnv1a":
				r = NewJumpHashRing(1)
			}
			for i := 0; i < 6; i++ {
				r.AddNode(Node{string(rune('a' + i)), 2003, fmt.Sprintf("%03d", i)})
			}
			names := make([]string, 4096)
			for i := range names {
				names[i] = fmt.Sprintf("aggregations.minutely.by_service.service_%d.by_host.host_%d.metric_%d", i%97, i%101, i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.GetNode(names[i%len(names)])
			}
		})
	}
}
