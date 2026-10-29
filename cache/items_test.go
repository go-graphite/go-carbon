package cache

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
)

// A tiny hash space forces collisions, replacement and tombstone reuse.
func TestItemMapMatchesStringMapWithCollisions(t *testing.T) {
	var items itemMap
	want := map[string]*item{}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 4000 {
		n := rng.IntN(64)
		name, hash := fmt.Sprintf("metric.%d", n), uint64(n%4)
		if rng.IntN(3) == 0 {
			items.removeHashed(hash, name)
			delete(want, name)
		} else {
			it := newItem(name, 0)
			items.setHashed(hash, it)
			want[name] = it
		}
		if items.getHashed(hash, name) != want[name] || items.getBytesHashed(hash, []byte(name)) != want[name] {
			t.Fatalf("lookup %s disagrees", name)
		}
		got := map[string]*item{}
		for key, it := range items.all {
			if got[key] != nil {
				t.Fatalf("duplicate %s", key)
			}
			got[key] = it
		}
		if items.len() != len(want) || !reflect.DeepEqual(got, want) {
			t.Fatal("enumeration disagrees with string map")
		}
	}
}

func TestItemMapWrappedProbeAndZeroHash(t *testing.T) {
	var m itemMap
	const hash = ^uint64(0)
	for i := range 256 {
		m.setHashed(hash, newItem(fmt.Sprintf("wrap.%d", i), 0))
	}
	for i := range 256 {
		if i%2 == 0 {
			m.removeHashed(hash, fmt.Sprintf("wrap.%d", i))
		}
	}
	for i := range 256 {
		got := m.getHashed(hash, fmt.Sprintf("wrap.%d", i))
		if (got == nil) != (i%2 == 0) {
			t.Fatalf("wrapped lookup %d", i)
		}
	}
	for i := range 256 {
		name := fmt.Sprintf("zero.%d", i)
		m.setHashed(0, newItem(name, 0))
	}
	for i := range 256 {
		name := fmt.Sprintf("zero.%d", i)
		if m.getHashed(0, name) == nil {
			t.Fatal("zero hash confused with empty slot")
		}
		m.removeHashed(0, name)
	}
	if m.len() != 128 {
		t.Fatalf("remaining metrics %d", m.len())
	}
}
