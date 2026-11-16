package main

import "testing"

func TestStableWholeNameSelection(t *testing.T) {
	for _, name := range []string{"a", "b", "some.metric;tag=value"} {
		if !selectedName(name, 1) {
			t.Fatal("full admission filtered", name)
		}
		selected := selectedName(name, 8192)
		for range 100 {
			if selectedName(name, 8192) != selected {
				t.Fatal("name selection changed")
			}
		}
	}
	// FNV-1a test vector for the ASCII string a is af63dc4c8601ec8c.
	if !selectedName("a", 4) || selectedName("a", 8) {
		t.Fatal("selection hash changed")
	}
}
