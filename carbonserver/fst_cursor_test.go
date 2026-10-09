package carbonserver

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// cursorKeys enumerates every key below s with the cursor alone, treating 0
// bytes as name separators the way directory listings do.
func cursorKeys(c *fstCursor, s fstState, prefix []byte, out *[][2]any) {
	if final, value := c.final(s); final && len(prefix) == 0 {
		*out = append(*out, [2]any{"", value})
	}
	c.names(s, new([]byte), func(name []byte, final bool, value uint64, child fstState) {
		key := append(bytes.Clone(prefix), name...)
		if final {
			*out = append(*out, [2]any{string(key), value})
		}
		if child.ok {
			if final, value := c.final(child); final {
				*out = append(*out, [2]any{string(append(bytes.Clone(key), 0)), value})
			}
			cursorKeys(c, child, append(key, 0), out)
		}
	})
}

func TestFSTCursorMatchesVellum(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for iter := 0; iter < 200; iter++ {
		keys := randomFSTKeys(rng, 1+rng.Intn(300))
		// Directory-shaped keys: no empty names, so no leading, doubled or
		// trailing separators, except the "D\x00" keys the catalogue uses.
		var shaped [][]byte
		for _, k := range keys {
			k = bytes.Trim(k, "\x00")
			for bytes.Contains(k, []byte{0, 0}) {
				k = bytes.ReplaceAll(k, []byte{0, 0}, []byte{0})
			}
			if len(k) > 0 {
				shaped = append(shaped, k)
				if rng.Intn(8) == 0 {
					shaped = append(shaped, append(bytes.Clone(k), 0))
				}
			}
		}
		slices.SortFunc(shaped, bytes.Compare)
		shaped = slices.CompactFunc(shaped, bytes.Equal)
		var cuts []int
		for i := rng.Intn(min(len(shaped), 8)); i > 0; i-- {
			cuts = append(cuts, rng.Intn(len(shaped)+1))
		}
		slices.Sort(cuts)
		cuts = slices.Compact(cuts)
		for _, joined := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/joined=%t", iter, joined), func(t *testing.T) {
				data := buildTestFST(t, shaped)
				if joined {
					data = joinTestFST(t, shaped, cuts)
				}
				c, err := newFSTCursor(data)
				if err != nil {
					t.Fatal(err)
				}
				var got [][2]any
				cursorKeys(c, c.start(), nil, &got)
				if len(got) != len(shaped) {
					seen := map[string]bool{}
					for _, g := range got {
						seen[g[0].(string)] = true
					}
					for _, k := range shaped {
						if !seen[string(k)] {
							t.Errorf("missing %q", k)
						}
					}
					t.Fatalf("cursor found %d keys, want %d", len(got), len(shaped))
				}
				for i, k := range shaped {
					if got[i][0] != string(k) || got[i][1] != uint64(i) {
						t.Fatalf("key %d: cursor %q=%v, want %q=%d", i, got[i][0], got[i][1], k, i)
					}
					if final, value := c.final(c.acceptBytes(c.start(), k)); !final || value != uint64(i) {
						t.Fatalf("accept %q: %t %d", k, final, value)
					}
				}
				for _, k := range randomFSTKeys(rng, 30) {
					_, want := slices.BinarySearchFunc(shaped, k, bytes.Compare)
					if final, _ := c.final(c.acceptBytes(c.start(), k)); final != want {
						t.Fatalf("probe %q: final=%t, want %t", k, final, want)
					}
				}
			})
		}
	}
}

func TestFSTCursorWideNodes(t *testing.T) {
	var keys [][]byte
	for c := 1; c < 256; c++ {
		keys = append(keys, []byte{'d', 0, byte(c)}, []byte{'d', 0, byte(c), 'x'})
	}
	slices.SortFunc(keys, bytes.Compare)
	c, err := newFSTCursor(buildTestFST(t, keys))
	if err != nil {
		t.Fatal(err)
	}
	var got [][2]any
	cursorKeys(c, c.start(), nil, &got)
	if len(got) != len(keys) {
		t.Fatalf("cursor found %d keys, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i][0] != string(keys[i]) {
			t.Fatalf("key %d: %q, want %q", i, got[i][0], keys[i])
		}
	}
	empty, err := newFSTCursor(buildTestFST(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if final, _ := empty.final(empty.start()); final {
		t.Fatal("empty fst has a key")
	}
}
