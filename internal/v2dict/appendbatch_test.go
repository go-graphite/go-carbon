package v2dict

import (
	"fmt"
	"maps"
	"sync"
	"testing"
)

// TestAppendBatchMatchesAppend: a batch assigns the same entries as one
// Append per name, in order: live, deleted (revived), repeated and new names,
// across scopes, append-level flushes and lock holds.
func TestAppendBatchMatchesAppend(t *testing.T) {
	one, err := Open(t.TempDir(), Options{BufferBytes: 4 << 10, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	batch, err := Open(t.TempDir(), Options{BufferBytes: 4 << 10, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Close()
	for i := 0; i < 300; i++ {
		g, vs := scopeOf(i)
		one.Append(g, vs, synthName(i), 1)
		batch.Append(g, vs, synthName(i), 1)
	}
	for i := 0; i < 300; i += 7 {
		g, vs := scopeOf(i)
		one.Delete(g, vs, synthName(i))
		batch.Delete(g, vs, synthName(i))
	}
	var names []NewName
	for i := 250; i < 900; i++ {
		k := i
		switch {
		case i%11 == 0:
			k = i / 3 // an older name, live or deleted
		case i%5 == 0:
			k = i - 3 // a repeat within the batch
		}
		g, vs := scopeOf(k)
		names = append(names, NewName{Group: g, VShard: vs, Name: string(synthName(k)), Policy: uint16(k % 4)})
	}
	// Small batches, then one batch of several lock holds.
	for lo := 0; lo < 200; lo += 97 {
		if err := batch.AppendBatch(names[lo:min(lo+97, 200)]); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.AppendBatch(names[200:]); err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		e, created, err := one.Append(n.Group, n.VShard, []byte(n.Name), n.Policy)
		if err != nil {
			t.Fatal(err)
		}
		if e != n.Entry || created != n.Created {
			t.Fatalf("name %d %q: batch %+v created %v, append %+v created %v", i, n.Name, n.Entry, n.Created, e, created)
		}
	}
	if err := batch.AppendBatch([]NewName{{Group: "a\x00", Name: "x"}}); err == nil {
		t.Fatal("invalid group accepted")
	}
}

// TestConcurrentShardedAppends: seals of different shards and of the same
// shard append overlapping names at once, across many freezes. Every name
// gets exactly one ID, IDs are dense per scope, and a reopen (log replay
// and level files) gives the same entries.
func TestConcurrentShardedAppends(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{BufferBytes: 2 << 10, Shards: 4, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	const writers, perScope, scopes = 16, 600, 12
	name := func(s, i int) string { return fmt.Sprintf("w.s%02d.n%04d", s, i) }
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			// Writers append the same names in differently cut batches.
			for lo := 0; lo < perScope; lo += 50 + w {
				var names []NewName
				// Scopes w/2 % 3 + 3k: every shard, so a batch is several holds.
				for s := w / 2 % 3; s < scopes; s += 3 {
					for i := lo; i < min(lo+50+w, perScope); i++ {
						names = append(names, NewName{Group: "g", VShard: uint16(s), Name: name(s, i), Policy: 1})
					}
				}
				if err := d.AppendBatch(names); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	check := func(d *Dict) map[string]uint32 {
		got := map[string]uint32{}
		for s := range scopes {
			if n := d.NextID("g", uint16(s)); n != perScope {
				t.Fatalf("scope %d: next ID %d, want %d (a name got two IDs)", s, n, perScope)
			}
			seen := make([]bool, perScope)
			for i := range perScope {
				e, ok := d.Lookup("g", uint16(s), []byte(name(s, i)))
				if !ok || e.ID >= perScope || seen[e.ID] {
					t.Fatalf("scope %d name %d: %+v %v", s, i, e, ok)
				}
				seen[e.ID] = true
				got[name(s, i)] = e.ID
			}
		}
		return got
	}
	before := check(d)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = Open(dir, Options{BufferBytes: 8 << 10, Shards: 3, NoMerge: true}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if after := check(d); !maps.Equal(before, after) {
		t.Fatal("entries changed across reopen")
	}
}

// TestAppendLooksUpAgainAfterFreeze: a freeze between an append's lock-free
// level lookup and its shard lock moves the name into the frozen level; the
// append must find it there, not assign a second ID.
func TestAppendLooksUpAgainAfterFreeze(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	first, _, err := d.Append("g", 1, []byte("x"), 0)
	if err != nil {
		t.Fatal(err)
	}
	froze := false
	testHook = func(point string) error {
		if point == "append-looked-up" && !froze {
			froze = true
			return d.freeze(true)
		}
		return nil
	}
	defer func() {
		d.flushWG.Wait() // the flush reads testHook
		testHook = nil
	}()
	again, created, err := d.Append("g", 1, []byte("x"), 0)
	if err != nil || !froze {
		t.Fatal(err, froze)
	}
	if created || again != first {
		t.Fatalf("after a freeze: %+v created %v, want %+v", again, created, first)
	}
}
