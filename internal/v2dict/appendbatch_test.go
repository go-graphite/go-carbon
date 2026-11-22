package v2dict

import (
	"testing"
)

// TestAppendBatchMatchesAppend: a batch assigns the same entries as one
// Append per name, in order: live, deleted (revived), repeated and new names,
// across scopes and append-level flushes.
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
	for lo := 0; lo < len(names); lo += 97 {
		if err := batch.AppendBatch(names[lo:min(lo+97, len(names))]); err != nil {
			t.Fatal(err)
		}
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
