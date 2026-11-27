package v2dict

import (
	"fmt"
	"slices"
	"testing"
)

func TestAdmissionPreservesCompactPrefix(t *testing.T) {
	d, err := Open(t.TempDir(), Options{BufferBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := range 1000 {
		if _, _, err = d.Append("g", 0, fmt.Appendf(nil, "n%04d", i), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	d.mergeWG.Wait()
	prefix := d.snap.Load().levels[0]
	if !prefix.compact {
		t.Fatal("fixture not compact")
	}
	for i := range 10 {
		if _, _, err = d.Append("g", 0, fmt.Appendf(nil, "z%04d", i), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	before := d.snap.Load().levels
	if pickMerge(before, d.opt.Fanout) >= 0 {
		t.Fatal("fixture unexpectedly needs fanout merge")
	}
	d.mergeWG.Wait()
	if !slices.Contains(d.snap.Load().levels, prefix) {
		t.Fatal("rewrote 1000-name compressed prefix for 10 new names without a due fanout merge")
	}
}
