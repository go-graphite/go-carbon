package v2index

import (
	"fmt"
	"slices"
	"testing"
)

func TestSnapshotEqualNames(t *testing.T) {
	c := &Catalog{}
	if err := c.Publish(1, []string{"b", "a"}); err != nil {
		t.Fatal(err)
	}
	s := c.Snapshot()
	for _, names := range [][]string{nil, {"a"}, {"b", "a"}, {"a", "c"}, {"a", "b", "c"}, {"a", "b"}} {
		if got, want := s.EqualNames(names), slices.Equal(slices.Collect(s.Names()), names); got != want {
			t.Fatalf("EqualNames(%v)=%v, want %v", names, got, want)
		}
	}
	if err := c.Apply(2, []string{"c"}, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if !s.EqualNames([]string{"a", "b"}) || !c.Snapshot().EqualNames([]string{"b", "c"}) {
		t.Fatal("publication modified a pinned view")
	}
}

// Both arms execute the exact old/new refresh comparison. Report allocations;
// wall time on a shared host is secondary. Setup is excluded from the timer.
func BenchmarkCatalogRefreshCompare(b *testing.B) {
	names := make([]string, 100_000)
	for i := range names {
		names[i] = fmt.Sprintf("metric.%08d", i)
	}
	var c Catalog
	if err := c.Publish(1, names); err != nil {
		b.Fatal(err)
	}
	old := c.Snapshot()
	for _, copyView := range []bool{true, false} {
		b.Run(fmt.Sprintf("copy=%t", copyView), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var equal bool
				if copyView {
					equal = slices.Equal(slices.Collect(old.Names()), names)
				} else {
					equal = old.EqualNames(names)
				}
				if !equal {
					b.Fatal("comparison changed")
				}
			}
		})
	}
}
