package v2read

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// TestMergeNamesMatchesSort pins MergeNames to the sort+compact it replaces.
func TestMergeNamesMatchesSort(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, k := range []int{0, 1, 2, 3, 7, 16, 97} {
		lists := make([][]string, k)
		var all []string
		for i := range lists {
			for range rng.Intn(50) {
				lists[i] = append(lists[i], fmt.Sprintf("a.%d.b", rng.Intn(300)))
			}
			slices.Sort(lists[i])
			lists[i] = slices.Compact(lists[i])
			all = append(all, lists[i]...)
		}
		slices.Sort(all)
		want := slices.Compact(all)
		if got := MergeNames(lists...); !slices.Equal(got, want) {
			t.Fatalf("k=%d: got %d names, want %d", k, len(got), len(want))
		}
	}
}
