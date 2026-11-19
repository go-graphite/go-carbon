package v2part

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func BenchmarkDensePlannedKey(b *testing.B) {
	for _, length := range []int{32, 259} {
		b.Run(fmt.Sprint(length), func(b *testing.B) {
			names := make([]ID, 4096)
			for i := range names {
				names[i] = ID(fmt.Sprintf("%s%08d", strings.Repeat("x", length-8), i))
			}
			slices.SortFunc(names, func(a, c ID) int {
				return cmp.Or(cmp.Compare(Route(a), Route(c)), cmp.Compare(a, c))
			})
			shards := make([]uint16, len(names))
			for i, name := range names {
				shards[i] = Route(name)
			}
			spec := fixtureSpec()
			spec.Scope.VShard = GroupVShard
			w, err := NewDenseWriter(filepath.Join(b.TempDir(), "part.sst"), spec)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Abort()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				j := i % len(names)
				if j == 0 {
					w.havePrev = false
				}
				if _, err := w.plannedKey(shards[j], uint32(j+1), names[j], 0, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
