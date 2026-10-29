package carbonserver

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestSnapshotChildrenIndependentOracle(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(2481))
	segments := []string{"a", "ab", "ab-", "ab_", "ab0", "b", "x", "d.e", "x.wsp", "空", "é", "-", "_"}
	for trial := 0; trial < 20; trial++ {
		paths := map[string]bool{"/p/ab.wsp": true, "/p/ab/child.wsp": true, "/p/ab.wsp/child.wsp": true}
		queries := map[string]bool{"*": true, "p/*": true, "p/ab.wsp/*": true, "absent/*": true, "///p//*": true}
		for i := 0; i < 300; i++ {
			parts := make([]string, 1+rng.Intn(4))
			for j := range parts {
				parts[j] = segments[rng.Intn(len(segments))]
			}
			paths["/"+strings.Join(parts, "/")+".wsp"] = true
			if len(queries) < 45 && len(parts) > 1 {
				queries[strings.Join(parts[:len(parts)-1], "/")+"/*"] = true
			}
		}
		var all []string
		for path := range paths {
			all = append(all, path)
		}
		base := buildFastPathSnapshot(t, all, true)
		for query := range queries {
			for _, limit := range []int{-1, 0, 1, 2, 3, 7, 10000} {
				// Separate node caches are essential: warming the reference cache
				// first could hide a wrong metadata row chosen by the new cursor.
				old := &indexSnapshot{index: base.index, metadata: base.metadata}
				next := &indexSnapshot{index: base.index, keys: base.keys, metadata: base.metadata}
				wn, wl, wd, wi, we := oldSnapshotQuery(old, query, limit, nil)
				gn, gl, gd, gi, ge := next.query(query, limit, nil)
				if (we != nil) != (ge != nil) || wi != gi || !reflect.DeepEqual(wn, gn) || !reflect.DeepEqual(wl, gl) || len(wd) != len(gd) {
					t.Fatalf("trial %d %q/%d: names %v/%v leaves %v/%v lookups %d/%d errors %v/%v", trial, query, limit, wn, gn, wl, gl, wi, gi, we, ge)
				}
				for i, leaf := range wl {
					if !leaf {
						continue
					}
					want, got := wd[i].meta.Load().(*fileMeta), gd[i].meta.Load().(*fileMeta)
					if want.logicalSize != got.logicalSize || want.physicalSize != got.physicalSize || want.dataPoints != got.dataPoints || want.firstSeenAt != got.firstSeenAt {
						t.Fatalf("trial %d %q/%d: metadata differs for %q", trial, query, limit, wn[i])
					}
				}
			}
		}
	}
}

func BenchmarkSnapshotChildrenSmallLimit(b *testing.B) {
	ti := fastPathBenchmarkIndex(b, 90000, true)
	const expr = "aggregations/secondly/service/by_feature_name/*"
	for _, next := range []bool{false, true} {
		name, query := "old", oldTrieQuery
		if next {
			name, query = "new", (*trieIndex).query
		}
		b.Run(name, func(b *testing.B) {
			if _, _, _, _, err := query(ti, expr, 1, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				names, _, _, _, err := query(ti, expr, 1, nil)
				if err != nil || len(names) != 1 {
					b.Fatalf("%d names, %v", len(names), err)
				}
			}
		})
	}
}
