package carbonserver

import (
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Keep the straightforward per-metric accounting as an independent oracle for
// the postorder aggregation, including immediate namespaces and read counters.
func overlayUsageReference(ti *trieIndex) (map[string]QuotaUsage, map[string][2]int64, int) {
	usage := make(map[string]QuotaUsage)
	reads := make(map[string][2]int64)
	dirs := make(map[string]bool)
	names, nodes, _, _, _ := ti.allMetricsNodeMutable(ti.root, '.', "", int(^uint(0)>>1), false)
	for i, name := range names {
		m := nodes[i].meta.Load().(*fileMeta)
		hits := atomic.SwapInt64(&m.readHits, 0)
		readBytes := atomic.SwapInt64(&m.readBytes, 0)
		metricNamespaces(name, func(prefix string) {
			u := usage[prefix]
			u.Metrics++
			u.LogicalSize += m.logicalSize
			u.PhysicalSize += m.physicalSize
			u.DataPoints += m.dataPoints
			usage[prefix] = u
			r := reads[prefix]
			r[0] += hits
			r[1] += readBytes
			reads[prefix] = r
			if prefix != "/" {
				dirs[prefix] = true
			}
		})
	}
	extraDirs := 0
	exists := ti.snapshot.namespaceLookup()
	for name := range dirs {
		if exists(name) {
			continue
		}
		parent := "/"
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			parent = name[:i]
		}
		u := usage[parent]
		u.Namespaces++
		usage[parent] = u
		extraDirs++
	}
	return usage, reads, extraDirs
}

// snapshotQuotaUsage counts only the affected mutable subtree and reads saved
// totals from the packed prefix sums. It leaves throughput/read counters intact.
func TestSnapshotOverlayUsageMatchesReference(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			fast, _, _, _ := snapshotTrieFixture(t)
			reference, _, _, _ := snapshotTrieFixture(t)
			for _, ti := range []*trieIndex{fast, reference} {
				if populated {
					paths := []string{"/new.wsp", "/new/child.wsp", "/a/branch.wsp", "/a/branch/leaf.wsp", "/é/東京/x.wsp", "/é/東京/y.wsp", "/ab/leaf.wsp", "/a/a1.wsp"}
					for i := 0; i < 1000; i++ {
						paths = append(paths, fmt.Sprintf("/a/group%02d/deep%d/m%04d.wsp", i%17, i%13, i))
					}
					for i, path := range paths {
						n, err := ti.insert(path, int64(i+3), int64(i+5), int64(i+7), 123)
						if err != nil {
							t.Fatal(err)
						}
						m := n.meta.Load().(*fileMeta)
						atomic.StoreInt64(&m.readHits, int64(i+11))
						atomic.StoreInt64(&m.readBytes, int64(i+19))
					}
				}
				if _, err := ti.applyQuotas(time.Minute, &Quota{Pattern: "/", Metrics: 10000}, &Quota{Pattern: "a", Metrics: 10000}, &Quota{Pattern: "a.group*", Metrics: 10000}, &Quota{Pattern: "é.*", Metrics: 10000}, &Quota{Pattern: "new", Metrics: 10000}); err != nil {
					t.Fatal(err)
				}
			}
			for pass := 0; pass < 2; pass++ {
				got, gr, gd := fast.overlayUsage()
				want, wr, wd := overlayUsageReference(reference)
				if gd != wd {
					t.Fatalf("extra namespaces %d != %d", gd, wd)
				}
				keys := []string{"/"}
				for name := range fast.quotaNodes {
					if name != "/" {
						keys = append(keys, name)
					}
				}
				if len(got) > len(keys) || len(gr) > len(keys) {
					t.Fatal("unconfigured totals retained")
				}
				for _, name := range keys {
					if !reflect.DeepEqual(got[name], want[name]) || gr[name] != wr[name] {
						t.Fatalf("pass %d namespace %q usage %+v != %+v reads %v != %v", pass, name, got[name], want[name], gr[name], wr[name])
					}
					if pass == 1 && gr[name] != [2]int64{} {
						t.Fatal("read counters counted twice")
					}
				}
			}
		})
	}
}
