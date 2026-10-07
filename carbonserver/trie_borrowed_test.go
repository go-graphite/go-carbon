package carbonserver

import (
	"reflect"
	"strings"
	"testing"
)

func TestBorrowedTriePathsMatchOwnedAndRetainStorage(t *testing.T) {
	paths := []string{"/shared/metric.wsp", "/shared/metrics.wsp", "/shared/metric/child.wsp", "/single.wsp", "/é/東京/longer/value.wsp", "/x//y.wsp", "/x/./z.wsp", "/x/../other.wsp", "", ".", "/", "relative/value.wsp", "/shared/met.wsp"}
	for _, bulk := range []bool{false, true} {
		borrowed, owned := newTrie(".wsp", 0, nil), newTrie(".wsp", 0, nil)
		if bulk {
			borrowed.builder = &trieBulkBuilder{}
		}
		var gotEstimates, wantEstimates []string
		borrowed.estimateSize = func(name string) (int64, int64, int64) { gotEstimates = append(gotEstimates, name); return 7, 11, 13 }
		owned.estimateSize = func(name string) (int64, int64, int64) { wantEstimates = append(wantEstimates, name); return 7, 11, 13 }
		for _, path := range paths {
			scratch := []byte(path)
			got, ge := borrowed.insertMutableBytes(scratch, 0, 0, 0, 123)
			want, we := owned.insertMutable(path, 0, 0, 0, 123)
			if (ge == nil) != (we == nil) || (got == nil) != (want == nil) {
				t.Fatalf("%q results differ: %v %v", path, ge, we)
			}
			for i := range scratch {
				scratch[i] = '!'
			}
			if borrowed.longestMetric != owned.longestMetric {
				t.Fatal("longest path retained borrowed storage")
			}
			if !reflect.DeepEqual(borrowed.allMetrics('.'), owned.allMetrics('.')) {
				t.Fatalf("metric labels changed after reusing %q", path)
			}
		}
		if !reflect.DeepEqual(gotEstimates, wantEstimates) || strings.Contains(strings.Join(gotEstimates, ""), "!") {
			t.Fatal("estimator did not receive an owned name")
		}
		gn, gf, gd, _, _, _, _, _ := borrowed.countNodes()
		wn, wf, wd, _, _, _, _, _ := owned.countNodes()
		if gn != wn || gf != wf || gd != wd {
			t.Fatal("node counts differ")
		}
	}
}

func FuzzBorrowedTriePaths(f *testing.F) {
	f.Add("/shared/metric.wsp\n/shared/metrics.wsp\n/shared/met.wsp\n/shared/metric/child.wsp")
	f.Add("/a//b.wsp\n/a/./c.wsp\n/a/../d.wsp\n/é/東京.wsp\n/\n.")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			t.Skip()
		}
		borrowed, owned := newTrie(".wsp", 0, nil), newTrie(".wsp", 0, nil)
		borrowed.builder = &trieBulkBuilder{}
		for _, path := range strings.Split(input, "\n") {
			scratch := []byte(path)
			got, ge := borrowed.insertMutableBytes(scratch, 1, 2, 3, 123)
			want, we := owned.insertMutable(path, 1, 2, 3, 123)
			if (ge == nil) != (we == nil) || (got == nil) != (want == nil) {
				t.Fatalf("%q results differ: %v %v", path, ge, we)
			}
			for i := range scratch {
				scratch[i] = '!'
			}
		}
		if borrowed.longestMetric != owned.longestMetric || !reflect.DeepEqual(borrowed.allMetrics('.'), owned.allMetrics('.')) {
			t.Fatal("borrowed paths differ after buffer reuse")
		}
	})
}
