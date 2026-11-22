package carbonserver

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestIsExactMetricName(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]bool{
		"a":            true,
		"a.b.c":        true,
		"a-b_c.d0.空":   true,
		"a.b.c^x!y=z":  true,
		"":             false,
		".a":           false,
		"a.":           false,
		"a..b":         false,
		" a.b":         false,
		"a.b\t":        false,
		"a.b*":         false,
		"a.?":          false,
		"a.[bc]":       false,
		"a.b]":         false,
		"a.{b,c}":      false,
		"a.b}":         false,
		"a.b,c":        false,
		"a.b\\c":       false,
		"a/b":          false,
		"a.b\x00c":     false,
		"a.b c":        true, // inner spaces are literal for the glob query too
		"a. b":         true,
		"a.b ":         false,
		"carbon.agent": true,
	} {
		if got := isExactMetricName(name); got != want {
			t.Errorf("isExactMetricName(%q) = %v, want %v", name, got, want)
		}
	}
}

func sameNodes(a, b []*trieNode) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// checkExactMatchesQuery compares the glob query with the exact lookup for
// one name: names, kinds, node pointers, lookup counts and errors.
func checkExactMatchesQuery(t *testing.T, ti *trieIndex, name string, limit int) bool {
	t.Helper()
	wn, wl, wd, wi, we := ti.query(strings.ReplaceAll(name, ".", "/"), limit, nil)
	gn, gl, gd, gi, ge := ti.queryExact(name, limit)
	if (we != nil) != (ge != nil) || wi != gi || !reflect.DeepEqual(wn, gn) || !reflect.DeepEqual(wl, gl) || !sameNodes(wd, gd) {
		t.Fatalf("%q limit %d: got %v %v lookups %d err %v, want %v %v lookups %d err %v", name, limit, gn, gl, gi, ge, wn, wl, wi, we)
	}
	return len(wn) > 0
}

// The exact lookup must return exactly what the glob query returns for every
// literal name: over snapshots with and without the key cursor, with and
// without a mutable overlay, a mutable trie alone, and every limit. Segments
// are prefixes of each other, sort around the separator, repeat as metric
// and namespace, and include a dotted file name.
func TestQueryExactMatchesGlobQuery(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(7))
	segments := []string{"a", "ab", "ab-", "ab_", "ab-c", "ab0", "b", "c", "-", "_", "wsp", "d.e", "空", "x"}
	randomName := func(depth int) string {
		parts := make([]string, random.Intn(depth)+1)
		for j := range parts {
			parts[j] = segments[random.Intn(len(segments))]
		}
		return strings.Join(parts, ".")
	}
	var found, total int
	for trial := 0; trial < 60; trial++ {
		snapshotPaths := map[string]bool{}
		for i, n := 0, 1+random.Intn(800); i < n; i++ {
			snapshotPaths["/"+strings.ReplaceAll(randomName(4), ".", "/")+".wsp"] = true
		}
		var mutable []string
		if trial%3 != 0 {
			for i, n := 0, random.Intn(40); i < n; i++ {
				if path := "/" + strings.ReplaceAll(randomName(4), ".", "/") + ".wsp"; !snapshotPaths[path] {
					mutable = append(mutable, path)
				}
			}
		}
		paths := make([]string, 0, len(snapshotPaths))
		for path := range snapshotPaths {
			paths = append(paths, path)
		}
		ti := newTrie(".wsp", 0, nil)
		if trial%4 == 3 {
			// No snapshot: every path lives in the mutable trie.
			mutable = append(mutable, paths...)
		} else {
			ti.snapshot = buildFastPathSnapshot(t, paths, trial%5 != 4)
		}
		for i, path := range mutable {
			if _, err := ti.insert(path, int64(-i-1), 2, 3, 4); err != nil {
				t.Fatal(err)
			}
		}
		for q := 0; q < 80; q++ {
			name := randomName(5)
			if !isExactMetricName(name) {
				t.Fatalf("generated name %q is not exact", name)
			}
			for _, limit := range []int{-1, 0, 1, 2, 3, 5, 10000} {
				total++
				if checkExactMatchesQuery(t, ti, name, limit) {
					found++
				}
			}
		}
	}
	if found == 0 || found == total {
		t.Fatalf("fixture does not cover both found and missing names: %d of %d", found, total)
	}
	t.Logf("%d of %d lookups found a metric or namespace", found, total)
}

// A name can be a metric, a namespace, both, or neither, and each half can
// live in the snapshot or the mutable trie.
func TestQueryExactMetricAndNamespace(t *testing.T) {
	t.Parallel()
	snapshot := []string{"/p/both.wsp", "/p/both/c.wsp", "/p/leaf.wsp", "/p/dir/c.wsp", "/p/leafdir/c.wsp", "/p/split.wsp"}
	mutable := []string{"/p/leafdir.wsp", "/p/split/c.wsp", "/p/new.wsp", "/p/newdir/c.wsp", "/p/both.wsp"}
	for _, withCursor := range []bool{true, false} {
		ti := newTrie(".wsp", 0, nil)
		ti.snapshot = buildFastPathSnapshot(t, snapshot, withCursor)
		for _, path := range mutable {
			if _, err := ti.insert(path, 1, 2, 3, 4); err != nil {
				t.Fatal(err)
			}
		}
		for name, want := range map[string][]bool{
			"p.both":     {true, false},
			"p.leaf":     {true},
			"p.dir":      {false},
			"p.leafdir":  {true, false},
			"p.split":    {true, false},
			"p.new":      {true},
			"p.newdir":   {false},
			"p.missing":  nil,
			"p.bot":      nil,
			"p.both.c.d": nil,
			"p":          {false},
			"q":          nil,
		} {
			for limit := -1; limit < 4; limit++ {
				checkExactMatchesQuery(t, ti, name, limit)
			}
			names, leaves, _, _, err := ti.queryExact(name, 100)
			if err != nil || !reflect.DeepEqual(leaves, want) {
				t.Fatalf("cursor %v %q: got %v %v %v, want kinds %v", withCursor, name, names, leaves, err, want)
			}
			for _, n := range names {
				if n != name {
					t.Fatalf("%q returned name %q", name, n)
				}
			}
		}
	}
}

func expandForTest(t *testing.T, listener *CarbonserverListener, names ...string) map[string]globs {
	t.Helper()
	expanded, err := listener.getExpandedGlobs(context.Background(), zap.NewNop(), time.Now(), names)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]globs, len(expanded))
	for _, g := range expanded {
		byName[g.Name] = g
	}
	return byName
}

// Render expansion through the listener: exact names take the lookup, globs
// the glob query, and a request mixing both returns what the glob query
// returns for each name.
func TestExpandGlobsExactNames(t *testing.T) {
	t.Parallel()
	listener := newTrieServer([]string{
		"/svc/host1/cpu.wsp",
		"/svc/host1/mem.wsp",
		"/svc/host2/cpu.wsp",
		"/svc/host2.wsp",
		"/svc/host20/cpu.wsp",
	}, t)
	ti := listener.CurrentFileIndex().trieIdx
	want := func(name string) globs {
		files, leafs, nodes, lookups, err := ti.query(strings.ReplaceAll(name, ".", "/"), listener.maxMetricsGlobbed, listener.expandGlobBraces)
		if err != nil {
			t.Fatal(err)
		}
		return globs{Name: name, Files: files, Leafs: leafs, TrieNodes: nodes, Lookups: lookups}
	}
	for _, tc := range []struct {
		name  string
		files []string
		leafs []bool
	}{
		{"svc.host1.cpu", []string{"svc.host1.cpu"}, []bool{true}},
		{"svc.host1.disk", nil, nil},
		{"svc.host1", []string{"svc.host1"}, []bool{false}},
		{"svc.host2", []string{"svc.host2", "svc.host2"}, []bool{true, false}},
		{"svc.host*.cpu", []string{"svc.host1.cpu", "svc.host2.cpu", "svc.host20.cpu"}, []bool{true, true, true}},
	} {
		got := expandForTest(t, listener, tc.name)[tc.name]
		if !reflect.DeepEqual(got.Files, tc.files) || !reflect.DeepEqual(got.Leafs, tc.leafs) {
			t.Errorf("%q: got %v %v, want %v %v", tc.name, got.Files, got.Leafs, tc.files, tc.leafs)
		}
		if w := want(tc.name); !reflect.DeepEqual(got.Files, w.Files) || !reflect.DeepEqual(got.Leafs, w.Leafs) || !sameNodes(got.TrieNodes, w.TrieNodes) || got.Lookups != w.Lookups {
			t.Errorf("%q: got %+v, glob query %+v", tc.name, got, w)
		}
	}

	mixed := []string{"svc.host1.cpu", "svc.{host1,host2}.mem", "svc.host2", "svc.nope.cpu", "svc.host?.cpu"}
	got := expandForTest(t, listener, mixed...)
	if len(got) != len(mixed) {
		t.Fatalf("mixed request returned %d names, want %d", len(got), len(mixed))
	}
	for _, name := range mixed {
		g, w := got[name], want(name)
		if !reflect.DeepEqual(g.Files, w.Files) || !reflect.DeepEqual(g.Leafs, w.Leafs) || !sameNodes(g.TrieNodes, w.TrieNodes) || g.Lookups != w.Lookups {
			t.Errorf("mixed %q: got %+v, glob query %+v", name, g, w)
		}
	}
}

// exactBenchmarkIndex builds a snapshot shaped like a store catalogue plus a
// small mutable overlay of newly created metrics, and 1000 exact names of
// existing metrics to render.
func exactBenchmarkIndex(b *testing.B) (*trieIndex, []string) {
	var paths, mutable, names []string
	for svc := 0; svc < 20; svc++ {
		for host := 0; host < 250; host++ {
			for m := 0; m < 40; m++ {
				path := fmt.Sprintf("/apps/service_%02d/host_%04d/metrics/requests_%02d/count.wsp", svc, host, m)
				if host%100 == 99 && m == 0 {
					mutable = append(mutable, path)
				} else {
					paths = append(paths, path)
				}
			}
		}
	}
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = buildFastPathSnapshot(b, paths, true)
	for _, path := range mutable {
		if _, err := ti.insert(path, 1, 2, 3, 4); err != nil {
			b.Fatal(err)
		}
	}
	random := rand.New(rand.NewSource(1))
	for len(names) < 1000 {
		path := paths[random.Intn(len(paths))]
		names = append(names, strings.ReplaceAll(strings.TrimSuffix(path[1:], ".wsp"), "/", "."))
	}
	sort.Strings(names)
	return ti, names
}

// One 1000-name multi-fetch render's expansion, every name exact: the glob
// query the trie used before against the exact lookup.
func BenchmarkExpandExactNames1000(b *testing.B) {
	ti, names := exactBenchmarkIndex(b)
	paths := make([]string, len(names))
	for i, name := range names {
		paths[i] = strings.ReplaceAll(name, ".", "/")
	}
	b.Run("glob-query", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, path := range paths {
				if files, _, _, _, err := ti.query(path, 1<<30, nil); err != nil || len(files) != 1 {
					b.Fatalf("%s: %v %v", path, files, err)
				}
			}
		}
	})
	b.Run("exact-lookup", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, name := range names {
				if files, _, _, _, err := ti.queryExact(name, 1<<30); err != nil || len(files) != 1 {
					b.Fatalf("%s: %v %v", name, files, err)
				}
			}
		}
	})
}
