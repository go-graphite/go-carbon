package carbonserver

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/blevesearch/vellum"
)

// oldSnapshotQuery is indexSnapshot.query before the counterpart cursor:
// a full index.Get per namespace match.
func oldSnapshotQuery(s *indexSnapshot, expr string, limit int, expand func([]string) ([]string, error)) (names []string, leaves []bool, nodes []*trieNode, lookups uint32, err error) {
	a, err := newSnapshotGlob(expr, expand)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	iterator, err := s.index.Search(a, nil, nil)
	pairedFiles := make(map[string]bool)
	if iterator != nil {
		defer iterator.Close()
	}
	for err == nil {
		lookups++
		key, row := iterator.Current()
		path := key[1:]
		boundary := -1
		for i, depth := 0, 0; i < len(path); i++ {
			if path[i] != 0 {
				continue
			}
			depth++
			if depth == len(a.components) {
				boundary = i
				break
			}
		}
		var name string
		var node *trieNode
		leaf := boundary < 0
		if leaf {
			name = strings.ReplaceAll(string(path[:len(path)-4]), "\x00", ".")
			if pairedFiles[name] {
				err = iterator.Next()
				continue
			}
			node, err = s.fileNode(name, row)
			if err != nil {
				return nil, nil, nil, lookups, err
			}
		} else {
			name = strings.ReplaceAll(string(path[:boundary]), "\x00", ".")
			node = s.directoryNode(name)
			counterpart := append([]byte(nil), key[:boundary+1]...)
			counterpart = append(counterpart, ".wsp"...)
			metricRow, found, getErr := s.index.Get(counterpart)
			if getErr != nil {
				return nil, nil, nil, lookups, getErr
			}
			if found {
				file, nodeErr := s.fileNode(name, metricRow)
				if nodeErr != nil {
					return nil, nil, nil, lookups, nodeErr
				}
				names = append(names, name)
				leaves = append(leaves, true)
				nodes = append(nodes, file)
				pairedFiles[name] = true
			}
		}
		names = append(names, name)
		leaves = append(leaves, leaf)
		nodes = append(nodes, node)
		if len(names) >= limit {
			break
		}
		if leaf {
			err = iterator.Next()
		} else {
			next := append([]byte(nil), key[:boundary+2]...)
			next[len(next)-1] = 1
			err = iterator.Seek(next)
		}
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		err = nil
	}
	return names, leaves, nodes, lookups, err
}

// oldTrieQuery is trieIndex.query before the ordered fast path: every result
// is copied, sorted and copied again.
func oldTrieQuery(ti *trieIndex, expr string, limit int, expand func([]string) ([]string, error)) ([]string, []bool, []*trieNode, uint32, error) {
	if ti.snapshot == nil {
		return ti.queryMutable(expr, limit, expand)
	}
	names, leaves, nodes, lookups, err := oldSnapshotQuery(ti.snapshot, expr, limit, expand)
	if err != nil {
		return nil, nil, nil, lookups, err
	}
	extra, el, en, its, err := ti.queryMutable(expr, limit, expand)
	lookups += its
	if err != nil {
		return nil, nil, nil, lookups, err
	}
	rows := make([]snapshotResult, 0, len(names)+len(extra))
	for i, name := range names {
		rows = append(rows, snapshotResult{name, leaves[i], nodes[i]})
	}
	for i, name := range extra {
		node := en[i]
		if !el[i] {
			node = ti.snapshot.directoryNode(name)
		}
		rows = append(rows, snapshotResult{name, el[i], node})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].name == rows[j].name {
			return rows[i].leaf && !rows[j].leaf
		}
		return rows[i].name < rows[j].name
	})
	names, leaves, nodes = nil, nil, nil
	for i, row := range rows {
		if i > 0 && row.name == rows[i-1].name && row.leaf == rows[i-1].leaf {
			continue
		}
		if len(names) > 0 && len(names) >= limit && row.name != names[len(names)-1] {
			break
		}
		names = append(names, row.name)
		leaves = append(leaves, row.leaf)
		nodes = append(nodes, row.node)
	}
	return names, leaves, nodes, lookups, nil
}

// buildFastPathSnapshot writes an in-memory generation for paths. Row i gets
// distinct metadata so a wrong counterpart row is visible in the nodes.
func buildFastPathSnapshot(tb testing.TB, paths []string, withCursor bool) *indexSnapshot {
	tb.Helper()
	keys := make([][]byte, len(paths))
	for i, path := range paths {
		keys[i], _ = encodeSnapshotPath(nil, path)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	var fst, metadata bytes.Buffer
	builder, err := vellum.New(&fst, nil)
	if err != nil {
		tb.Fatal(err)
	}
	mw := snapshotMetadataWriter{w: &metadata}
	for row, key := range keys {
		if err := builder.Insert(key, uint64(row)); err != nil {
			tb.Fatal(err)
		}
		if err := mw.append([4]int64{int64(row) + 1, 2, 3, 4}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := builder.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := mw.finish(); err != nil {
		tb.Fatal(err)
	}
	index, err := vellum.Load(fst.Bytes())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = index.Close() })
	m, err := openSnapshotMetadata(metadata.Bytes())
	if err != nil {
		tb.Fatal(err)
	}
	s := &indexSnapshot{index: index, metadata: m}
	if withCursor {
		if s.keys, err = newFSTCursor(fst.Bytes()); err != nil {
			tb.Fatal(err)
		}
	}
	return s
}

// Old and new query paths must return the same names, kinds, node pointers,
// lookup counts and errors, with and without a mutable overlay. Segments make
// the separator order differ from name order ("ab", "ab-", "ab_", "ab.c"),
// names both metric and namespace, and dotted file names.
func TestSnapshotQueryFastPathMatchesOld(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(11))
	segments := []string{"a", "ab", "ab-", "ab_", "ab-c", "ab0", "b", "c", "-", "_", "wsp", "d.e", "空", "x"}
	patterns := append(append([]string(nil), segments...), "*", "ab*", "a?", "?", "{ab,ab_}", "{ab,ab-c,b}", "[a-b]*", "*-*", "[!a]*", "[", "{}")
	randomPath := func() string {
		parts := make([]string, random.Intn(4)+1)
		for j := range parts {
			parts[j] = segments[random.Intn(len(segments))]
		}
		return "/" + strings.Join(parts, "/") + ".wsp"
	}
	var ordered, total, fastQueries, merged int
	for trial := 0; trial < 60; trial++ {
		snapshotPaths := map[string]bool{}
		for i, n := 0, 1+random.Intn(800); i < n; i++ {
			snapshotPaths[randomPath()] = true
		}
		var mutable []string
		if trial%3 != 0 {
			for i, n := 0, random.Intn(40); i < n; i++ {
				if path := randomPath(); !snapshotPaths[path] {
					mutable = append(mutable, path)
				}
			}
		}
		paths := make([]string, 0, len(snapshotPaths))
		for path := range snapshotPaths {
			paths = append(paths, path)
		}
		withCursor := trial%5 != 4
		ti := newTrie(".wsp", 0, nil)
		ti.snapshot = buildFastPathSnapshot(t, paths, withCursor)
		for i, path := range mutable {
			if _, err := ti.insert(path, int64(-i-1), 2, 3, 4); err != nil {
				t.Fatal(err)
			}
		}
		for q := 0; q < 60; q++ {
			components := make([]string, random.Intn(4)+1)
			for j := range components {
				components[j] = patterns[random.Intn(len(patterns))]
			}
			query := strings.Join(components, "/")
			for _, limit := range []int{-1, 0, 1, 2, 3, 5, 17, 10000} {
				wn, wl, wd, wi, we := oldTrieQuery(ti, query, limit, nil)
				gn, gl, gd, gi, ge := ti.query(query, limit, nil)
				if (we != nil) != (ge != nil) || wi != gi {
					t.Fatalf("trial %d %q/%d: errors %v/%v lookups %d/%d", trial, query, limit, we, ge, wi, gi)
				}
				if len(wn) == 0 && len(gn) == 0 {
					continue
				}
				if !reflect.DeepEqual(wn, gn) || !reflect.DeepEqual(wl, gl) || !reflect.DeepEqual(wd, gd) {
					t.Fatalf("trial %d %q/%d (mutable %d):\n got %v %v\nwant %v %v", trial, query, limit, len(mutable), gn, gl, wn, wl)
				}
				sn, sl, sd, si, se := ti.snapshot.query(query, limit, nil)
				on, ol, od, oi, oe := oldSnapshotQuery(ti.snapshot, query, limit, nil)
				if (se != nil) != (oe != nil) || si != oi || !reflect.DeepEqual(sn, on) || !reflect.DeepEqual(sl, ol) || !reflect.DeepEqual(sd, od) {
					t.Fatalf("snapshot query %q/%d differs: got %v %v want %v %v", query, limit, sn, sl, on, ol)
				}
				total++
				if mn, ml, _, _, _ := ti.queryMutable(query, limit, nil); len(mn) > 0 && len(on) > 0 && snapshotResultsOrdered(on, ol) && snapshotResultsOrdered(mn, ml) {
					merged++
				}
				if snapshotResultsOrdered(on, ol) {
					ordered++
					if len(mutable) == 0 {
						fastQueries++
					}
				}
			}
		}
	}
	if ordered == 0 || ordered == total || merged == 0 {
		t.Fatalf("fixture does not cover both paths: %d of %d ordered", ordered, total)
	}
	t.Logf("%d of %d snapshot results already ordered, %d without overlay, %d merged", ordered, total, fastQueries, merged)
}

// The trie returns a same-name metric and namespace pair together even past
// the limit, whichever source holds each half.
func TestSnapshotQueryFastPathPairAtLimit(t *testing.T) {
	t.Parallel()
	for _, mutable := range [][]string{nil, {"/p/ab.wsp"}, {"/p/ab/z.wsp"}, {"/p/aa.wsp", "/p/zz/y.wsp"}} {
		ti := newTrie(".wsp", 0, nil)
		ti.snapshot = buildFastPathSnapshot(t, []string{"/p/ab/c.wsp", "/p/ab_/c.wsp", "/p/ab-/c.wsp", "/p/b.wsp", "/p/b/c.wsp", "/p/c/d.wsp"}, true)
		for _, path := range mutable {
			if _, err := ti.insert(path, 1, 2, 3, 4); err != nil {
				t.Fatal(err)
			}
		}
		for limit := -1; limit < 10; limit++ {
			wn, wl, wd, wi, we := oldTrieQuery(ti, "p/*", limit, nil)
			gn, gl, gd, gi, ge := ti.query("p/*", limit, nil)
			if we != nil || ge != nil || wi != gi || !reflect.DeepEqual(wn, gn) || !reflect.DeepEqual(wl, gl) || !reflect.DeepEqual(wd, gd) {
				t.Fatalf("mutable %v limit %d: got %v %v want %v %v", mutable, limit, gn, gl, wn, wl)
			}
		}
	}
}

func fastPathBenchmarkIndex(b *testing.B, children int, withCursor bool) *trieIndex {
	paths := make([]string, 0, children+2)
	for i := 0; i < children; i++ {
		paths = append(paths, fmt.Sprintf("/aggregations/secondly/service/by_feature_name/feature_%06d/count.wsp", i))
	}
	paths = append(paths, "/aggregations/secondly/other/by_feature_name/x/count.wsp", "/aggregations/minutely/service/by_feature_name/x/count.wsp")
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = buildFastPathSnapshot(b, paths, withCursor)
	return ti
}

// A wildcard over one namespace with 90k child namespaces and an empty
// mutable trie, the production find shape.
func BenchmarkSnapshotQueryWideDirectory(b *testing.B) {
	ti, noCursor := fastPathBenchmarkIndex(b, 90000, true), fastPathBenchmarkIndex(b, 90000, false)
	const expr = "aggregations/secondly/service/by_feature_name/*"
	for _, bench := range []struct {
		name  string
		index *trieIndex
		query func(*trieIndex, string, int, func([]string) ([]string, error)) ([]string, []bool, []*trieNode, uint32, error)
	}{{"old", ti, oldTrieQuery}, {"new", ti, (*trieIndex).query}, {"new-without-cursor", noCursor, (*trieIndex).query}} {
		b.Run(bench.name, func(b *testing.B) {
			// Compare warm node caches for every implementation.
			if _, _, _, _, err := bench.query(bench.index, expr, 1<<30, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				names, _, _, _, err := bench.query(bench.index, expr, 1<<30, nil)
				if err != nil || len(names) != 90000 {
					b.Fatalf("%d names, %v", len(names), err)
				}
			}
		})
	}
}
