package carbonserver

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/blevesearch/vellum"
)

func TestSnapshotQueryMatchesTrie(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	trie := newTrie(".wsp", 0, nil)
	for _, entry := range entries {
		if _, err := trie.insert(entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt); err != nil {
			t.Fatal(err)
		}
	}
	queries := []string{"", "/", "*", "**", "a", "a*", "?", "[a-b]*", "[^x]*", "{a,b}", "{a*,b}", "{,a}*", "a/value", "a/*", "*/*", "b/child", "b/*", "b/child/*", "a-sibling/*", "a?/*", "空间/值", "空间/*", "*/?", "missing/*", "/a///value/", "a/[", "b/{x"}
	for _, query := range queries {
		for _, limit := range []int{0, 1, 2, 10000} {
			t.Run(fmt.Sprintf("%s/%d", query, limit), func(t *testing.T) {
				want, wl, wn, _, we := trie.query(query, limit, nil)
				got, gl, gn, _, ge := s.query(query, limit, nil)
				if (we != nil) != (ge != nil) {
					t.Fatalf("errors differ: %v / %v", we, ge)
				}
				if we != nil {
					return
				}
				canonical := func(names []string, leaves []bool, nodes []*trieNode) []string {
					var result []string
					for i, name := range names {
						row := fmt.Sprintf("%s/%t", name, leaves[i])
						if leaves[i] {
							m := nodes[i].meta.Load().(*fileMeta)
							row += fmt.Sprintf("/%d/%d/%d/%d", m.logicalSize, m.physicalSize, m.dataPoints, m.firstSeenAt)
						}
						result = append(result, row)
					}
					sort.Strings(result)
					return result
				}
				if g, w := canonical(got, gl, gn), canonical(want, wl, wn); !reflect.DeepEqual(g, w) {
					t.Fatalf("got %v want %v", g, w)
				}
			})
		}
	}
}

func TestSnapshotQuotaPrefixSums(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for _, prefix := range []string{"/", "a", "a-sibling", "a0", "b", "b.child", "空间", "missing"} {
		var want QuotaUsage
		dirs := make(map[string]bool)
		for _, entry := range entries {
			pathPrefix := "/" + strings.ReplaceAll(prefix, ".", "/") + "/"
			if prefix == "/" {
				pathPrefix = "/"
			}
			if prefix != "/" && !strings.HasPrefix(entry.Path, pathPrefix) {
				continue
			}
			want.Metrics++
			want.LogicalSize += entry.LogicalSize
			want.PhysicalSize += entry.PhysicalSize
			want.DataPoints += entry.DataPoints
			if path := strings.TrimPrefix(entry.Path, pathPrefix); strings.Contains(path, "/") {
				dirs[strings.SplitN(path, "/", 2)[0]] = true
			}
		}
		want.Namespaces = int64(len(dirs))
		got, err := s.namespaceUsage(prefix)
		if err != nil || got != want {
			t.Fatalf("prefix %q got %v/%v want %v", prefix, got, err, want)
		}
	}
}

// Query against the unchanged trie with many component shapes and name/namespace
// collisions, including the punctuation that differs from slash lexical order.
func TestSnapshotGeneratedGlobParity(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	paths := map[string]bool{}
	segments := []string{"a", "a-1", "a0", "a_b", "b", "b1", "b2", "long-prefix", "long-prefix-other", "0", "空间", "x", "y", "z"}
	for i := 0; i < 4000; i++ {
		parts := make([]string, random.Intn(5)+1)
		for j := range parts {
			parts[j] = segments[random.Intn(len(segments))]
		}
		paths["/"+strings.Join(parts, "/")+".wsp"] = true
	}
	keys := make([]string, 0, len(paths))
	for path := range paths {
		keys = append(keys, path)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := encodeSnapshotPath(nil, keys[i])
		b, _ := encodeSnapshotPath(nil, keys[j])
		return bytes.Compare(a, b) < 0
	})
	var fst, metadata bytes.Buffer
	builder, err := vellum.New(&fst, nil)
	if err != nil {
		t.Fatal(err)
	}
	mw := snapshotMetadataWriter{w: &metadata}
	oracle := newTrie(".wsp", 0, nil)
	for row, path := range keys {
		key, _ := encodeSnapshotPath(nil, path)
		if err := builder.Insert(key, uint64(row)); err != nil {
			t.Fatal(err)
		}
		if err := mw.append([4]int64{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
		if _, err := oracle.insert(path, 1, 2, 3, 4); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mw.finish(); err != nil {
		t.Fatal(err)
	}
	index, err := vellum.Load(fst.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	m, err := openSnapshotMetadata(metadata.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &indexSnapshot{index: index, metadata: m}
	patterns := append(append([]string(nil), segments...), "*", "a*", "?", "??", "[a-z]", "[!a-z]", "[^a-z]", "[0-9]*", "{a,b,x}", "{a*,long*}", "{,a}*", "*prefix*", "[", "a{", "{}")
	for i := 0; i < 700; i++ {
		components := make([]string, random.Intn(5)+1)
		for j := range components {
			components[j] = patterns[random.Intn(len(patterns))]
		}
		query := strings.Join(components, "/")
		want, wl, _, _, we := oracle.query(query, 10000, nil)
		got, gl, _, _, ge := snapshot.query(query, 10000, nil)
		if (we != nil) != (ge != nil) {
			t.Fatalf("error differs for %q: %v/%v", query, we, ge)
		}
		if we == nil && !reflect.DeepEqual(canonicalIndexResult(want, wl), canonicalIndexResult(got, gl)) {
			t.Fatalf("result differs for %q: got %v want %v", query, canonicalIndexResult(got, gl), canonicalIndexResult(want, wl))
		}
	}
}

func TestSnapshotNamespaceExistsMatchesFiles(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	lookup := s.namespaceLookup()
	cases := []string{"b.child.more", "b.child", "missing.child", "missing", "/", "", "a", "a-sibling", "a0", "a.value", "b", "b.child", "空间", "空间.值", "missing", "z", "a-siblin", "b.child.more"}
	for _, name := range cases {
		prefix := "/" + strings.ReplaceAll(name, ".", "/") + "/"
		if name == "/" || name == "" {
			prefix = "/"
		}
		want := false
		for _, entry := range entries {
			want = want || strings.HasPrefix(entry.Path, prefix)
		}
		if got := s.namespaceExists(name); got != want {
			t.Fatalf("namespace %q: got %v want %v", name, got, want)
		}
		if got := lookup(name); got != want {
			t.Fatalf("cached namespace %q: got %v want %v", name, got, want)
		}
	}
}
