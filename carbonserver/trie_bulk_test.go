package carbonserver

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"
	"weak"
)

func TestTrieBulkPruningReclaimsNodes(t *testing.T) {
	trie, removed := bulkTreeForReclamation(t)
	// A private builder must not pin retired nodes through shared pointer-bearing
	// allocation blocks. Queries may still hold a node during pruning; once those
	// references are gone, the runtime must be able to reclaim it.
	for i := 0; i < 10; i++ {
		runtime.GC()
		if removed.Value() == nil {
			runtime.KeepAlive(trie)
			return
		}
	}
	runtime.KeepAlive(trie)
	t.Fatal("pruned bulk node is still retained")
}

func bulkTreeForReclamation(t *testing.T) (*trieIndex, weak.Pointer[trieNode]) {
	t.Helper()
	trie := newTrie(".wsp", 0, nil)
	trie.builder = &trieBulkBuilder{}
	removed, err := trie.insert("/retired/subtree/metric.wsp", 1, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := trie.insert("/retained/metric.wsp", 1, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	reference := weak.Make(removed)
	trie.builder = nil
	trie.root.gen++
	if _, err := trie.insert("/retained/metric.wsp", 1, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	trie.prune()
	return trie, reference
}

// TestTrieBulkMatchesIncremental checks insertion order, duplicate metadata,
// radix splits, directory/file collisions, quotas and the subsequent live update.
func TestTrieBulkMatchesIncremental(t *testing.T) {
	paths := []string{"/abc/def.wsp", "/ab/def.wsp", "/abc.wsp", "/abc", "/abcdef/long.wsp", "/系统/核心/cpu.wsp", "/duplicate.wsp", "/duplicate.wsp"}
	for i := 0; i < 2000; i++ {
		paths = append(paths, fmt.Sprintf("/ns%d/host%04d/cpu%d.wsp", i%13, i%179, i%11))
	}
	for seed := int64(0); seed < 10; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			order := rand.New(rand.NewSource(seed)).Perm(len(paths))
			reference, bulk := newTrie(".wsp", 0, nil), newTrie(".wsp", 0, nil)
			bulk.builder = &trieBulkBuilder{}
			for _, i := range order {
				for _, trie := range []*trieIndex{reference, bulk} {
					if _, err := trie.insert(paths[i], int64(i+1), int64(i+2), int64(i+3), 1700000000-int64(i)); err != nil {
						t.Fatal(err)
					}
				}
			}
			reference.prune()
			count, files, dirs, _, _, _, _, _ := reference.countNodes()
			if bulk.builder.nodes != count || bulk.builder.dirs != dirs || bulk.fileCount != files {
				t.Fatalf("bulk counts = %d/%d/%d, reference = %d/%d/%d", bulk.builder.nodes, bulk.fileCount, bulk.builder.dirs, count, files, dirs)
			}
			bulk.builder = nil // the publication boundary
			checkTrieEquivalent(t, reference, bulk)
			for _, trie := range []*trieIndex{reference, bulk} {
				throughputs, err := trie.applyQuotas(time.Minute, &Quota{Pattern: "/", Metrics: 10000}, &Quota{Pattern: "ns1", Metrics: 100})
				if err != nil {
					t.Fatal(err)
				}
				trie.refreshUsage(throughputs)
			}
			if !reflect.DeepEqual(reference.root.meta.Load().(*dirMeta).usage, bulk.root.meta.Load().(*dirMeta).usage) {
				t.Fatal("bulk root usage differs")
			}
			for _, trie := range []*trieIndex{reference, bulk} {
				trie.root.gen++
				for i, path := range paths {
					if i%2 == 0 {
						if _, err := trie.insert(path, 200, 300, 10, 1700000010); err != nil {
							t.Fatal(err)
						}
					}
				}
				trie.prune()
			}
			checkTrieEquivalent(t, reference, bulk)
		})
	}
}

func checkTrieEquivalent(t *testing.T, reference, bulk *trieIndex) {
	t.Helper()
	for _, sep := range []byte{'.', '/'} {
		if !reflect.DeepEqual(reference.allMetrics(sep), bulk.allMetrics(sep)) {
			t.Fatal("metric enumeration differs")
		}
	}
	for _, glob := range []string{"*", "abc", "abc/*", "ab*/*", "系统/核心/*", "ns*/host00?*/cpu[0-5]", "ns{1,2}/host*/*", "missing/*"} {
		a, af, an, _, ae := reference.query(glob, 100000, nil)
		b, bf, bn, _, be := bulk.query(glob, 100000, nil)
		if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(af, bf) || !reflect.DeepEqual(ae, be) {
			t.Fatalf("query %q differs", glob)
		}
		for i := range an {
			if af[i] && !reflect.DeepEqual(an[i].meta.Load(), bn[i].meta.Load()) {
				t.Fatalf("metadata differs for %q", a[i])
			}
		}
	}
}
