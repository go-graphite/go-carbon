package carbonserver

import (
	"runtime"
	"strings"
)

// isExactMetricName reports whether the trie query for name has no glob
// syntax, so it can only match the metric and the namespace called name.
// Besides the glob characters it refuses everything the query normalizes or
// splits differently: surrounding whitespace, empty components, '/' (a
// separator as well), NUL (the snapshot separator), ',' and '\'. Such names
// keep using the glob query.
func isExactMetricName(name string) bool {
	if name == "" || name[0] == '.' || name[len(name)-1] == '.' || strings.TrimSpace(name) != name {
		return false
	}
	for i := 0; i < len(name); i++ {
		switch name[i] {
		case '*', '?', '[', ']', '{', '}', ',', '\\', '/', 0:
			return false
		case '.':
			if name[i+1] == '.' {
				return false
			}
		}
	}
	return true
}

// queryExact returns what query returns for a name accepted by
// isExactMetricName: the same names, kinds, nodes and lookup count, without
// compiling a glob matcher per component or searching the snapshot with it.
func (ti *trieIndex) queryExact(name string, limit int) ([]string, []bool, []*trieNode, uint32, error) {
	if ti.snapshot == nil {
		files, leaves, nodes, lookups := ti.queryMutableExact(name, limit)
		return files, leaves, nodes, lookups, nil
	}
	names, leaves, nodes, lookups, err := ti.snapshot.queryExact(name, limit)
	if err != nil {
		return nil, nil, nil, lookups, err
	}
	extra, el, en, its := ti.queryMutableExact(name, limit)
	lookups += its
	names, leaves, nodes = ti.combineSnapshotResults(names, leaves, nodes, extra, el, en, limit)
	return names, leaves, nodes, lookups, nil
}

// queryExact answers snapshot query for a literal name with two point
// probes: the namespace prefix "<name>/" and the metric key "<name>.wsp".
// The automaton search visits the namespace first (NUL sorts before '.'),
// emits the paired metric with it and then the metric key once more, so the
// lookup count follows that order and the limit.
func (s *indexSnapshot) queryExact(name string, limit int) (names []string, leaves []bool, nodes []*trieNode, lookups uint32, err error) {
	defer runtime.KeepAlive(s)
	key := snapshotNamespacePrefix(name)
	var dir, leaf bool
	var row uint64
	if c := s.keys; c != nil {
		state := c.acceptBytes(c.start(), key[:len(key)-1])
		dir = c.accept(state, 0).ok
		leaf, row = c.final(c.acceptString(state, ".wsp"))
	} else {
		dir = s.namespaceExists(name)
		key = append(key[:len(key)-1], ".wsp"...)
		if row, leaf, err = s.index.Get(key); err != nil {
			return nil, nil, nil, 0, err
		}
	}
	if !dir && !leaf {
		return nil, nil, nil, 0, nil
	}
	lookups = 1
	if leaf {
		node, err := s.fileNode(name, row)
		if err != nil {
			return nil, nil, nil, lookups, err
		}
		names, leaves, nodes = append(names, name), append(leaves, true), append(nodes, node)
	}
	if dir {
		names, leaves, nodes = append(names, name), append(leaves, false), append(nodes, s.directoryNode(name))
		if leaf && len(names) < limit {
			lookups++
		}
	}
	return names, leaves, nodes, lookups, nil
}

// queryMutableExact returns what queryMutable returns for a name accepted by
// isExactMetricName. It walks the trie in the same order, visiting the same
// children, so results, node pointers and the lookup count are identical;
// only the per-component glob matchers and the walk buffers are gone.
func (ti *trieIndex) queryMutableExact(name string, limit int) ([]string, []bool, []*trieNode, uint32) {
	w := exactTrieWalk{
		name:     name,
		comps:    strings.Split(name, "."),
		limit:    limit,
		maxLevel: int(ti.getDepth()+trieDepthBuffer) - 1,
	}
	w.visit(ti.root.getChildrens(), 0, 0, 1)
	return w.files, w.leaves, w.nodes, w.lookups
}

type exactTrieWalk struct {
	name     string
	comps    []string
	limit    int
	maxLevel int // queryMutable skips nodes this deep

	files   []string
	leaves  []bool
	nodes   []*trieNode
	lookups uint32
	done    bool
}

// visit descends into each of children like queryMutable: comp is the
// component being matched, pos how many of its bytes the ancestors matched,
// and level queryMutable's depth after the descent.
func (w *exactTrieWalk) visit(children []*trieNode, comp, pos, level int) {
	for i := range children {
		if w.done {
			return
		}
		child := (*trieNode).getChild(nil, children, i)
		w.lookups++
		if level >= w.maxLevel {
			continue
		}
		grand := child.getChildrens()
		if child.dir() {
			if comp+1 < len(w.comps) && pos == len(w.comps[comp]) && len(grand) > 0 {
				w.visit(grand, comp+1, 0, level+1)
			}
			continue
		}
		next, ok := matchExactLabel(w.comps[comp], pos, child.c)
		if !ok {
			continue
		}
		if comp+1 < len(w.comps) {
			w.visit(grand, comp, next, level+1)
			continue
		}
		var fileNode, dirNode *trieNode
		var hasMoreNodes bool
		for j := range grand {
			switch g := child.getChild(grand, j); {
			case g.file():
				fileNode = g
			case g.dir():
				dirNode = g
			default:
				hasMoreNodes = true
			}
		}
		if fileNode == nil && dirNode == nil {
			w.visit(grand, comp, next, level+1)
			continue
		}
		if next != len(w.comps[comp]) {
			if hasMoreNodes {
				w.visit(grand, comp, next, level+1)
			}
			continue
		}
		if fileNode != nil {
			w.files, w.leaves, w.nodes = append(w.files, w.name), append(w.leaves, true), append(w.nodes, fileNode)
		}
		if dirNode != nil {
			w.files, w.leaves, w.nodes = append(w.files, w.name), append(w.leaves, false), append(w.nodes, dirNode)
		}
		if len(w.files) >= w.limit {
			w.done = true
			return
		}
		if hasMoreNodes {
			w.visit(grand, comp, next, level+1)
		}
	}
}

// matchExactLabel matches a node label against comp from pos and returns
// the position after it.
func matchExactLabel(comp string, pos int, label []byte) (int, bool) {
	if len(label) > len(comp)-pos || comp[pos:pos+len(label)] != string(label) {
		return pos, false
	}
	return pos + len(label), true
}
