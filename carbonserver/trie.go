package carbonserver

import (
	"os"
	"path/filepath"
	"strings"
)

type trieIndex struct {
	root      *trieNode
	depth     int
	fileExt   string
	fileCount int
}

type trieNode struct {
	val       string
	parent    *trieNode
	childrens []*trieNode
	isFile    bool
}

func newTrie(fileExt string) *trieIndex {
	return &trieIndex{root: &trieNode{}, fileExt: fileExt}
}

func (ti *trieIndex) insert(path string) {
	path = filepath.Clean(path)
	if path[0] == '/' {
		path = path[1:]
	}
	if path == "" || path == "." {
		return
	}

	nodes := strings.Split(path, string(os.PathSeparator))
	cur := ti.root
	if len(nodes) > ti.depth {
		ti.depth = len(nodes)
	}

outer:
	for i, node := range nodes {
		for _, child := range cur.childrens {
			if child.val == node {
				cur = child
				continue outer
			}
		}

		var isFile bool
		if i == len(nodes)-1 {
			isFile = strings.HasSuffix(node, ti.fileExt)
			node = strings.TrimSuffix(node, ti.fileExt)
			ti.fileCount++
		}
		tnode := &trieNode{val: node, parent: cur, isFile: isFile}
		cur.childrens = append(cur.childrens, tnode)
		cur = tnode
	}
}

// depth first search
func (ti *trieIndex) search(pattern string, limit int) (matched []string, isFile []bool) {
	// matched = make([]string, 0, limit)
	// isFile = make([]bool, 0, limit)
	var nodes = strings.Split(pattern, "/")
	var childIndex = make([]int, ti.depth+1)
	var curLevel int
	var cur = ti.root
	for {
		if len(cur.childrens) == 0 {
			goto parent
		}

		if childIndex[curLevel] == len(cur.childrens) {
			goto parent
		}

		cur = cur.childrens[childIndex[curLevel]]
		curLevel++

		if curLevel > len(nodes) {
			goto parent
		}

		// rework
		if ok, _ := filepath.Match(nodes[curLevel-1], cur.val); ok {
			if curLevel < len(nodes) {
				continue
			}

			matched = append(matched, cur.fullPath("."))
			isFile = append(isFile, cur.isFile)
			if len(matched) >= limit {
				break
			}

			goto parent
		} else {
			goto parent
		}

		continue

	parent:
		childIndex[curLevel] = 0
		curLevel--
		if curLevel < 0 {
			break
		}
		childIndex[curLevel]++
		cur = cur.parent
		continue
	}

	return matched, isFile
}

func (tn *trieNode) fullPath(sep string) string {
	var nodes []string
	cur := tn
	for cur.parent != nil {
		nodes = append(nodes, cur.val)
		cur = cur.parent
	}
	for i := 0; i < len(nodes)/2; i++ {
		tmp := nodes[i]
		nodes[i] = nodes[len(nodes)-i-1]
		nodes[len(nodes)-i-1] = tmp
	}

	return strings.Join(nodes, sep)
}

func (ti *trieIndex) allFiles(sep string) []string {
	var files = make([]string, 0, ti.fileCount)
	var childIndex = make([]int, ti.depth+1)
	var curLevel int
	var cur = ti.root
	for {
		if len(cur.childrens) == 0 {
			files = append(files, cur.fullPath(sep))
			goto parent
		}

		if childIndex[curLevel] == len(cur.childrens) {
			goto parent
		}

		cur = cur.childrens[childIndex[curLevel]]
		curLevel++
		continue

	parent:
		childIndex[curLevel] = 0
		curLevel--
		if curLevel < 0 {
			break
		}
		childIndex[curLevel]++
		cur = cur.parent
		continue
	}
	return files
}
