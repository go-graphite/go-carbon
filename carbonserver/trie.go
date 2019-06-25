package carbonserver

import (
	"errors"
	"log"
	"path/filepath"
	"strings"
	"unsafe"
)

type globState struct {
	// c        byte
	// ranges   *[256]bool      // [a-z0-9A-Z]
	// depth int
	exact bool
	root  *globNode

	cur *globNode
}

type globNode struct {
	end        bool
	star       bool
	parent     *globNode
	starParent *globNode
	children   [256]*globNode // make it smaller
}

func (gn *globNode) isNodeEnd() bool {
	return gn.end || gn.children['/'] != nil
}

// TODO: add range value validation
func newGlobState(expr string) (*globState, error) {
	var gs = globState{root: &globNode{}, exact: true}
	var cur = gs.root
	var curStar *globNode
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch c {
		case '[':
			gs.exact = false
			// var ranges [256]bool
			s := &globNode{parent: cur, starParent: curStar}
			for i < len(expr) && expr[i] != ']' {
				if expr[i] == '-' {
					if i+1 >= len(expr) {
						return nil, errors.New("glob: missing closing range")
					}

					for j := expr[i-1]; j <= expr[i+1]; j++ {
						cur.children[j] = s
					}

					i += 2
					continue
				}

				cur.children[expr[i]] = s
				i++
			}
			if expr[i] != ']' {
				return nil, errors.New("glob: missing ]")
			}

			// cur.children[c] = s
			cur = s
		case '*':
			gs.exact = false
			// de-dup multi stars: *** -> *
			for ; i+1 < len(expr) && expr[i+1] == '*'; i++ {
			}
			s := &globNode{parent: cur, starParent: curStar, star: true}
			cur.children[c] = s
			if curStar == nil {
				s.starParent = s
			}

			cur = s
			curStar = cur
		default:
			s := &globNode{parent: cur, starParent: curStar}
			cur.children[c] = s
			cur = s
		}
	}
	cur.end = true

	return &gs, nil
}

// func (gs *globState) dump() string {
// 	var r string
// 	cur := gs
// 	for cur != nil {
// 		var v string
// 		if cur.c != 0 {
// 			v = string(cur.c)
// 		} else {
// 			for c, ok := range cur.ranges {

// 			}
// 		}
// 		fmt.Printf("%s%s", pad, v)
// 	}
// }

type trieIndex struct {
	root *trieNode
	// nodeDepth int
	// byteDepth int
	depth     int
	fileExt   string
	fileCount int

	// stringIDs map[string]int
}

type trieNode struct {
	c byte
	// node      string
	parent    *trieNode
	childrens []*trieNode
	isFile    bool
	isNodeEnd bool
}

func newTrie(fileExt string) *trieIndex {
	return &trieIndex{
		root:    &trieNode{},
		fileExt: fileExt,
		// stringIDs: map[string]int{},
	}
}

// abc.def.ghi
// abc.def2.ghi
func (ti *trieIndex) insert(path string) {
	path = filepath.Clean(path)
	if path[0] == '/' {
		path = path[1:]
	}
	if path == "" || path == "." {
		return
	}

	// nodes := strings.Split(path, string(os.PathSeparator))
	cur := ti.root
	// if len(nodes) > ti.nodeDepth {
	// 	ti.nodeDepth = len(nodes)
	// }
	// var bdepth int
	// for _, node := range nodes {
	// 	bdepth += len(node)
	// }
	// if bdepth > ti.byteDepth {
	// 	ti.byteDepth = bdepth
	// }
	if len(path) > ti.depth {
		ti.depth = len(path)
	}

	isFile := strings.HasSuffix(path, ti.fileExt)
	if isFile {
		path = path[:len(path)-len(ti.fileExt)]
	}

outer:
	for i := 0; i < len(path); i++ {
		c := path[i]
		for _, child := range cur.childrens {
			if child.c == byte(c) {
				// if child.val == node {
				cur = child
				continue outer
			}
		}

		tnode := &trieNode{c: c, parent: cur}
		cur.childrens = append(cur.childrens, tnode)
		cur = tnode
		cur.parent.isNodeEnd = c == '/'
	}

	if isFile {
		cur.isFile = true
		cur.isNodeEnd = true
		ti.fileCount++
	}

	// var isFile bool
	// if i == len(path)-1 {
	// 	isFile = strings.HasSuffix(node, ti.fileExt)
	// 	node = strings.TrimSuffix(node, ti.fileExt)
	// 	ti.fileCount++
	// }
}

// depth first search
func (ti *trieIndex) search(pattern string, limit int) (files []string, isFile []bool, err error) {
	var gstates []*globState
	for _, node := range strings.Split(pattern, "/") {
		if node == "" {
			return nil, nil, errors.New("empty node in query")
		}
		gs, err := newGlobState(node)
		if err != nil {
			return nil, nil, err
		}
		gstates = append(gstates, gs)
	}

	// matched = make([]string, 0, limit)
	// isFile = make([]bool, 0, limit)

	// var nodes = strings.Split(pattern, "/")
	var childIndex = make([]int, ti.depth+1)
	var gsIndex int
	var curLevel int
	var cur = ti.root
	var curGS = gstates[0]
	curGS.cur = curGS.root
	// var prevGS = gs

	var gst *globNode
	var matched bool
	var matchedByStar bool
	_ = matched
	_ = matchedByStar
	for {
		if len(cur.childrens) == 0 {
			goto parent
		}

		if childIndex[curLevel] == len(cur.childrens) {
			goto parent
		}

		cur = cur.childrens[childIndex[curLevel]]
		curLevel++

		// if curLevel > len(pattern) {
		// 	goto parent
		// }

		// TODO:
		//  * quit early for exact match
		//  * use string id for exact match

		// rework
		// if ok, _ := filepath.Match(nodes[curLevel-1], cur.val); ok {
		// 	if curLevel < len(nodes) {
		// 		continue
		// 	}

		// TODO: not right?
		log.Printf("cur.c = %+v\n", string(cur.c))
		if cur.c == '/' {
			// if !gstates[gsIndex].cur.end || gsIndex+1 >= len(gstates) {
			// 	goto parent
			// }

			// gsIndex++
			// curGS = gstates[gsIndex]
			// curGS.cur = curGS.root

			// if gsIndex+1 < len(gstates) {
			// 	// nextNode:
			// 	// 	for _, child := range cur.childrens {
			// 	// 		if child.c == '/' {
			// 	// 			gsIndex++
			// 	// 			curGS = gstates[gsIndex]
			// 	// 			curGS.cur = curGS.root

			// 	// 			cur = child
			// 	// 			curLevel++

			// 	// 			break nextNode
			// 	// 		}
			// 	// 	}
			// 	// 	continue
			// }

			if gsIndex+1 >= len(gstates) {
				goto parent
			}
			gsIndex++
			curGS = gstates[gsIndex]
			curGS.cur = curGS.root

			// log.Printf("gsIndex = %+v\n", gsIndex)

			// cur = cur.childrens[childIndex[curLevel]]
			// curLevel++
			continue
		}

		gst = curGS.cur.children[cur.c]
		matched = gst != nil
		matchedByStar = false

		if !matched {
			if !curGS.cur.star {
				gst = curGS.cur.children['*']
				matchedByStar = gst != nil
				matched = matchedByStar
			} else {
				gst = curGS.cur
				matched = true
			}
		}

		if !matched {
			// if curGS.cur.starParent == nil {
			// goto parent
			// }
			goto parent

			// gst = curGS.cur.starParent
			// continue
		}

		// log.Printf("c = %s matched %t\n", string(cur.c), curGS.children[cur.c] != nil)

		curGS.cur = gst
		if !curGS.cur.end {
			// if !matchedByStar {
			// 	curGS.cur = gst
			// 	continue
			// }

			// if gst.children['/'] != nil {
			// 	//
			// }
			continue
		}

		// continue matching if the current glob state a trailing star
		// log.Printf("cur.c = %+v\n", string(cur.c))
		// log.Printf("!cur.isNodeEnd  = %+v\n", !cur.isNodeEnd)
		// log.Printf("curGS.cur.parent != nil  = %+v\n", curGS.cur.parent != nil)
		// log.Printf("curGS.cur == curGS.cur.parent.children['*'] = %+v\n", curGS.cur == curGS.cur.parent.children['*'])

		if !cur.isNodeEnd && curGS.cur.star {
			// .parent != nil && curGS.cur == curGS.cur.parent.children['*']
			continue
		}

		// log.Printf("cur.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', ti.depth))
		// log.Printf("gst.end = %+v\n", gst.end)
		// log.Printf("gsIndex = %+v\n", gsIndex)
		if gsIndex+1 < len(gstates) {
			// gsIndex++
			// curGS = gstates[gsIndex]
			// curGS.cur = curGS.root
			continue
		}

		files = append(files, cur.fullPath('.', ti.depth))
		isFile = append(isFile, cur.isFile)
		if len(files) >= limit {
			break
		}
		goto parent

		// continue

	parent:
		// use exact for fast exit
		childIndex[curLevel] = 0
		curLevel--
		if curLevel < 0 {
			break
		}
		childIndex[curLevel]++
		cur = cur.parent

		if cur.c == '/' && childIndex[curLevel] >= len(cur.childrens) {
			gsIndex--
			curGS = gstates[gsIndex]
			goto parent
		}
		continue
	}

	return files, isFile, nil
}

func (tn *trieNode) fullPath(sep byte, depth int) string {
	// var nodes = make([]string, 0, depth)
	// cur := tn
	// for cur.parent != nil {
	// 	nodes = append(nodes, cur.val)
	// 	cur = cur.parent
	// }
	// for i := 0; i < len(nodes)/2; i++ {
	// 	tmp := nodes[i]
	// 	nodes[i] = nodes[len(nodes)-i-1]
	// 	nodes[len(nodes)-i-1] = tmp
	// }

	// return strings.Join(nodes, sep)

	var r = make([]byte, depth)
	var index = depth - 1
	var cur = tn
	for cur.parent != nil {
		if cur.c == '/' {
			r[index] = sep
		} else {
			r[index] = cur.c
		}
		index--
		cur = cur.parent
	}
	r = r[index+1:]

	return *(*string)(unsafe.Pointer(&r))
}

func (ti *trieIndex) allFiles(sep byte) []string {
	var files = make([]string, 0, ti.fileCount)
	var childIndex = make([]int, ti.depth+1)
	var curLevel int
	var cur = ti.root
	for {
		if len(cur.childrens) == 0 {
			files = append(files, cur.fullPath(sep, ti.depth))
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
