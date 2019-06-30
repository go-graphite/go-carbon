package carbonserver

import (
	"errors"
	"log"
	"math"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type globState struct {
	// c        byte
	// ranges   *[256]bool      // [a-z0-9A-Z]
	// depth int
	exact bool
	root  *globNode

	cur  *globNode
	expr string
}

type globNode struct {
	end        bool
	star       bool
	ranges     bool
	parent     *globNode
	starParent *globNode
	children   [256]*globNode // make it smaller

	matchFrom, matchUntil *trieNode

	posStart, posEnd int
}

func (gn *globNode) isNodeEnd() bool {
	return gn.end || gn.children['/'] != nil
}

func (gs *globState) state(gn *globNode) string {
	return gs.expr[gn.posStart:gn.posEnd]
}

func (gs *globState) reset() {
	gs.cur = gs.root

	cur := gs.root
reset:
	for cur != nil {
		cur.matchFrom = nil
		cur.matchUntil = nil
		for i := 0; i < 256; i++ {
			if cur.children[i] != nil {
				cur = cur.children[i]
				continue reset
			}
		}

		break
	}
}

// TODO:
//     add range value validation
//     add ^ in range match
func newGlobState(expr string) (*globState, error) {
	var gs = globState{root: &globNode{}, exact: true, expr: expr}
	var cur = gs.root
	var curStar *globNode
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch c {
		case '[':
			gs.exact = false
			// var ranges [256]bool
			s := &globNode{parent: cur, starParent: curStar, posStart: i, ranges: true}
			i++
			for i < len(expr) && expr[i] != ']' {
				if expr[i] == '-' {
					if i+1 >= len(expr) {
						return nil, errors.New("glob: missing closing range")
					}

					for j := expr[i-1] + 1; j <= expr[i+1]; j++ {
						// log.Printf("j = %+v\n", string(j))
						cur.children[j] = s
					}

					i += 2
					continue
				}

				// log.Printf("expr[i] = %+v\n", string(expr[i]))
				cur.children[expr[i]] = s
				i++
			}
			if expr[i] != ']' {
				return nil, errors.New("glob: missing ]")
			}

			// log.Printf("s.children = %+v\n", cur.children)
			s.posEnd = i + 1
			cur = s
		case '*':
			gs.exact = false
			// de-dup multi stars: *** -> *
			for ; i+1 < len(expr) && expr[i+1] == '*'; i++ {
			}

			s := &globNode{parent: cur, starParent: curStar, star: true, posStart: i, posEnd: i + 1}
			for i := 0; i < 256; i++ {
				cur.children[c] = s
			}
			if curStar == nil {
				s.starParent = s
			}

			cur = s
			curStar = cur
		default:
			s := &globNode{parent: cur, starParent: curStar, posStart: i, posEnd: i + 1}
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
	var exact bool
	for _, node := range strings.Split(pattern, "/") {
		if node == "" {
			return nil, nil, errors.New("empty node in query")
		}
		gs, err := newGlobState(node)
		if err != nil {
			return nil, nil, err
		}
		exact = exact && gs.exact
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
	curGS.reset()

	var gst *globNode
	var matched bool
	var matchedByStar bool
	_ = matched
	_ = matchedByStar
	for {
		if childIndex[curLevel] >= len(cur.childrens) {
			if curGS.cur.parent != nil && curGS.cur.matchFrom == cur {
				curGS.cur = curGS.cur.parent
			}

			goto parent
		}

		cur = cur.childrens[childIndex[curLevel]]
		curLevel++

		log.Printf("cur.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', ti.depth))
		log.Printf("curGS.state(curGS.cur) = %+v\n", curGS.state(curGS.cur))

		// TODO:
		//  * quit early for exact match
		//  * use string id for exact match

		// TODO: not right?
		if cur.c == '/' {
			if gsIndex+1 >= len(gstates) || !curGS.cur.end {
				goto parent
			}

			gsIndex++
			curGS = gstates[gsIndex]
			curGS.reset()

			continue
		}

		gst = curGS.cur.children[cur.c]
		matched = gst != nil
		matchedByStar = false

		if !matched {
			if curGS.cur.star {
				gst = curGS.cur
				matched = true
				// } else {
				// 	gst = curGS.cur.children['*']
				// 	matchedByStar = gst != nil
				// 	matched = matchedByStar
			}
		}

		if !matched {
			if curGS.cur.starParent == nil {
				goto parent
			}
			// goto parent

			gst = curGS.cur.starParent
			// gst.matchUntil = cur

			// continue
		}

		// log.Printf("c = %s matched %t\n", string(cur.c), curGS.children[cur.c] != nil)

		if gst.matchFrom == nil {
			gst.matchFrom = cur
		}

		curGS.cur = gst
		if !curGS.cur.end {
			continue
		}

		// continue matching if the current glob state a trailing star
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
		if len(files) >= limit || exact {
			break
		}
		goto parent

	parent:
		// use exact for fast exit
		childIndex[curLevel] = 0
		curLevel--
		if curLevel < 0 {
			break
		}
		childIndex[curLevel]++
		cur = cur.parent

		// log.Printf("curGS.state(curGS.cur) = %+v\n", curGS.state(curGS.cur))

		// log.Printf("cur.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', ti.depth))

		if cur.c == '/' && childIndex[curLevel] >= len(cur.childrens) {
			gsIndex--
			curGS = gstates[gsIndex]
			// curGS.cur = curGS.cur.parent

			goto parent
		}

		// log.Printf("cur.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', ti.depth))
		// log.Printf("curGS.state(curGS.cur) = %+v\n", curGS.state(curGS.cur))
		// if curGS.cur.ranges {
		// 	curGS.cur = curGS.cur.parent
		// 	log.Printf("curGS.state(curGS.cur) = %+v\n", curGS.state(curGS.cur))
		// }

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

func (listener *CarbonserverListener) expandGlobsTrie(query string) ([]string, []bool, error) {
	var useGlob bool
	logger := zapwriter.Logger("carbonserver")

	// TODO: Find out why we have set 'useGlob' if 'star == -1'
	if star := strings.IndexByte(query, '*'); strings.IndexByte(query, '[') == -1 && strings.IndexByte(query, '?') == -1 && (star == -1 || star == len(query)-1) {
		useGlob = true
	}
	logger = logger.With(zap.Bool("use_glob", useGlob))

	/* things to glob:
	 * - carbon.relays  -> carbon.relays
	 * - carbon.re      -> carbon.relays, carbon.rewhatever
	 * - carbon.[rz]    -> carbon.relays, carbon.zipper
	 * - carbon.{re,zi} -> carbon.relays, carbon.zipper
	 * - match is either dir or .wsp file
	 * unfortunately, filepath.Glob doesn't handle the curly brace
	 * expansion for us */

	query = strings.Replace(query, ".", "/", -1)

	var globs []string
	globs = append(globs, query)
	// TODO(dgryski): move this loop into its own function + add tests
	for {
		bracematch := false
		var newglobs []string
		for _, glob := range globs {
			lbrace := strings.Index(glob, "{")
			rbrace := -1
			if lbrace > -1 {
				rbrace = strings.Index(glob[lbrace:], "}")
				if rbrace > -1 {
					rbrace += lbrace
				}
			}

			if lbrace > -1 && rbrace > -1 {
				bracematch = true
				expansion := glob[lbrace+1 : rbrace]
				parts := strings.Split(expansion, ",")
				for _, sub := range parts {
					if len(newglobs) > listener.maxGlobs {
						if listener.failOnMaxGlobs {
							return nil, nil, errMaxGlobsExhausted
						}
						break
					}
					newglobs = append(newglobs, glob[:lbrace]+sub+glob[rbrace+1:])
				}
			} else {
				if len(newglobs) > listener.maxGlobs {
					if listener.failOnMaxGlobs {
						return nil, nil, errMaxGlobsExhausted
					}
					break
				}
				newglobs = append(newglobs, glob)
			}
		}
		globs = newglobs
		if !bracematch {
			break
		}
	}

	fidx := listener.CurrentFileIndex()

	var files []string
	var leafs []bool
	for _, g := range globs {
		f, l, err := fidx.trieIdx.search(g, math.MaxInt64)
		if err != nil {
			panic(err)
		}
		files = append(files, f...)
		leafs = append(leafs, l...)
	}
	return files, leafs, nil
}
