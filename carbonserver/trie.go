package carbonserver

import (
	"errors"
	"math"
	"path/filepath"
	"strings"
	"unsafe"
)

const (
	gstateSplit  = 128
	gstateMatch  = 129
	gstateRanges = 130
)

var endGstate = &gstate{}

type gmatcher struct {
	exact   bool
	root    *gstate
	expr    string
	dstates []*gdstate
	dsindex int
}

func (g *gmatcher) dstate() *gdstate { return g.dstates[len(g.dstates)-1] }
func (g *gmatcher) push(s *gdstate)  { g.dstates = append(g.dstates, s) }
func (g *gmatcher) pop() {
	if len(g.dstates) <= 1 {
		return
	}
	g.dstates = g.dstates[:len(g.dstates)-1]
}

type gstate struct {
	// TODO: make c compact
	c    [131]bool
	next []*gstate
}

func (g *gstate) String() string {
	var toStr func(g *gstate) string

	ref := map[*gstate]string{}
	toStr = func(g *gstate) string {
		if r, ok := ref[g]; ok {
			return r
		}

		if g == endGstate {
			ref[g] = "end"
			return "end"
		}
		var b []byte
		for c, t := range g.c {
			if t {
				if isAlphanumeric(byte(c)) {
					b = append(b, byte(c))
				} else {
					b = append(b, '.')
				}
			}
		}
		var r = string(b)
		ref[g] = r + "(...)"
		for _, n := range g.next {
			r += "(" + toStr(n) + ")"
		}

		ref[g] = r
		return r
	}

	return toStr(g)
}

type gdstate struct {
	next    [131]*gdstate
	gstates []*gstate

	cacheHit int
}

func (g gdstate) String() string {
	var r []string
	for _, s := range g.gstates {
		r = append(r, s.String())
	}
	return strings.Join(r, ",")
}

func (g *gdstate) step(c byte) *gdstate {
	if g.next[c] != nil {
		g.cacheHit++
		return g.next[c]
	}

	var ng gdstate
	for _, s := range g.gstates {
		if s.c[c] || s.c['*'] {
			for _, ns := range s.next {
				ng.gstates = g.add(ng.gstates, ns)
			}
		}
	}
	g.next[c] = &ng
	return &ng
}

func (g *gdstate) add(list []*gstate, s *gstate) []*gstate {
	if s.c[gstateSplit] {
		for _, ns := range s.next {
			list = g.add(list, ns)
		}

		return list
	}

	list = append(list, s)
	return list
}

func (g *gdstate) matched() bool {
	for _, s := range g.gstates {
		if s == endGstate {
			return true
		}
	}
	return false
}

// TODO:
//     add range value validation
//     add ^ in range match
//     support {} cases
func newGlobState(expr string) (*gmatcher, error) {
	var m = gmatcher{root: &gstate{}, exact: true, expr: expr}
	var cur = m.root
	// var inAlternate bool
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch c {
		// case '{':
		// case '}':
		case '[':
			m.exact = false
			s := &gstate{}
			i++
			negative := expr[i] == '^'
			if negative {
				i++
			}
			for i < len(expr) && expr[i] != ']' {
				if expr[i] == '-' {
					if i+1 >= len(expr) {
						return nil, errors.New("glob: missing closing range")
					}

					for j := expr[i-1] + 1; j <= expr[i+1]; j++ {
						s.c[j] = true
					}

					i += 2
					continue
				}

				s.c[expr[i]] = true
				i++
			}
			if expr[i] != ']' {
				return nil, errors.New("glob: missing ]")
			}

			if negative {
				// TODO: revisit
				for i := 33; i <= 126; i++ {
					s.c[i] = !s.c[i]
				}
			}

			cur.next = append(cur.next, s)
			cur = s
		case '*':
			m.exact = false
			// de-dup multi stars: *** -> *
			for ; i+1 < len(expr) && expr[i+1] == '*'; i++ {
			}

			s := &gstate{}
			s.c['*'] = true

			var split, star gstate
			star.c['*'] = true
			split.c[gstateSplit] = true
			split.next = append(split.next, &star)
			cur.next = append(cur.next, &split)
			star.next = append(star.next, &split)

			cur = &split
		default:
			s := &gstate{}
			s.c[c] = true
			cur.next = append(cur.next, s)
			cur = s
		}
	}
	cur.next = append(cur.next, endGstate)

	var droot gdstate
	for _, s := range m.root.next {
		droot.gstates = droot.add(droot.gstates, s)
	}
	m.dstates = append(m.dstates, &droot)

	return &m, nil
}

func isAlphanumeric(c byte) bool {
	return 33 <= c && c <= 127
}

type trieIndex struct {
	root      *trieNode
	depth     int
	fileExt   string
	fileCount int
}

// TODO: use compact c (string/[]byte rather than byte)
type trieNode struct {
	c         byte
	parent    *trieNode
	childrens []*trieNode
}

var fileNode = &trieNode{}

func newTrie(fileExt string) *trieIndex {
	return &trieIndex{
		root:    &trieNode{},
		fileExt: fileExt,
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

	cur := ti.root
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
				cur = child
				continue outer
			}
		}

		tnode := &trieNode{c: c, parent: cur}
		cur.childrens = append(cur.childrens, tnode)
		cur = tnode
	}

	if !isFile {
		if cur.c != '/' {
			cur.childrens = append(cur.childrens, &trieNode{c: '/', parent: cur})
		}
		return
	}

	cur.childrens = append(cur.childrens, fileNode)
	ti.fileCount++
}

// depth first search
func (ti *trieIndex) search(pattern string, limit int) (files []string, isFile []bool, err error) {
	var matchers []*gmatcher
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
		matchers = append(matchers, gs)
	}

	var cur = ti.root
	var nindex = make([]int, ti.depth+1)
	var ncindex int
	var mindex int
	var curm = matchers[0]
	var ndstate *gdstate

	for {
		if nindex[ncindex] >= len(cur.childrens) {
			curm.pop()
			goto parent
		}

		cur = cur.childrens[nindex[ncindex]]
		ncindex++

		// log.Printf("cur.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', ti.depth))
		// log.Printf("curm.dstate = %+v\n", curm.dstate())

		if cur.c == '/' {
			if mindex+1 >= len(matchers) || !curm.dstate().matched() {
				goto parent
			}

			mindex++
			curm = matchers[mindex]
			curm.dstates = curm.dstates[:1]

			continue
		}

		ndstate = curm.dstate().step(cur.c)
		if len(ndstate.gstates) == 0 {
			goto parent
		}

		curm.push(ndstate)

		if mindex+1 < len(matchers) {
			continue
		}

		if !(cur.childrens[0].c == '/' || cur.childrens[0] == fileNode) {
			continue
		}

		if !curm.dstate().matched() {
			curm.pop()
			goto parent
		}

		files = append(files, cur.fullPath('.', ti.depth))
		isFile = append(isFile, cur.childrens[0] == fileNode)
		if len(files) >= limit || exact {
			break
		}
		curm.pop()
		goto parent

	parent:
		// use exact for fast exit
		nindex[ncindex] = 0
		ncindex--
		if ncindex < 0 {
			break
		}
		nindex[ncindex]++
		cur = cur.parent

		if cur.c == '/' && nindex[ncindex] >= len(cur.childrens) {
			mindex--
			curm = matchers[mindex]

			goto parent
		}

		continue
	}

	return files, isFile, nil
}

func (tn *trieNode) fullPath(sep byte, depth int) string {
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
