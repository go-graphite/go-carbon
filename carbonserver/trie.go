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

type gstate struct {
	// TODO: make c compact
	c    [131]bool
	next []*gstate
}

type gdstate struct {
	next    [131]*gdstate
	gstates []*gstate

	cacheHit int
}

func (g *gmatcher) dstate() *gdstate { return g.dstates[len(g.dstates)-1] }
func (g *gmatcher) push(s *gdstate)  { g.dstates = append(g.dstates, s) }
func (g *gmatcher) pop() {
	if len(g.dstates) <= 1 {
		return
	}
	g.dstates = g.dstates[:len(g.dstates)-1]
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
func newGlobState(expr string) (*gmatcher, error) {
	var m = gmatcher{root: &gstate{}, exact: true, expr: expr}
	var cur = m.root
	var inAlter bool
	var alterStart, alterEnd *gstate
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch c {
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
				for i := 32; i <= 126; i++ {
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
		case '{':
			inAlter = true
			alterStart = &gstate{c: [131]bool{gstateSplit: true}}
			alterEnd = &gstate{c: [131]bool{gstateSplit: true}}
			cur.next = append(cur.next, alterStart)
			cur = alterStart
		case '}':
			inAlter = false
			cur.next = append(cur.next, alterEnd)
			cur = alterEnd
		case ',':
			if inAlter {
				cur.next = append(cur.next, alterEnd)
				cur = alterStart
				continue
			}

			// TODO: should return error?
			fallthrough
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
	return 32 <= c && c <= 126
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

		tnode := &trieNode{c: c /*parent: cur*/}
		cur.childrens = append(cur.childrens, tnode)
		cur = tnode
	}

	if !isFile {
		if cur.c != '/' {
			cur.childrens = append(cur.childrens, &trieNode{c: '/' /*parent: cur*/})
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
	var trieNodes = make([]*trieNode, ti.depth+1)
	var ncindex int
	var mindex int
	var curm = matchers[0]
	var ndstate *gdstate

	for {
		if nindex[ncindex] >= len(cur.childrens) {
			curm.pop()
			goto parent
		}

		trieNodes[ncindex] = cur
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

		if !(len(cur.childrens) > 0 && (cur.childrens[0].c == '/' || cur.childrens[0] == fileNode)) {
			continue
		}

		if !curm.dstate().matched() {
			curm.pop()
			goto parent
		}

		files = append(files, cur.fullPath('.', trieNodes[:ncindex]))
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
		cur = trieNodes[ncindex]

		if cur.c == '/' && nindex[ncindex] >= len(cur.childrens) {
			mindex--
			curm = matchers[mindex]

			goto parent
		}

		continue
	}

	return files, isFile, nil
}

func (tn *trieNode) fullPath(sep byte, parents []*trieNode) string {
	var r = make([]byte, len(parents))
	for i, n := range parents[1:] {
		if n.c == '/' {
			r[i] = sep
		} else {
			r[i] = n.c
		}
	}
	r[len(parents)-1] = tn.c

	return *(*string)(unsafe.Pointer(&r))
}

func (ti *trieIndex) allMetrics(sep byte) []string {
	var files = make([]string, 0, ti.fileCount)
	var nindex = make([]int, ti.depth+1)
	var ncindex int
	var cur = ti.root
	var trieNodes = make([]*trieNode, ti.depth+1)
	for {
		if nindex[ncindex] >= len(cur.childrens) {
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.childrens[nindex[ncindex]]
		ncindex++

		if len(cur.childrens) == 1 && cur.childrens[0] == fileNode {
			files = append(files, cur.fullPath(sep, trieNodes[:ncindex]))
			goto parent
		}

		continue

	parent:
		nindex[ncindex] = 0
		ncindex--
		if ncindex < 0 {
			break
		}
		nindex[ncindex]++
		cur = trieNodes[ncindex]
		continue
	}
	return files
}

func (listener *CarbonserverListener) expandGlobsTrie(query string) ([]string, []bool, error) {
	query = strings.Replace(query, ".", "/", -1)
	globs := []string{query}

	var bracesContainsSlash, inAlter bool
	for _, c := range query {
		if c == '{' {
			inAlter = true
		} else if c == '}' {
			inAlter = false
		} else if inAlter && c == '/' {
			bracesContainsSlash = true
			break
		}
	}
	if bracesContainsSlash {
		var err error
		globs, err = listener.expandGlobBraces(globs)
		if err != nil {
			return nil, nil, err
		}
	}

	var fidx = listener.CurrentFileIndex()
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
