package carbonserver

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"

	trigram "github.com/dgryski/go-trigram"
)

// debug codes diff for reference: https://play.golang.org/p/FxuvRyosk3U

// dfa inspiration: https://swtch.com/~rsc/regexp/

const (
	gstateSplit = 128
)

var endGstate = &gstate{}

type gmatcher struct {
	exact   bool
	root    *gstate
	expr    string
	dstates []*gdstate
	dsindex int

	// has leading star following by complex expressions
	lsComplex bool
	// trigrams  roaring.Bitmap
	trigrams []uint32
}

type gstate struct {
	// TODO: make c compact
	c    [131]bool
	next []*gstate
}

type gdstate struct {
	gstates []*gstate
	// next    [131]*gdstate
	// cacheHit int
}

func (g *gmatcher) dstate() *gdstate { return g.dstates[len(g.dstates)-1] }
func (g *gmatcher) push(s *gdstate)  { g.dstates = append(g.dstates, s) }
func (g *gmatcher) pop(i int) {
	if len(g.dstates) <= i {
		g.dstates = g.dstates[:1]
		return
	}
	g.dstates = g.dstates[:len(g.dstates)-i]
}

func (g *gstate) String() string {
	var toStr func(g *gstate) string
	var ref = map[*gstate]string{}
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
	// if g.next[c] != nil {
	// 	g.cacheHit++
	// 	return g.next[c]
	// }

	var ng gdstate
	for _, s := range g.gstates {
		if s.c[c] || s.c['*'] {
			for _, ns := range s.next {
				ng.gstates = g.add(ng.gstates, ns)
			}
		}
	}
	// g.next[c] = &ng
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
func newGlobState(expr string, expand func(globs []string) ([]string, error)) (*gmatcher, error) {
	var m = gmatcher{root: &gstate{}, exact: true, expr: expr}
	var cur = m.root
	var alters [][2]*gstate

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
						if j != '*' {
							s.c[j] = true
						}
					}

					i += 2
					continue
				}

				s.c[expr[i]] = true
				i++
			}
			if i >= len(expr) || expr[i] != ']' {
				return nil, errors.New("glob: missing ]")
			}

			if negative {
				// TODO: revisit
				for i := 32; i <= 126; i++ {
					if i != '*' {
						s.c[i] = !s.c[i]
					}
				}
			}

			cur.next = append(cur.next, s)
			cur = s

			m.lsComplex = false
		case '?':
			m.exact = false
			var star gstate
			star.c['*'] = true
			cur.next = append(cur.next, &star)
			cur = &star

			m.lsComplex = false
		case '*':
			m.exact = false
			if i == 0 && len(expr) > 2 {
				// TODO: check dup stars
				m.lsComplex = true
			}

			// de-dup multi stars: *** -> *
			for ; i+1 < len(expr) && expr[i+1] == '*'; i++ {
			}

			var split, star gstate
			star.c['*'] = true
			split.c[gstateSplit] = true
			split.next = append(split.next, &star)
			cur.next = append(cur.next, &split)
			star.next = append(star.next, &split)

			cur = &split
		case '{':
			alterStart := &gstate{c: [131]bool{gstateSplit: true}}
			alterEnd := &gstate{c: [131]bool{gstateSplit: true}}
			cur.next = append(cur.next, alterStart)
			cur = alterStart
			alters = append(alters, [2]*gstate{alterStart, alterEnd})

			m.lsComplex = false
		case '}':
			if len(alters) == 0 {
				return nil, errors.New("glob: missing {")
			}
			cur.next = append(cur.next, alters[len(alters)-1][1])
			cur = alters[len(alters)-1][1]
			alters = alters[:len(alters)-1]

			m.lsComplex = false
		case ',':
			if len(alters) > 0 {
				cur.next = append(cur.next, alters[len(alters)-1][1])
				cur = alters[len(alters)-1][0]
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

	if len(alters) > 0 {
		return nil, errors.New("glob: missing }")
	}

	var droot gdstate
	for _, s := range m.root.next {
		droot.gstates = droot.add(droot.gstates, s)
	}
	m.dstates = append(m.dstates, &droot)

	if m.lsComplex {
		es, err := expand([]string{expr})
		if err != nil {
			return nil, nil
		}
		for _, e := range es {
			trigrams := extractTrigrams(e)

		appendt:
			for i := 0; i < len(trigrams); i++ {
				for _, t := range m.trigrams {
					if t == uint32(trigrams[i]) {
						continue appendt
					}
				}
				m.trigrams = append(m.trigrams, uint32(trigrams[i]))
			}
		}
	}

	return &m, nil
}

func isAlphanumeric(c byte) bool {
	return 32 <= c && c <= 126
}

type trieIndex struct {
	root          *trieNode
	fileExt       string
	fileCount     int
	depth         int
	longestMetric string
	trigrams      map[*trieNode][]uint32
}

type trieNode struct {
	c         []byte // TODO: look for a more compact/compressed formats
	childrens []*trieNode
}

var fileNode = &trieNode{}

func (tn *trieNode) dir() bool { return len(tn.c) == 1 && tn.c[0] == '/' }

func (ti *trieIndex) trigramsContains(tn *trieNode, t uint32) bool {
	for _, i := range ti.trigrams[tn] {
		if i == t {
			return true
		}
	}
	return false
}

func newTrie(fileExt string) *trieIndex {
	return &trieIndex{
		root:     &trieNode{},
		fileExt:  fileExt,
		trigrams: map[*trieNode][]uint32{},
	}
}

// TODO: add some defensive logics agains bad paths?
//
// abc.def.ghi
// abc.def2.ghi
// abc.daf2.ghi
// efg.cjk
func (ti *trieIndex) insert(path string) error {
	path = filepath.Clean(path)
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	if path == "" || path == "." {
		return nil
	}

	cur := ti.root
	if len(path) > ti.depth {
		ti.depth = len(path)
		ti.longestMetric = path
	}

	isFile := strings.HasSuffix(path, ti.fileExt)
	if isFile {
		path = path[:len(path)-len(ti.fileExt)]
	}

	var start, match, nlen int
	var sn, newn *trieNode
outer:
	for i := 0; i < len(path)+1; i++ {
		// getting a full node
		if i < len(path) && path[i] != '/' {
			continue
		}

		// case 1:
		// 	abc . xxx
		// 	ab  . xxx
		// case 2:
		// 	ab  . xxx
		// 	abc . xxx
		// case 3:
		// 	abc . xxx
		// 	abc . xxx
		// case 4:
		// 	abc . xxx
		// 	xyz . xxx
		// case 5:
		// 	abc . xxx
		// 	acc . xxx
		// case 6:
		//  abc . xxx
		//  abd . xxx
		// case 7:
		//  abc  . xxx
		//  abde . xxx
		// case 8:
		//  abc . xxx
		//  abc

	inner:
		for ci := 0; ci < len(cur.childrens); ci++ {
			child := cur.childrens[ci]

			if len(child.c) == 0 || child.c[0] != path[start] {
				continue
			}

			nlen = i - start
			start++
			for match = 1; match < len(child.c) && match < nlen; match++ {
				if child.c[match] != path[start] {
					break
				}
				start++
			}

			if match == nlen {
				// case 1
				if len(child.c) > match {
					cur = child
					goto split
				}

				// case 3
				if len(child.c) == match {
					cur = child
					goto dir
				}

				return fmt.Errorf("failed to index metric %s: unknwon case of match == nlen = %d", path, nlen)
			}

			if match == len(child.c) && len(child.c) < nlen { // case 2
				cur = child
				goto inner
			}

		split:
			// case 5, 6, 7
			prefix, suffix := child.c[:match], child.c[match:]
			sn = &trieNode{c: suffix, childrens: child.childrens}

			child.c = prefix
			child.childrens = []*trieNode{sn}

			cur = child

			if nlen-match > 0 {
				newn = &trieNode{c: make([]byte, nlen-match)}
				copy(newn.c, []byte(path[start:i]))

				cur.childrens = append(cur.childrens, newn)
				cur = newn
			}

			goto dir
		}

		// case 4 & 2
		if i-start > 0 {
			newn = &trieNode{c: make([]byte, i-start)}
			copy(newn.c, []byte(path[start:i]))
			cur.childrens = append(cur.childrens, newn)
			cur = newn
		}

	dir:
		// case 8
		if i == len(path) {
			break outer
		}

		start = i + 1
		for _, child := range cur.childrens {
			if child.dir() {
				cur = child
				continue outer
			}
		}

		if i < len(path) {
			newn = &trieNode{c: []byte{'/'}}
			cur.childrens = append(cur.childrens, newn)
			cur = newn
		}
	}

	if !isFile {
		if cur.dir() {
			cur.childrens = append(cur.childrens, &trieNode{c: []byte{'/'}})
		}
		return nil
	}

	cur.childrens = append(cur.childrens, fileNode)
	ti.fileCount++

	return nil
}

// TODO: add some defensive logics agains bad queries?
// depth first search
func (ti *trieIndex) query(expr string, limit int, expand func(globs []string) ([]string, error)) (files []string, isFiles []bool, err error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		expr = "*"
	}
	var matchers []*gmatcher
	var exact bool
	for _, node := range strings.Split(expr, "/") {
		if node == "" {
			continue
		}
		gs, err := newGlobState(node, expand)
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
	var isFile, isDir, hasMoreNodes bool

	for {
		if nindex[ncindex] >= len(cur.childrens) {
			curm.pop(len(cur.c))
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.childrens[nindex[ncindex]]
		ncindex++

		if cur.dir() {
			if mindex+1 >= len(matchers) || !curm.dstate().matched() {
				goto parent
			}

			mindex++
			curm = matchers[mindex]
			curm.dstates = curm.dstates[:1]

			continue
		}

		if curm.lsComplex && len(curm.trigrams) > 0 {
			if _, ok := ti.trigrams[cur]; ok {
				for _, t := range curm.trigrams {
					if !ti.trigramsContains(cur, t) {
						goto parent
					}
				}
			}
		}

		for i := 0; i < len(cur.c); i++ {
			ndstate = curm.dstate().step(cur.c[i])
			if len(ndstate.gstates) == 0 {
				if i > 0 {
					// TODO: add test case
					curm.pop(i)
				}
				goto parent
			}
			curm.push(ndstate)
		}

		if mindex+1 < len(matchers) {
			continue
		}

		isFile = false
		isDir = false
		hasMoreNodes = false
		for _, child := range cur.childrens {
			switch {
			case child == fileNode:
				isFile = true
			case child.dir():
				isDir = true
			default:
				hasMoreNodes = true
			}
		}

		if !(isFile || isDir) {
			continue
		}

		if !curm.dstate().matched() {
			if hasMoreNodes {
				continue
			}

			curm.pop(len(cur.c))
			goto parent
		}

		if isFile {
			files = append(files, cur.fullPath('.', trieNodes[:ncindex]))
			isFiles = append(isFiles, isFile)
		}
		if isDir {
			files = append(files, cur.fullPath('.', trieNodes[:ncindex]))
			isFiles = append(isFiles, false)
		}

		if len(files) >= limit || exact {
			break
		}

		if hasMoreNodes {
			continue
		}

		curm.pop(len(cur.c))
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

		if cur.dir() && nindex[ncindex] >= len(cur.childrens) {
			mindex--
			curm = matchers[mindex]

			goto parent
		}

		continue
	}

	return files, isFiles, nil
}

func (tn *trieNode) fullPath(sep byte, parents []*trieNode) string {
	var size = len(tn.c)
	for _, n := range parents {
		size += len(n.c)
	}
	var r = make([]byte, size)
	var i int
	for _, n := range parents[1:] {
		if n.dir() {
			r[i] = sep
			i++
		} else {
			copy(r[i:], n.c)
			i += len(n.c)
		}
	}
	copy(r[i:], tn.c)

	return *(*string)(unsafe.Pointer(&r))
}

func dumpTrigrams(data []uint32) []trigram.T { //nolint:deadcode
	var ts []trigram.T
	for _, t := range data {
		ts = append(ts, trigram.T(t))
	}
	return ts
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

		if cur == fileNode {
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

// statNodes returns the how many files/dirs a dir node has (doesn't go over dir
// boundary)
func (ti *trieIndex) statNodes() map[*trieNode]int {
	var stats = map[*trieNode]int{}
	var nindex = make([]int, ti.depth+1)
	var ncindex int
	var cur = ti.root
	var trieNodes = make([]*trieNode, ti.depth+1)
	var curdirs = make([]int, ti.depth+1)
	var curindex int

	for {
		if nindex[ncindex] >= len(cur.childrens) {
			if cur.dir() {
				stats[cur] = curdirs[curindex]
				curdirs[curindex] = 0
				curindex--
			}

			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.childrens[nindex[ncindex]]
		ncindex++

		if cur.dir() {
			curdirs[curindex]++
			curindex++
		}

		if cur == fileNode {
			curdirs[curindex]++
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

	return stats
}

func (ti *trieIndex) setTrigrams() {
	var nindex = make([]int, ti.depth+1)
	var ncindex int
	var cur = ti.root
	var trieNodes = make([]*trieNode, ti.depth+1)
	var trigrams = make([][]uint32, ti.depth+1)
	var stats = ti.statNodes()

	// chosen semi-randomly. maybe we could turn this into a configurations
	// if we have enough evidence to prove it's valuable.
	const factor = 10

	gent := func(b1, b2, b3 byte) uint32 { return uint32(uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3)) }

	for {
		if nindex[ncindex] >= len(cur.childrens) {
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.childrens[nindex[ncindex]]
		ncindex++
		trieNodes[ncindex] = cur

		// abc.xyz.cjk
		trigrams[ncindex] = []uint32{}
		if ncindex > 1 && len(cur.c) > 0 && !cur.dir() {
			cur1 := trieNodes[ncindex-1]
			if !cur1.dir() {
				if len(cur1.c) > 1 {
					t := gent(cur1.c[len(cur1.c)-2], cur1.c[len(cur1.c)-1], cur.c[0])
					trigrams[ncindex] = append(trigrams[ncindex], t)
				} else if ncindex-2 >= 0 {
					cur2 := trieNodes[ncindex-2]
					if !cur2.dir() && len(cur2.c) > 0 && len(cur1.c) > 0 {
						t := gent(cur2.c[len(cur2.c)-1], cur1.c[len(cur1.c)-1], cur.c[0])
						trigrams[ncindex] = append(trigrams[ncindex], t)
					}
				}

				if len(cur.c) > 1 {
					t := gent(cur1.c[len(cur1.c)-1], cur.c[0], cur.c[1])
					trigrams[ncindex] = append(trigrams[ncindex], t)
				}
			}
		}
		if !cur.dir() && len(cur.c) > 2 {
			for i := 0; i < len(cur.c)-2; i++ {
				t := gent(cur.c[i], cur.c[i+1], cur.c[i+2])
				trigrams[ncindex] = append(trigrams[ncindex], t)
			}
		}

		for i := ncindex - 1; i >= 0 && len(trigrams[ncindex]) > 0; i-- {
			if trieNodes[i] == nil || !trieNodes[i].dir() {
				continue
			}

			// TODO: index not just the first node, but also the 5th, 9th, etc.
			if stats[trieNodes[i]] > factor {
				// the current trigrams strategy only works for dir nodes contains many direct children
				// but should be able to change to handle deep-nested children
				for _, pos := range []int{1} {
					if i+pos >= len(trieNodes) || trieNodes[i+pos] == nil {
						break
					}
				appendt:
					for _, t := range trigrams[ncindex] {
						for _, tit := range ti.trigrams[trieNodes[i+pos]] {
							if t == tit {
								continue appendt
							}
						}
						ti.trigrams[trieNodes[i+pos]] = append(ti.trigrams[trieNodes[i+pos]], t)
					}

				}
			}
			break
		}

		if cur == fileNode {
			goto parent
		}

		continue

	parent:
		nindex[ncindex] = 0
		trieNodes[ncindex] = nil
		ncindex--
		if ncindex < 0 {
			break
		}
		nindex[ncindex]++
		cur = trieNodes[ncindex]
		continue
	}

}

func (listener *CarbonserverListener) expandGlobsTrie(query string) ([]string, []bool, error) {
	query = strings.Replace(query, ".", "/", -1)
	globs := []string{query}

	var slashInBraces, inAlter bool
	for _, c := range query {
		switch {
		case c == '{':
			inAlter = true
		case c == '}':
			inAlter = false
		case inAlter && c == '/':
			slashInBraces = true
		}
	}
	// for complex queries like {a.b.c,x}.o.p.q, fall back to simple expansion
	if slashInBraces {
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
		f, l, err := fidx.trieIdx.query(g, listener.maxMetricsGlobbed-len(files), listener.expandGlobBraces)
		if err != nil {
			return nil, nil, err
		}
		files = append(files, f...)
		leafs = append(leafs, l...)
	}

	return files, leafs, nil
}
