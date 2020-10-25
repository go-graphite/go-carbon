package carbonserver

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"unsafe"

	trigram "github.com/dgryski/go-trigram"
)

// debug codes diff for reference: https://play.golang.org/p/FxuvRyosk3U

// dfa inspiration: https://swtch.com/~rsc/regexp/

const (
	gstateSplit   = 256
	maxGstateCLen = 257
)

var endGstate = &gstate{}

type gmatcher struct {
	exact   bool
	root    *gstate
	expr    string
	dstates []*gdstate
	dsindex int //nolint:unused,structcheck

	// has leading star following by complex expressions
	lsComplex bool
	// trigrams  roaring.Bitmap
	trigrams []uint32
}

type gstate struct {
	// TODO: make c compact
	c    [maxGstateCLen]bool
	next []*gstate
}

type gdstate struct {
	gstates []*gstate
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
			alterStart := &gstate{c: [maxGstateCLen]bool{gstateSplit: true}}
			alterEnd := &gstate{c: [maxGstateCLen]bool{gstateSplit: true}}
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
	depth         uint64
	longestMetric string
	trigrams      map[*trieNode][]uint32
}

type trieNode struct {
	c         []byte // TODO: look for a more compact/compressed formats
	childrens *[]*trieNode
	gen       uint8
}

var emptyTrieNodes = &[]*trieNode{}

func newFileNode(m uint8) *trieNode { return &trieNode{childrens: emptyTrieNodes, gen: m} }

func (tn *trieNode) dir() bool  { return len(tn.c) == 1 && tn.c[0] == '/' }
func (tn *trieNode) file() bool { return tn.c == nil }

func (tn *trieNode) getChildrens() []*trieNode {
	return *(*[]*trieNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tn.childrens))))
}

func (tn *trieNode) getChild(curChildrens []*trieNode, i int) *trieNode {
	return (*trieNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&curChildrens[i]))))
}

func (tn *trieNode) setChildrens(cs []*trieNode) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&tn.childrens)), unsafe.Pointer(&cs))
}

func (tn *trieNode) addChild(n *trieNode) {
	tn.setChildrens(append(*tn.childrens, n))
}

func (tn *trieNode) setChild(i int, n *trieNode) {
	atomic.StorePointer(
		(*unsafe.Pointer)(unsafe.Pointer(&(*tn.childrens)[i])),
		unsafe.Pointer(n),
	)
}

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
		root:     &trieNode{childrens: &[]*trieNode{}},
		fileExt:  fileExt,
		trigrams: map[*trieNode][]uint32{},
	}
}

func (ti *trieIndex) getDepth() uint64  { return atomic.LoadUint64(&ti.depth) }
func (ti *trieIndex) setDepth(d uint64) { atomic.StoreUint64(&ti.depth, d) }

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
	if uint64(len(path)) > ti.getDepth() {
		ti.setDepth(uint64(len(path)))
		ti.longestMetric = path
	}

	isFile := strings.HasSuffix(path, ti.fileExt)
	if isFile {
		path = path[:len(path)-len(ti.fileExt)]
	}

	var start, nlen int
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
		for ci := 0; ci < len(*cur.childrens); ci++ {
			child := (*cur.childrens)[ci]
			match := 0

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
					goto split
				}

				// case 3
				if len(child.c) == match {
					child.gen = ti.root.gen
					cur = child
					goto dir
				}

				return fmt.Errorf("failed to index metric %s: unknwon case of match == nlen = %d", path, nlen)
			}

			if match == len(child.c) && len(child.c) < nlen { // case 2
				child.gen = ti.root.gen
				cur = child
				goto inner
			}

		split:
			// case 5, 6, 7
			prefix, suffix := child.c[:match], child.c[match:]
			sn = &trieNode{c: suffix, childrens: child.childrens, gen: child.gen}

			cur.setChild(ci, &trieNode{c: prefix, childrens: &[]*trieNode{sn}, gen: ti.root.gen})
			cur = (*cur.childrens)[ci]

			if nlen-match > 0 {
				newn = &trieNode{c: make([]byte, nlen-match), childrens: &[]*trieNode{}, gen: ti.root.gen}
				copy(newn.c, path[start:i])

				cur.addChild(newn)
				cur = newn
			}

			goto dir
		}

		// case 4 & 2
		if i-start > 0 {
			newn = &trieNode{c: make([]byte, i-start), childrens: &[]*trieNode{}, gen: ti.root.gen}
			copy(newn.c, path[start:i])
			cur.addChild(newn)
			cur = newn
		}

	dir:
		// case 8
		if i == len(path) {
			break outer
		}

		start = i + 1
		for _, child := range *cur.childrens {
			if child.dir() {
				cur = child
				cur.gen = ti.root.gen
				continue outer
			}
		}

		if i < len(path) {
			newn = &trieNode{c: []byte{'/'}, childrens: &[]*trieNode{}, gen: ti.root.gen}
			cur.addChild(newn)
			cur = newn
		}
	}

	// TODO: there seems to be a problem of fetching a directory node without files

	if !isFile {
		// TODO: should double check if / already exists
		if cur.dir() {
			cur.addChild(&trieNode{c: []byte{'/'}, childrens: &[]*trieNode{}, gen: ti.root.gen})
		}
		return nil
	}

	var hasFileNode bool
	for _, c := range *cur.childrens {
		// if c == fileNode {
		if c.file() {
			hasFileNode = true
			c.gen = ti.root.gen
			break
		}
	}
	if !hasFileNode {
		cur.addChild(newFileNode(ti.root.gen))
		ti.fileCount++
	}

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
	var curChildrens = cur.getChildrens()
	var depth = ti.getDepth() + 1
	var nindex = make([]int, depth)
	var trieNodes = make([]*trieNode, depth)
	var childrensStack = make([][]*trieNode, depth)
	var ncindex int
	var mindex int
	var curm = matchers[0]
	var ndstate *gdstate
	var isFile, isDir, hasMoreNodes bool

	for {
		if nindex[ncindex] >= len(curChildrens) {
			curm.pop(len(cur.c))
			goto parent
		}

		trieNodes[ncindex] = cur
		childrensStack[ncindex] = curChildrens
		cur = cur.getChild(curChildrens, nindex[ncindex])
		curChildrens = cur.getChildrens()
		ncindex++

		// It's possible to run into a situation of newer and longer metrics
		// are added during the query, for such situation, it's safer to just
		// skip them for the current query.
		if ncindex >= len(nindex)-1 {
			goto parent
		}

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
		for i := 0; i < len(curChildrens); i++ {
			child := cur.getChild(curChildrens, i)
			switch {
			case child.file():
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
		curChildrens = childrensStack[ncindex]

		if cur.dir() && nindex[ncindex] >= len(curChildrens) {
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

func dumpTrigrams(data []uint32) []trigram.T { //nolint:deadcode,unused
	var ts []trigram.T
	for _, t := range data {
		ts = append(ts, trigram.T(t))
	}
	return ts
}

func (ti *trieIndex) allMetrics(sep byte) []string {
	var files = make([]string, 0, ti.fileCount)
	var depth = ti.getDepth() + 1
	var nindex = make([]int, depth)
	var ncindex int
	var cur = ti.root
	var curChildrens = cur.getChildrens()
	var trieNodes = make([]*trieNode, depth)
	for {
		if nindex[ncindex] >= len(curChildrens) {
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.getChild(curChildrens, nindex[ncindex])
		curChildrens = cur.getChildrens()
		ncindex++
		if ncindex >= len(trieNodes)-1 {
			goto parent
		}

		if cur.file() {
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
		curChildrens = cur.getChildrens()
		continue
	}

	sort.Strings(files)

	return files
}

func (ti *trieIndex) dump(w io.Writer) {
	var depth = ti.getDepth() + 1
	var nindex = make([]int, depth)
	var ncindex int
	var cur = ti.root
	var curChildrens = cur.getChildrens()
	var trieNodes = make([]*trieNode, depth)
	var ident []byte
	for {
		if nindex[ncindex] >= len(curChildrens) {
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.getChild(curChildrens, nindex[ncindex])
		curChildrens = cur.getChildrens()
		ncindex++
		if ncindex >= len(trieNodes)-1 {
			goto parent
		}

		if cur.file() {
			fmt.Fprintf(w, "%s$ (%d/%d)\n", ident, len(*cur.childrens), cur.gen)
		} else {
			fmt.Fprintf(w, "%s%s (%d/%d)\n", ident, cur.c, len(*cur.childrens), cur.gen)
		}
		ident = append(ident, ' ', ' ')

		if cur.file() {
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
		curChildrens = cur.getChildrens()

		ident = ident[:len(ident)-2]

		continue
	}
}

// statNodes returns the how many files/dirs a dir node has (doesn't go over dir
// boundary)
func (ti *trieIndex) statNodes() map[*trieNode]int {
	var stats = map[*trieNode]int{}
	var depth = ti.getDepth() + 1
	var nindex = make([]int, depth)
	var ncindex int
	var cur = ti.root
	var curChildrens = cur.getChildrens()
	var trieNodes = make([]*trieNode, depth)
	var curdirs = make([]int, depth)
	var curindex int

	for {
		if nindex[ncindex] >= len(curChildrens) {
			if cur.dir() {
				stats[cur] = curdirs[curindex]
				curdirs[curindex] = 0
				curindex--
			}

			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.getChild(curChildrens, nindex[ncindex])
		ncindex++
		if ncindex >= len(nindex)-1 {
			goto parent
		}

		if cur.dir() {
			curdirs[curindex]++
			curindex++
		}

		if cur.file() {
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
		curChildrens = cur.getChildrens()
		continue
	}

	return stats
}

// TODO: support ctrie
func (ti *trieIndex) setTrigrams() {
	var depth = ti.getDepth() + 1
	var nindex = make([]int, depth)
	var ncindex int
	var cur = ti.root
	var trieNodes = make([]*trieNode, depth)
	var trigrams = make([][]uint32, depth)
	var stats = ti.statNodes()

	// chosen semi-randomly. maybe we could turn this into a configurations
	// if we have enough evidence to prove it's valuable.
	const factor = 10

	gent := func(b1, b2, b3 byte) uint32 { return uint32(uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3)) }

	for {
		if nindex[ncindex] >= len(*cur.childrens) {
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = (*cur.childrens)[nindex[ncindex]]
		ncindex++
		if ncindex >= len(nindex)-1 {
			goto parent
		}

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

		if cur.file() {
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

// prune prunes trie nodes gened with a different values against root.
// prune has to be evoked after all inserts are done.
func (ti *trieIndex) prune() {
	type state struct {
		next      int
		node      *trieNode
		childrens []*trieNode
	}

	var cur state
	cur.node = ti.root
	cur.childrens = *cur.node.childrens

	var idx int
	var depth = ti.getDepth() + 1
	var states = make([]state, depth)
	for {
		if cur.next >= len(cur.childrens) {
			cc := cur.node.getChildrens()
			if idx > 0 && !cur.node.dir() && len(cc) == 1 && !cc[0].file() && !cc[0].dir() {
				n := &trieNode{childrens: cc[0].childrens, gen: ti.root.gen}
				n.c = make([]byte, 0, len(cur.node.c)+len(cc[0].c))
				n.c = append(n.c, cur.node.c...)
				n.c = append(n.c, cc[0].c...)
				for i, t := range *states[idx-1].node.childrens {
					if t == cur.node {
						states[idx-1].node.setChild(i, n)
					}
				}
				cur.node = n
			}

			goto parent
		}

		states[idx] = cur
		idx++
		if idx >= len(states)-1 {
			goto parent
		}

		cur.next = 0
		cur.node = (cur.childrens)[states[idx-1].next]
		cur.childrens = *cur.node.childrens

		// trimming deleted/old-gen metrics
		if cur.node.gen != ti.root.gen {
			pchildrens := states[idx-1].node.getChildrens()
			nc := make([]*trieNode, len(pchildrens)-1)
			for i, j := 0, 0; i < len(pchildrens); i++ {
				if (pchildrens)[i] == cur.node {
					continue
				}

				nc[j] = (pchildrens)[i]
				j++
			}
			states[idx-1].node.setChildrens(nc)

			goto parent
		}

		continue

	parent:
		states[idx] = state{}
		idx--
		if idx < 0 {
			break
		}

		states[idx].next++
		cur = states[idx]

		continue
	}
}

type trieCounter [256]int

func (tc *trieCounter) String() string {
	var str string
	for i, c := range tc {
		if c > 0 {
			if str != "" {
				str += " "
			}
			str += fmt.Sprintf("%d:%d", i, c)
		}
	}
	return fmt.Sprintf("{%s}", str)
}

func (ti *trieIndex) countNodes() (count, files, dirs, onec, onefc, onedc int, countByChildren, nodesByGen *trieCounter) {
	type state struct {
		next      int
		node      *trieNode
		childrens *[]*trieNode
	}

	var cur state
	cur.node = ti.root
	cur.childrens = cur.node.childrens

	var idx int
	var depth = ti.getDepth() + 1
	var states = make([]state, depth)
	countByChildren = &trieCounter{}
	nodesByGen = &trieCounter{}
	for {
		if cur.next >= len(*cur.childrens) {
			if len(*cur.childrens) == 1 && !cur.node.dir() {
				onec++
				if (*cur.childrens)[0].file() {
					onefc++
				} else if !(*cur.childrens)[0].dir() {
					onedc++
				}
			}

			goto parent
		}

		states[idx] = cur
		idx++
		if idx >= len(states)-1 {
			goto parent
		}

		cur.next = 0
		cur.node = (*cur.childrens)[states[idx-1].next]
		cur.childrens = cur.node.childrens

		count++
		countByChildren[len(*cur.childrens)%256] += 1
		nodesByGen[cur.node.gen] += 1

		if cur.node.file() {
			files++
			goto parent
		} else if cur.node.dir() {
			dirs++
		}

		continue

	parent:
		states[idx] = state{}
		idx--
		if idx < 0 {
			break
		}

		states[idx].next++
		cur = states[idx]

		continue
	}

	return
}

func (listener *CarbonserverListener) expandGlobsTrie(query string) ([]string, []bool, error) {
	query = strings.ReplaceAll(query, ".", "/")
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
