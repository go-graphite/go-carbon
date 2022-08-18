package carbonserver

import (
	"crypto/md5" // skipcq: GSC-G501
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	trigram "github.com/dgryski/go-trigram"
	"github.com/go-graphite/go-carbon/points"
)

// debug codes diff for reference: https://play.golang.org/p/FxuvRyosk3U

// dfa inspiration: https://swtch.com/~rsc/regexp/

const (
	gstateSplit   = 256
	maxGstateCLen = 257

	// used in walking over the tree, a lazy way to make sure that all the nodes are
	// covered without risking index out of range.
	trieDepthBuffer = 7
)

var endGstate = &gstate{}

type gmatcher struct {
	exact   bool
	root    *gstate
	expr    string
	dstates []*gdstate
	dsindex int //nolint:unused,structcheck

	// TODO: depcreate lsComplex and trigrams. the gains of it isn't very
	// clear and it would make things simpler.
	//
	// has leading star following by complex expressions
	lsComplex bool
	trigrams  []uint32
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
			if i >= len(expr) {
				return nil, errors.New("glob: broken range syntax")
			}
			negative := expr[i] == '^'
			if negative {
				i++
			}
			for i < len(expr) && expr[i] != ']' {
				if expr[i] == '-' {
					if i+1 >= len(expr) {
						return nil, errors.New("glob: missing closing range")
					}
					if expr[i-1] > expr[i+1] {
						return nil, errors.New("glob: range start is bigger than range end")
					}
					// a simple check to make sure that range doesn't ends with 0xff,
					// which would causes endless loop bellow
					if expr[i-1] > 128 || expr[i+1] > 128 {
						return nil, errors.New("glob: range overflow")
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

	// TODO: consider dropping trigram integration
	if m.lsComplex && expand != nil {
		es, err := expand([]string{expr})
		if err != nil {
			return &m, nil
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

	// qau: Quota And Usage
	qauMetrics   []points.Points
	estimateSize func(metric string) (size, dataPoints int64)

	throughputs    *throughputQuotaManager
	resetFrequency time.Duration
}

func (ti *trieIndex) setResetFrequency(f time.Duration) {
	atomic.StoreInt64((*int64)(&ti.resetFrequency), int64(f))
}

func (ti *trieIndex) getResetFrequency() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(&ti.resetFrequency)))
}

// note: root and file leaf node has a nil c, mainly used in trieNode.fullPath
type trieNode struct {
	c         []byte // TODO: look for a more compact/compressed formats
	childrens *[]*trieNode
	gen       uint8

	meta trieMeta
}

type trieMeta interface{ trieMeta() }

type fileMeta struct {
	logicalSize  int64
	physicalSize int64
	dataPoints   int64
	readHits     int64
	readBytes    int64
	// a timestamp of the first time when go-carbon has seen this metric.
	// strictly speaking, it is not metric creation time, but can be used
	// as a timing reference to infer that the metric is at least created
	// before this timestamp. some file systems doesn't maintain ctime of
	// files (like xfs before v5:
	// https://unix.stackexchange.com/questions/7562/what-file-systems-on-linux-store-the-creation-time#comment1184213_40093)
	//
	// the reliablity of this timestamp counts on file list cache v2
	// (and above), more in carbonserver.go and flc.go.
	firstSeenAt int64
}

func (*fileMeta) trieMeta() {}

type dirMeta struct {
	// type: *Quota
	// note: the underlying Quota value is shared with other dir nodes.
	//
	// TODO: save 8 bytes by using a pointer value?
	quota atomic.Value
	usage *QuotaUsage
}

func newDirMeta() *dirMeta { return &dirMeta{usage: &QuotaUsage{}} }

func (*dirMeta) trieMeta()              {}
func (dm *dirMeta) update(quota *Quota) { dm.quota.Store(quota) }

func (dm *dirMeta) withinQuota(metrics, namespaces, logical, physical, dataPoints int64) bool {
	quota, ok := dm.quota.Load().(*Quota)
	if !ok {
		return true
	}

	var (
		qmetrics      = quota.Metrics
		qnamespaces   = quota.Namespaces
		qlogicalSize  = quota.LogicalSize
		qphysicalSize = quota.PhysicalSize
		qdataPoints   = quota.DataPoints

		umetrics      = atomic.LoadInt64(&dm.usage.Metrics)
		unamespaces   = atomic.LoadInt64(&dm.usage.Namespaces)
		ulogicalsize  = atomic.LoadInt64(&dm.usage.LogicalSize)
		uphysicalsize = atomic.LoadInt64(&dm.usage.PhysicalSize)
		udataPoints   = atomic.LoadInt64(&dm.usage.DataPoints)
	)

	if qmetrics > 0 && umetrics+metrics > qmetrics {
		return false
	}
	if qnamespaces > 0 && unamespaces+namespaces > qnamespaces {
		return false
	}
	if qlogicalSize > 0 && ulogicalsize+logical > qlogicalSize {
		return false
	}
	if qphysicalSize > 0 && uphysicalsize+physical > qphysicalSize {
		return false
	}
	if qdataPoints > 0 && udataPoints+dataPoints > qdataPoints {
		return false
	}

	return true
}

var emptyTrieNodes = &[]*trieNode{}

// TODO: consider not initialize fileMeta if quota feature isn't enabled?
func newFileNode(m uint8, logicalSize, physicalSize, dataPoints, firstSeenAt int64) *trieNode {
	return &trieNode{
		childrens: emptyTrieNodes,
		gen:       m,
		meta: &fileMeta{
			logicalSize:  logicalSize,
			physicalSize: physicalSize,
			dataPoints:   dataPoints,
			firstSeenAt:  firstSeenAt,
		},
	}
}

func (tn *trieNode) dir() bool  { return len(tn.c) == 1 && tn.c[0] == '/' }
func (tn *trieNode) file() bool { return tn.c == nil }

func (tn *trieNode) getChildrens() []*trieNode {
	// skipcq: GSC-G103
	return *(*[]*trieNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tn.childrens))))
}

func (*trieNode) getChild(curChildrens []*trieNode, i int) *trieNode {
	// skipcq: GSC-G103
	return (*trieNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&curChildrens[i]))))
}

func (tn *trieNode) setChildrens(cs []*trieNode) {
	// skipcq: GSC-G103
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&tn.childrens)), unsafe.Pointer(&cs))
}

func (tn *trieNode) addChild(n *trieNode) {
	tn.setChildrens(append(*tn.childrens, n))
}

func (tn *trieNode) setChild(i int, n *trieNode) {
	atomic.StorePointer(
		(*unsafe.Pointer)(unsafe.Pointer(&(*tn.childrens)[i])), // skipcq: GSC-G103
		unsafe.Pointer(n), // skipcq: GSC-G103
	)
}

func (tn *trieNode) incrementReadHitsMetric() {
	if tn.file() {
		meta := tn.meta.(*fileMeta)
		atomic.AddInt64(&meta.readHits, 1)
	}
}
func (tn *trieNode) incrementReadBytesMetric(bytesNumber int64) {
	if tn.file() {
		meta := tn.meta.(*fileMeta)
		atomic.AddInt64(&meta.readBytes, bytesNumber)
	}
}

func (ti *trieIndex) trigramsContains(tn *trieNode, t uint32) bool {
	for _, i := range ti.trigrams[tn] {
		if i == t {
			return true
		}
	}
	return false
}

func newTrie(fileExt string, estimateSize func(metric string) (size, dataPoints int64)) *trieIndex {
	meta := newDirMeta()
	return &trieIndex{
		root:         &trieNode{childrens: &[]*trieNode{}, meta: meta},
		fileExt:      fileExt,
		trigrams:     map[*trieNode][]uint32{},
		estimateSize: estimateSize,
		throughputs:  newQuotaThroughputQuotaManager(),
	}
}

func (ti *trieIndex) getDepth() uint64  { return atomic.LoadUint64(&ti.depth) }
func (ti *trieIndex) setDepth(d uint64) { atomic.StoreUint64(&ti.depth, d) }

type nilFilenameError string

func (nfe nilFilenameError) Error() string { return string(nfe) }

type trieInsertError struct {
	typ  string
	info string
}

func (t *trieInsertError) Error() string { return t.typ }

// TODO: add some more defensive logics agains bad paths?
//
// abc.def.ghi
// abc.def2.ghi
// abc.daf2.ghi
// efg.cjk
//
// insert expects / separated file path, like ns1/ns2/ns3/metric.wsp.
// insert considers path name ending with trieIndex.fileExt as a metric.
//
// insert returns either a file node or dir node, after inserted.
func (ti *trieIndex) insert(path string, logicalSize, physicalSize, dataPoints, firstSeenAt int64) (*trieNode, error) {
	path = filepath.Clean(path)
	if len(path) > 0 && path[0] == '/' { // skipcq: GO-S1005
		path = path[1:]
	}
	if path == "" || path == "." {
		return nil, nil
	}

	isFile := strings.HasSuffix(path, ti.fileExt)
	if isFile {
		path = path[:len(path)-len(ti.fileExt)]
	}

	if path == "" || path[len(path)-1] == '/' {
		return nil, nilFilenameError("metric filename is nil")
	}

	if uint64(len(path)) > ti.getDepth() {
		ti.setDepth(uint64(len(path)))
		ti.longestMetric = path
	}

	var start, nlen int
	var sn, newn *trieNode
	var cur = ti.root
outer:
	// why len(path)+1: make sure the last node is also processed in the loop
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

				return nil, &trieInsertError{typ: "failed to index metric: unknwon case of match == nlen", info: fmt.Sprintf("match == nlen == %d", nlen)}
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
			newn = ti.newDir()
			cur.addChild(newn)
			cur = newn
		}
	}

	// TODO: there seems to be a problem of fetching a directory node without files

	if !isFile {
		if cur.dir() {
			cur.gen = ti.root.gen
			return cur, nil
		}

		var newDir = true
		for _, child := range *cur.childrens {
			if child.dir() {
				cur = child
				cur.gen = ti.root.gen
				newDir = false
				break
			}
		}
		if newDir {
			child := ti.newDir()
			cur.addChild(child)
			cur = child
		}

		return cur, nil
	}

	if firstSeenAt == 0 {
		firstSeenAt = time.Now().Unix()
	}

	var hasFileNode bool
	for _, c := range *cur.childrens {
		if c.file() {
			hasFileNode = true
			c.gen = ti.root.gen

			meta := c.meta.(*fileMeta)
			if logicalSize > 0 || physicalSize > 0 || dataPoints > 0 {
				atomic.StoreInt64(&meta.logicalSize, logicalSize)
				atomic.StoreInt64(&meta.physicalSize, physicalSize)
				atomic.StoreInt64(&meta.dataPoints, dataPoints)
			}
			if meta.firstSeenAt == 0 || meta.firstSeenAt > firstSeenAt {
				atomic.StoreInt64(&meta.firstSeenAt, firstSeenAt)
			}

			cur = c
			break
		}
	}
	if !hasFileNode {
		if ti.estimateSize != nil && logicalSize == 0 && physicalSize == 0 && dataPoints == 0 {
			logicalSize, dataPoints = ti.estimateSize(strings.ReplaceAll(path, "/", "."))
			physicalSize = logicalSize
		}

		child := newFileNode(ti.root.gen, logicalSize, physicalSize, dataPoints, firstSeenAt)
		cur.addChild(child)
		cur = child

		ti.fileCount++
	}

	return cur, nil
}

func (ti *trieIndex) newDir() *trieNode {
	n := &trieNode{
		c:         []byte{'/'},
		childrens: &[]*trieNode{},
		gen:       ti.root.gen,
	}

	return n
}

// TODO: add some defensive logics agains bad queries?
// depth first search
// TODO: refactor to make the function more readable. Some ideas:
// - to isolate Depth first search in separate class
// - probably we can optimize the length of existed arrays. It uses tree depth + 7, where tree depth is the longest path in the tree in characters
//   but we should be able to calculate max required depth based on expr, we don't have to look into deeper levels than expr's depth unless there is some unknown corner case exist
// - get rid of 'goto', since it adds complexions
//
func (ti *trieIndex) query(expr string, limit int, expand func(globs []string) ([]string, error)) (files []string, isFiles []bool, nodes []*trieNode, its uint64, err error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		expr = "*"
	}
	var matchers []*gmatcher // information about matching texts, has bytes array with used characters, one matcher per one depth level

	// TODO:
	// Supposely it indicates that we look for just one exact path, and it stops search after finding one path
	// but it is always false in current code. 'exact' should be true by default
	// to test if it works properly
	var exact bool
	// complexity of query
	var lookups uint64
	for _, node := range strings.Split(expr, "/") {
		if node == "" {
			continue
		}
		gs, err := newGlobState(node, expand)
		if err != nil {
			return nil, nil, nil, lookups, err
		}
		exact = exact && gs.exact
		matchers = append(matchers, gs)
	}

	if len(matchers) == 0 {
		return nil, nil, nil, lookups, nil
	}

	// current node
	var cur = ti.root
	// children of current node
	var curChildrens = cur.getChildrens()
	// longest path in trie (length in characters) + 7 (some magic number for extra capacity)
	var depth = ti.getDepth() + trieDepthBuffer
	// array of child indexes we looked at last time
	var nindex = make([]int, depth)
	// array of nodes, contains the path to the current processed node
	var trieNodes = make([]*trieNode, depth)
	// each item contains an array of all children for every trieNode in current looking path
	var childrensStack = make([][]*trieNode, depth)
	//current searching depth
	var ncindex int
	// index of needed matcher
	var mindex int
	//// matcher for the current level depth
	var curm = matchers[0]
	var ndstate *gdstate
	var isFile, isDir, hasMoreNodes bool
	var dirNode, fileNode *trieNode

	for {
		if nindex[ncindex] >= len(curChildrens) {
			curm.pop(len(cur.c))
			goto parent
		}

		// starting processing children of current node and saving current node into path
		// the root is supposed to be '/' node, not containing useful information, we skip checking it
		trieNodes[ncindex] = cur
		childrensStack[ncindex] = curChildrens
		cur = cur.getChild(curChildrens, nindex[ncindex])
		curChildrens = cur.getChildrens()
		ncindex++
		lookups++

		// It's possible to run into a situation of newer and longer metrics
		// are added during the query, for such situation, it's safer to just
		// skip them for the current query.
		if ncindex >= len(nindex)-1 {
			goto parent
		}

		// if node is '/'
		if cur.dir() {
			// amount of matchers should correlate the max searching depth,
			// first condition: checking the depth is too deep it doesn't make sense, going to parent
			if mindex+1 >= len(matchers) || !curm.dstate().matched() || len(curChildrens) == 0 {
				goto parent
			}

			mindex++
			curm = matchers[mindex]
			curm.dstates = curm.dstates[:1]

			continue
		}

		// matching regexp
		if curm.lsComplex && len(curm.trigrams) > 0 {
			if _, ok := ti.trigrams[cur]; ok {
				for _, t := range curm.trigrams {
					if !ti.trigramsContains(cur, t) {
						goto parent
					}
				}
			}
		}

		// matching regexp
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
				fileNode = child
			case child.dir():
				isDir = true
				dirNode = child
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

		// we found result metric here
		if isFile {
			files = append(files, cur.fullPath('.', trieNodes[:ncindex]))
			isFiles = append(isFiles, isFile)
			nodes = append(nodes, fileNode)
		}
		if isDir {
			files = append(files, cur.fullPath('.', trieNodes[:ncindex]))
			isFiles = append(isFiles, false)
			nodes = append(nodes, dirNode)
		}

		if len(files) >= limit || exact {
			break
		}

		if hasMoreNodes {
			continue
		}

		curm.pop(len(cur.c))
		goto parent

		// calling this procedure means that we are done with current child and moving level up
		// we are supposed to check next child of current parent next
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

	return files, isFiles, nodes, lookups, nil
}

// note: tn might be a root or file node, which has a nil c
func (tn *trieNode) fullPath(sep byte, parents []*trieNode) string {
	var size = len(tn.c)
	for _, n := range parents {
		size += len(n.c)
	}

	var r = make([]byte, size)
	var i int
	for _, n := range parents {
		if n.dir() {
			r[i] = sep
			i++
		} else if len(n.c) > 0 {
			copy(r[i:], n.c)
			i += len(n.c)
		}
	}
	if len(tn.c) > 0 {
		copy(r[i:], tn.c)
	}

	// skipcq: GSC-G103
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
	var depth = ti.getDepth() + trieDepthBuffer
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

	// TODO: remove, might be too expensive with large index
	sort.Strings(files)

	return files
}

// limit only applies when statsOnly is set to false.
// count means the number of files/metrics under the trieNode.
// skipcq: RVV-A0005
func (ti *trieIndex) allMetricsNode(tn *trieNode, sep byte, prefix string, limit int, statsOnly bool) (files []string, fileNodes []*trieNode, count int, physicalSize, logicalSize int64) {
	var depth = ti.getDepth() + trieDepthBuffer
	var nindex = make([]int, depth)
	var ncindex int
	var cur = tn
	var curChildrens = cur.getChildrens()
	var stack = make([]*trieNode, depth)

	for {
		if nindex[ncindex] >= len(curChildrens) {
			goto parent
		}

		stack[ncindex] = cur
		cur = cur.getChild(curChildrens, nindex[ncindex])
		curChildrens = cur.getChildrens()
		ncindex++
		if ncindex >= len(stack)-1 {
			goto parent
		}

		if cur.file() {
			count++
			if meta, ok := cur.meta.(*fileMeta); ok && meta != nil {
				physicalSize += meta.physicalSize
				logicalSize += meta.logicalSize
			}

			if !statsOnly && ncindex > 0 {
				files = append(files, prefix+cur.fullPath(sep, stack[:ncindex]))
				fileNodes = append(fileNodes, cur)
			}

			if len(files) >= limit {
				break
			}

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
		cur = stack[ncindex]
		curChildrens = cur.getChildrens()

		continue
	}

	// sort.Strings(files)

	return
}

// skipcq: SCC-U1000
func (ti *trieIndex) dump(w io.Writer) {
	var depth = ti.getDepth() + trieDepthBuffer
	var nindex = make([]int, depth)
	var ncindex int
	var cur = ti.root
	var curChildrens = cur.getChildrens()
	var trieNodes = make([]*trieNode, depth)
	var ident []byte

	fmt.Fprintf(w, "%s%s (%d/%d) (quota:%s usage:%s) %p\n", ident, "/", len(*cur.childrens), cur.gen, cur.meta.(*dirMeta).quota.Load(), cur.meta.(*dirMeta).usage, cur)
	ident = append(ident, ' ', ' ')

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

		switch {
		case cur.file():
			fmt.Fprintf(w, "%s$ (%d/%d) %p\n", ident, len(*cur.childrens), cur.gen, cur)
		case cur.dir() && cur.meta != nil:
			fmt.Fprintf(w, "%s%s (%d/%d) (quota:%s usage:%s) %p\n", ident, cur.c, len(*cur.childrens), cur.gen, cur.meta.(*dirMeta).quota.Load(), cur.meta.(*dirMeta).usage, cur)
		default:
			fmt.Fprintf(w, "%s%s (%d/%d) %p\n", ident, cur.c, len(*cur.childrens), cur.gen, cur)
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

// skipcq: RVV-A0006
func (ti *trieIndex) getQuotaTree(w io.Writer) {
	var depth = ti.getDepth() + trieDepthBuffer
	var nindex = make([]int, depth)
	var ncindex int
	var cur = ti.root
	var curChildrens = cur.getChildrens()
	var trieNodes = make([]*trieNode, depth)
	var ident []byte

	fmt.Fprintf(w, "%s%s (%d/%d) (quota:%s usage:%s) %p\n", ident, "/", len(*cur.childrens), cur.gen, cur.meta.(*dirMeta).quota.Load(), cur.meta.(*dirMeta).usage, cur)
	ident = append(ident, ' ', ' ')

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

		if cur.dir() && cur.meta != nil {
			name := ti.root.fullPath('.', trieNodes[:ncindex])

			fmt.Fprintf(w, "%s%s %s (%d/%d) (quota:%s usage:%s) %p\n", ident, cur.c, name, len(*cur.childrens), cur.gen, cur.meta.(*dirMeta).quota.Load(), cur.meta.(*dirMeta).usage, cur)
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
	var depth = ti.getDepth() + trieDepthBuffer
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
		curChildrens = cur.getChildrens()
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
	var depth = ti.getDepth() + trieDepthBuffer
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
	var depth = ti.getDepth() + trieDepthBuffer
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
	var depth = ti.getDepth() + trieDepthBuffer
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

func (listener *CarbonserverListener) expandGlobsTrie(query string) ([]string, []bool, []*trieNode, uint64, error) {
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
			return nil, nil, nil, 0, err
		}
	}

	var fidx = listener.CurrentFileIndex()
	var files []string
	var leafs []bool
	var nodes []*trieNode
	var lookups uint64

	for _, g := range globs {
		f, l, n, lk, err := fidx.trieIdx.query(g, listener.maxMetricsGlobbed-len(files), listener.expandGlobBraces)
		if err != nil {
			return nil, nil, nil, lookups, err
		}
		files = append(files, f...)
		leafs = append(leafs, l...)
		nodes = append(nodes, n...)
		lookups += lk
	}
	// set node as viewed
	for _, node := range nodes {
		node.incrementReadHitsMetric()
	}
	return files, leafs, nodes, lookups, nil
}

type QuotaDroppingPolicy int8

const (
	QDPNew QuotaDroppingPolicy = iota
	QDPNone
)

func ParseQuotaDroppingPolicy(policy string) QuotaDroppingPolicy {
	switch policy {
	case "none":
		return QDPNone
	case "new":
		return QDPNew
	default:
		return QDPNew
	}
}

func (qdp QuotaDroppingPolicy) String() string {
	switch qdp {
	case QDPNone:
		return "none"
	case QDPNew:
		return "new"
	default:
		return "new"
	}
}

type Quota struct {
	Pattern      string
	Namespaces   int64 // top level subdirectories
	Metrics      int64 // files
	LogicalSize  int64
	PhysicalSize int64

	DataPoints int64
	Throughput int64

	DroppingPolicy   QuotaDroppingPolicy
	StatMetricPrefix string
}

func (q *Quota) String() string {
	return fmt.Sprintf("pattern:%s,dirs:%d,files:%d,points:%d,logical:%d,physical:%d,throughput:%d,policy:%s", q.Pattern, q.Namespaces, q.Metrics, q.DataPoints, q.LogicalSize, q.PhysicalSize, q.Throughput, q.DroppingPolicy)
}

type throughputUsagePerNamespace struct {
	// TODO: use generics when it's in stdlib: https://github.com/golang/go/issues/50860
	quotaInfosv atomic.Value // *throughputUsagePerNamespaceQuotaInfo
	dpRecorderv atomic.Value // *throughputUsageDataPointsRecoder
}

type throughputUsagePerNamespaceQuotaInfo struct {
	gen   uint8
	quota *Quota
	usage *QuotaUsage
}

// check comment in *throughputUsageDataPointsRecoder.offset.
type throughputUsageDataPointsRecoder struct {
	dataPoints int64
	resetAt    time.Time
}

func newThroughputUsageDataPointsRecorder() *throughputUsageDataPointsRecoder {
	return &throughputUsageDataPointsRecoder{resetAt: time.Now()}
}

func newThroughputUsagePerNamespace(gen uint8, quota *Quota, quotaUsage *QuotaUsage) *throughputUsagePerNamespace {
	var tp throughputUsagePerNamespace
	tp.quotaInfosv.Store(&throughputUsagePerNamespaceQuotaInfo{
		gen:   gen,
		quota: quota,
		usage: quotaUsage,
	})
	tp.dpRecorderv.Store(newThroughputUsageDataPointsRecorder())

	return &tp
}

func (q *throughputUsagePerNamespace) quotaInfos() *throughputUsagePerNamespaceQuotaInfo {
	return q.quotaInfosv.Load().(*throughputUsagePerNamespaceQuotaInfo)
}

func (q *throughputUsagePerNamespace) quota() *Quota {
	return q.quotaInfos().quota
}

func (q *throughputUsagePerNamespace) quotaUsage() *QuotaUsage {
	return q.quotaInfos().usage
}

func (q *throughputUsagePerNamespace) dpRecorder() *throughputUsageDataPointsRecoder {
	return q.dpRecorderv.Load().(*throughputUsageDataPointsRecoder)
}

func (q *throughputUsagePerNamespace) increase(x int64) {
	atomic.AddInt64(&q.dpRecorder().dataPoints, x)
}

// offset tries to avoid a potential over-enforcement due to a race condition by
// updating resetAt and dataPoints with struct wrapper
// (i.e. throughputUsagePerNamespace).
//
// if go-carbon updates resetAt and dataPoints separately, there is a potential
// edge case might be triggered:
//
// supposed quota is 1000:
//
// throughputUsagePerNamespace.offset  throughputUsagePerNamespace.withinQuota
// dataPoints -> 1024
// resetAt    -> now-1m
//                                     dataPoints -> 1024
// dataPoints -> 0
// resetAt    -> now
//                                     resetAt    -> now
//
//                                     incorrect quota over enforcement as
//                                     dataPoints is already 0 by the time
//                                     withinQuota retrieves resetAt
//
// the downside of this approach is that go-carbon might still be updating on
// the counter in the replaced recorder, causing some mis-report or
// under-enforcement.
//
// under-enforcement is favored here.
func (q *throughputUsagePerNamespace) offset() *throughputUsageDataPointsRecoder {
	oldRecorder := q.dpRecorder()
	q.dpRecorderv.Store(newThroughputUsageDataPointsRecorder())

	return oldRecorder
}

// the current throughput enforcement is largely a fixed window based rate
// limiting solution, with some gentle processing around the edge of reset
// timer: when go-carbon fails to reset the counter timely (specified by
// resetFrequency), it would calculate the average points per second and use it
// get the full quota since the last reset timestamp
// (avg_pps_quota * seconds_since_last_reset), and it only drops data if the
// current usage is above that dynamically calculated result.
func (q *throughputUsagePerNamespace) withinQuota(c int64, resetFrequency time.Duration) bool {
	quota := q.quota().Throughput
	recorder := q.dpRecorder()
	usage := recorder.dataPoints
	if quota > 0 && usage+c > quota {
		// slow path: ensure if it isn't due to untimely usage reset
		//
		// why float64: to keep avg per data points more accurate, for
		// example, 61/60 could be rounded to 1 with int64.
		rseconds := float64(time.Since(recorder.resetAt).Seconds())
		fseconds := float64(resetFrequency.Seconds())

		// if for some really bad reason resetFrequency is 0, go-carbon
		// would accept the data.
		if fseconds == 0 {
			return true
		}

		// if the reset window is still within resetFrequency, go-carbon
		// could safely drop the data.
		if rseconds < fseconds {
			return false
		}

		// quota per seconds * elapsed time since last usage reset
		if float64(usage) > float64(quota)*(rseconds/fseconds) {
			return false
		}
	}

	return true
}

type throughputQuotaManager struct {
	depth int64

	// key:   materialized metric path of quota glob query specified in quota config
	// value: *throughputUsagePerNamespace
	entries sync.Map
}

func newQuotaThroughputQuotaManager() *throughputQuotaManager {
	return &throughputQuotaManager{}
}

func (tu *throughputQuotaManager) load(path string) *throughputUsagePerNamespace {
	v, ok := tu.entries.Load(path)
	if !ok {
		return nil
	}

	return v.(*throughputUsagePerNamespace)
}

func (tu *throughputQuotaManager) store(path string, tuq *throughputUsagePerNamespace) {
	v, loaded := tu.entries.LoadOrStore(path, tuq)
	if !loaded {
		return
	}

	v.(*throughputUsagePerNamespace).quotaInfosv.Store(tuq.quotaInfos())
}

func (tu *throughputQuotaManager) setDepth(depth int) { atomic.AddInt64(&tu.depth, int64(depth)) }
func (tu *throughputQuotaManager) getDepth() int      { return int(atomic.LoadInt64(&tu.depth)) }

// NOTE: Throughput is checked separately by throughputQuotaManager
type QuotaUsage struct {
	Namespaces   int64 // top level subdirectories
	Metrics      int64 // files
	LogicalSize  int64
	PhysicalSize int64
	DataPoints   int64 // inferred from retention policy
	Throttled    int64
}

func (q *QuotaUsage) String() string {
	// TODO: maybe consider retrieve throughput usage from throughputQuotaManager?
	return fmt.Sprintf("dirs:%d,files:%d,points:%d,logical:%d,physical:%d,throttled:%d", q.Namespaces, q.Metrics, q.DataPoints, q.LogicalSize, q.PhysicalSize, q.Throttled)
}

// applyQuotas applies quotas on new and old dirnodes. It can't be evoked
// concurrently during trieIndex.insert.
//
// this method is not goroutine-safe.
func (ti *trieIndex) applyQuotas(resetFrequency time.Duration, quotas ...*Quota) (*throughputQuotaManager, error) {
	ti.setResetFrequency(resetFrequency)

	// caveat (why updateChecker is needed):
	//
	// the current quota config file is last-match-wins, and with heavy
	// concurrent quota enforcement, go-carbon should only update quota
	// info per node once, otherwise it risks of having confusing and
	// incorrect quota enforcement.
	//
	// for example, suppose we have the following quota configs:
	//
	//     [sys.*]
	//         throughput = 1024
	//     [sys.app]
	//         throughput = 4096
	//
	// if sys.app is updated twice using top down order in the quota config
	// file, there is a window that sys.app would have a quota with
	// throughput of 1024, and if the namespace happen to be receiving more
	// than 1024 data points during that window, it would trigger incorrect
	// throttling.
	//
	// TODO: with updateChecker, it's also straightforward now to support
	// first-match-wins in quota config file, but we would have to
	// introduce a new flat to ask for it for backward compatibility.
	updateChecker := map[string]bool{}

	for j := len(quotas) - 1; j >= 0; j-- {
		quota := quotas[j]
		if quota.Pattern == "/" {
			if !updateChecker["/"] {
				meta := ti.root.meta.(*dirMeta)
				meta.update(quota)
				ti.throughputs.store("/", newThroughputUsagePerNamespace(ti.root.gen, quota, meta.usage))

				updateChecker["/"] = true
			}

			continue
		}

		paths, _, nodes, _, err := ti.query(strings.ReplaceAll(quota.Pattern, ".", "/"), 1<<31-1, nil)
		if err != nil {
			return nil, err
		}

		for i, node := range nodes {
			if node.meta == nil {
				node.meta = newDirMeta()
			}

			meta, ok := node.meta.(*dirMeta)
			if !ok {
				// could be a fileMeta
				continue
			}

			if c := strings.Count(paths[i], "."); c > ti.throughputs.getDepth() {
				ti.throughputs.setDepth(c)
			}

			if !updateChecker[paths[i]] {
				ti.throughputs.store(paths[i], newThroughputUsagePerNamespace(ti.root.gen, quota, meta.usage))
				meta.update(quota)

				updateChecker[paths[i]] = true
			}
		}
	}

	// delete stale entries
	ti.throughputs.entries.Range(func(k, v interface{}) bool {
		if v.(*throughputUsagePerNamespace).quotaInfos().gen != ti.root.gen {
			ti.throughputs.entries.Delete(k)
		}
		return true
	})

	return ti.throughputs, nil
}

// refreshUsage updates usage data and generate stat metrics.
// It can't be evoked with concurrent trieIndex.insert.
func (ti *trieIndex) refreshUsage(throughputs *throughputQuotaManager) (files uint64) {
	if throughputs == nil {
		throughputs = newQuotaThroughputQuotaManager()
	}

	type state struct {
		next      int
		node      *trieNode
		childrens *[]*trieNode

		files        int64
		logicalSize  int64
		physicalSize int64
		dataPoints   int64
		readHits     int64
		readBytes    int64
	}

	var idx int
	var depth = ti.getDepth() + trieDepthBuffer
	var states = make([]state, depth)

	var pstate *state
	var cur = &states[idx]
	cur.node = ti.root
	cur.childrens = cur.node.childrens

	var dirs = make([]int64, depth)
	var dirIndex int

	// Drops unflushed metrics to avoid overusing memories, as timestamp is
	// set by app.Collector, keeping old stats helps no one.
	ti.qauMetrics = ti.qauMetrics[:0]

	for {
		if cur.next >= len(*cur.childrens) {
			if (cur.node.dir() || cur.node == ti.root) && dirIndex >= 0 {
				if cur.node.meta != nil && cur.node.meta.(*dirMeta) != nil && cur.node.meta.(*dirMeta).usage != nil {
					usage := cur.node.meta.(*dirMeta).usage
					atomic.StoreInt64(&usage.Namespaces, dirs[dirIndex])
					atomic.StoreInt64(&usage.Metrics, cur.files)
					atomic.StoreInt64(&usage.LogicalSize, cur.logicalSize)
					atomic.StoreInt64(&usage.PhysicalSize, cur.physicalSize)
					atomic.StoreInt64(&usage.DataPoints, cur.dataPoints)

					var name, tname string
					if cur.node == ti.root {
						name = "root"
						tname = "/"
					} else {
						var nodes []*trieNode
						for i := 0; i < idx; i++ {
							nodes = append(nodes, states[i].node)
						}
						name = ti.root.fullPath('.', nodes)
						tname = name
					}
					var throughput int64
					if te := throughputs.load(tname); te != nil {
						throughput = te.offset().dataPoints
					}

					throttled := atomic.LoadInt64(&usage.Throttled)
					metricName := ti.metricName(cur.node, name)
					ti.generateTrieMetrics(metricName, cur.node, throughput, throttled, cur.readHits, cur.readBytes)
					if throttled > 0 {
						atomic.AddInt64(&usage.Throttled, -throttled)
					}
				}

				dirIndex--
			}

			goto parent
		}

		idx++
		if idx >= len(states)-1 {
			goto parent
		}

		states[idx] = state{} // reset to zero value

		pstate = &states[idx-1]
		cur = &states[idx]
		cur.node = (*pstate.node.childrens)[pstate.next]
		cur.childrens = cur.node.childrens

		if cur.node.file() {
			cur.files++
			cur.logicalSize += cur.node.meta.(*fileMeta).logicalSize
			cur.physicalSize += cur.node.meta.(*fileMeta).physicalSize
			cur.dataPoints += cur.node.meta.(*fileMeta).dataPoints
			deltaReadHits := atomic.LoadInt64(&cur.node.meta.(*fileMeta).readHits)
			cur.readHits += deltaReadHits
			atomic.AddInt64(&cur.node.meta.(*fileMeta).readHits, -deltaReadHits)
			deltaReadBytes := atomic.LoadInt64(&cur.node.meta.(*fileMeta).readBytes)
			cur.readBytes += deltaReadBytes
			atomic.AddInt64(&cur.node.meta.(*fileMeta).readBytes, -deltaReadBytes)
			files++

			goto parent
		} else if cur.node.dir() {
			dirs[dirIndex]++
			dirIndex++
			dirs[dirIndex] = 0
		}

		continue

	parent:
		files := cur.files
		logicalSize := cur.logicalSize
		physicalSize := cur.physicalSize
		dataPoints := cur.dataPoints
		readHits := cur.readHits
		readBytes := cur.readBytes
		states[idx] = state{}
		idx--
		if idx < 0 {
			break
		}

		cur = &states[idx]
		cur.files += files
		cur.logicalSize += logicalSize
		cur.physicalSize += physicalSize
		cur.dataPoints += dataPoints
		cur.readHits += readHits
		cur.readBytes += readBytes
		cur.next++

		continue
	}

	return
}

func (ti *trieIndex) generateTrieMetrics(metricName string, node *trieNode, throughput, throttled, readHits, readBytes int64) {

	// Note: Timestamp for each points.Points are set by collector send logics
	meta := node.meta.(*dirMeta)
	quota := meta.quota.Load().(*Quota)
	if quota.Namespaces > 0 {
		ti.qauMetrics = append(
			ti.qauMetrics,
			points.Points{
				Metric: fmt.Sprintf("quota.namespaces.%s", metricName),
				Data: []points.Point{{
					Value: float64(quota.Namespaces),
				}},
			},
			points.Points{
				Metric: fmt.Sprintf("usage.namespaces.%s", metricName),
				Data: []points.Point{{
					Value: float64(atomic.LoadInt64(&meta.usage.Namespaces)),
				}},
			},
		)
	}
	if quota.Metrics > 0 {
		ti.qauMetrics = append(
			ti.qauMetrics, points.Points{
				Metric: fmt.Sprintf("quota.metrics.%s", metricName),
				Data: []points.Point{{
					Value: float64(quota.Metrics),
				}},
			},
			points.Points{
				Metric: fmt.Sprintf("usage.metrics.%s", metricName),
				Data: []points.Point{{
					Value: float64(atomic.LoadInt64(&meta.usage.Metrics)),
				}},
			},
		)
	}
	if quota.DataPoints > 0 {
		ti.qauMetrics = append(
			ti.qauMetrics,
			points.Points{
				Metric: fmt.Sprintf("quota.data_points.%s", metricName),
				Data: []points.Point{{
					Value: float64(quota.DataPoints),
				}},
			},
			points.Points{
				Metric: fmt.Sprintf("usage.data_points.%s", metricName),
				Data: []points.Point{{
					Value: float64(atomic.LoadInt64(&meta.usage.DataPoints)),
				}},
			},
		)
	}
	if quota.LogicalSize > 0 {
		ti.qauMetrics = append(
			ti.qauMetrics,
			points.Points{
				Metric: fmt.Sprintf("quota.logical_size.%s", metricName),
				Data: []points.Point{{
					Value: float64(quota.LogicalSize),
				}},
			},
			points.Points{
				Metric: fmt.Sprintf("usage.logical_size.%s", metricName),
				Data: []points.Point{{
					Value: float64(atomic.LoadInt64(&meta.usage.LogicalSize)),
				}},
			},
		)
	}
	if quota.PhysicalSize > 0 {
		ti.qauMetrics = append(
			ti.qauMetrics,
			points.Points{
				Metric: fmt.Sprintf("quota.physical_size.%s", metricName),
				Data: []points.Point{{
					Value: float64(quota.PhysicalSize),
				}},
			},
			points.Points{
				Metric: fmt.Sprintf("usage.physical_size.%s", metricName),
				Data: []points.Point{{
					Value: float64(atomic.LoadInt64(&meta.usage.PhysicalSize)),
				}},
			},
		)
	}
	if quota.Throughput > 0 {
		ti.qauMetrics = append(
			ti.qauMetrics, points.Points{
				Metric: fmt.Sprintf("quota.throughput.%s", metricName),
				Data: []points.Point{{
					Value: float64(quota.Throughput),
				}},
			},
			points.Points{
				Metric: fmt.Sprintf("usage.throughput.%s", metricName),
				Data: []points.Point{{
					Value: float64(throughput),
				}},
			},
		)
	}

	ti.qauMetrics = append(ti.qauMetrics, points.Points{
		Metric: fmt.Sprintf("throttle.%s", metricName),
		Data: []points.Point{{
			Value: float64(throttled),
		}},
	})
	ti.qauMetrics = append(ti.qauMetrics, points.Points{
		Metric: fmt.Sprintf("read_hits.%s", metricName),
		Data: []points.Point{{
			Value: float64(readHits),
		}},
	})
	ti.qauMetrics = append(ti.qauMetrics, points.Points{
		Metric: fmt.Sprintf("read_bytes.%s", metricName),
		Data: []points.Point{{
			Value: float64(readBytes),
		}},
	})
}

//nolint:golint,unused
func (ti *trieIndex) getNodeFullPath(node *trieNode) string { // skipcq: SCC-U1000
	//nolint:golint,unused
	type state struct { // skipcq: SCC-U1000
		next      int
		node      *trieNode
		childrens *[]*trieNode
	}

	var idx int
	var depth = ti.getDepth() + trieDepthBuffer
	var states = make([]state, depth)

	var cur = &states[idx]
	cur.node = ti.root
	cur.childrens = cur.node.childrens

	for {
		if cur.next >= len(*cur.childrens) {
			goto parent
		}

		idx++
		if idx >= len(states)-1 {
			goto parent
		}

		cur = &states[idx]
		cur.next = 0
		cur.node = (*states[idx-1].childrens)[states[idx-1].next]
		cur.childrens = cur.node.childrens

		if cur.node == node {
			var parents []*trieNode
			for i := 0; i < idx; i++ {
				parents = append(parents, states[i].node)
			}

			return cur.node.fullPath('.', parents)
		}

		if cur.node.file() {
			goto parent
		}

		continue

	parent:
		states[idx] = state{}
		idx--
		if idx < 0 {
			break
		}

		cur = &states[idx]
		cur.next++

		continue
	}

	return ""
}

// skipcq: RVV-A0005
func (ti *trieIndex) throttle(ps *points.Points, inCache bool) bool {
	dataLen := int64(len(ps.Data))
	if dataLen == 0 {
		// WHY: in theory, this should not happen. But in cases where
		// go-carbon receives a metric without data points, it should
		// still be counted as one data point, for throttling and
		// throughput accounting.
		dataLen = 1
	}

	// WHY: hashes work much faster than the trie tree working.
	//
	// When checking throughput usage using trie traversal, for go-carbon
	// instances that are seeing more than 1 millions metrics/data points
	// per second, cpu usage growth is about 700%, which isn't ideal. By
	// using pure hash maps, we are able to cut down cpu usage grwoth to 100%.
	if throughputs := ti.throughputs; throughputs != nil {
		// For simplicity of implementation, we would either ingest all
		// data points or throttle all of them if throughput quota is
		// exceeded. It seems not nice, but in real life, we are
		// usually looking 1 or a few data points per metric when
		// checking throughput. And throughput quota is usually at the
		// order of 100k or more. It's less likely to be an issue. At
		// the same time, partial ingestion would lead to
		// over-consumption of quota, which itself is arguably as bad
		// as full throttling.

		tus := make([]*throughputUsagePerNamespace, 0, 8)
		resetFreq := ti.getResetFrequency()

		// TODO: should include in throughputs.depth and simplify the code here a bit.
		// check root throughput capacity
		if v := throughputs.load("/"); v != nil {
			if !v.withinQuota(dataLen, resetFreq) {
				atomic.AddInt64(&v.quotaUsage().Throttled, dataLen)

				if v.quota().DroppingPolicy != QDPNone {
					return true
				}
			}

			tus = append(tus, v)
		}

		for i, d, maxd := 0, 0, throughputs.getDepth(); d <= maxd && i < len(ps.Metric); i++ {
			if ps.Metric[i] != '.' {
				continue
			}

			ns := ps.Metric[:i]
			if v := throughputs.load(ns); v != nil {
				if !v.withinQuota(dataLen, resetFreq) {
					atomic.AddInt64(&v.quotaUsage().Throttled, dataLen)

					if v.quota().DroppingPolicy != QDPNone {
						return true
					}
				}

				tus = append(tus, v)
			}
			d++
		}

		// batch increasing the counter to avoid potential
		// over-throttling/accounting in parent nodes as the data is
		// later on dropped due to lower child quotas.
		for _, tu := range tus {
			tu.increase(dataLen)
		}
	}

	// quick pass if the metric is already seen in cache
	if inCache {
		return false
	}

	var node = ti.root
	var dirs = make([]*trieNode, 0, 32) // WHY: reduce the majority of allocations in mloop
	var mindex int
	var isNew bool
	var metric = ps.Metric

	dirs = append(dirs, ti.root)

mloop:
	for {
		cindex := 0
		for ; cindex < len(node.c) && mindex < len(metric); cindex++ {
			if node.c[cindex] != metric[mindex] {
				isNew = true
				break mloop
			}

			mindex++
		}

		if cindex < len(node.c) {
			isNew = true
			break
		}

		childrens := node.getChildrens()

		if mindex < len(metric) && metric[mindex] == '.' {
			var dir *trieNode
			for i := 0; i < len(childrens); i++ {
				child := node.getChild(childrens, i)
				if !child.dir() {
					continue
				}

				dirs = append(dirs, child)
				dir = child
				break
			}

			if dir == nil {
				isNew = true
				break
			}

			node = dir
			childrens = dir.getChildrens()
			mindex++
		}

		for i := 0; i < len(childrens); i++ {
			child := node.getChild(childrens, i)
			if mindex >= len(metric) {
				if child.file() {
					break mloop
				}

				continue
			}

			if len(child.c) > 0 && child.c[0] == metric[mindex] {
				node = child
				continue mloop
			}
		}

		isNew = true
		break
	}

	if !isNew {
		return false
	}

	size, dataPoints := ti.estimateSize(metric)

	var toThrottle bool
	for i, n := range dirs {
		// TODO: might need more considerations?
		var namespaces int64
		if i == len(dirs)-1 {
			namespaces = 1
		}

		meta, ok := n.meta.(*dirMeta)
		if !ok || meta.withinQuota(1, namespaces, size, size, dataPoints) {
			continue
		}

		if meta.usage != nil {
			atomic.AddInt64(&meta.usage.Throttled, dataLen)
		}

		if quota, ok := meta.quota.Load().(*Quota); ok && quota != nil && quota.DroppingPolicy == QDPNone {
			continue
		}

		toThrottle = true
		break
	}

	return toThrottle
}

func (*trieIndex) metricName(node *trieNode, name string) string {
	var prefix string
	if quota, ok := node.meta.(*dirMeta).quota.Load().(*Quota); ok {
		prefix = quota.StatMetricPrefix
	}
	name = strings.ReplaceAll(name, ".", "-")
	// WHY: on linux, the maximum filename length is 255, keeping 5 here for
	// file extension.
	if len(name) >= 250 {
		// skipcq: GSC-G401, GO-S1023
		name = fmt.Sprintf("%s-%x", name[:(250-md5.Size*2-1)], md5.Sum([]byte(name)))
	}
	return prefix + name
}
