package carbonserver

// trieBlock packs pointer-free metadata into stable allocations. Do not put
// nodes or child headers in these blocks: a retired node would keep its former
// subtree alive as long as any other node in the same block remains reachable.
type trieBlock[T any] struct {
	remaining []T
}

func (b *trieBlock[T]) alloc() *T {
	if len(b.remaining) == 0 {
		b.remaining = make([]T, 128)
	}
	p := &b.remaining[0]
	b.remaining = b.remaining[1:]
	return p
}

// trieBulkBuilder is private to the single initial-index writer. Readers never
// observe mutable child slice headers; ordinary atomic publication resumes when
// the completed index becomes visible.
type trieBulkBuilder struct {
	metaBlock   trieBlock[fileMeta]
	labels      []byte
	nodes, dirs int
	prefix      []byte
	directories []bulkDirectory
	generation  uint8
}

type bulkDirectory struct {
	end  int
	node *trieNode
}

// Keep each node's child header in its own allocation. Unlike a shared node
// arena, this does not pin retired sibling subtrees after the builder is gone.
type bulkTrieNode struct {
	node     trieNode
	children []*trieNode
}

// Directory sentinels survive radix splits, so the private builder can resume
// below a shared namespace instead of walking it again for every metric. The
// retained prefix is owned storage; decoded file-list buffers remain borrowed.
func bulkDirectoryMatch[P string | []byte](b *trieBulkBuilder, path P) int {
	for i := len(b.directories) - 1; i >= 0; i-- {
		end := b.directories[i].end
		if end < len(path) && string(b.prefix[:end]) == string(path[:end]) {
			return i
		}
	}
	return -1
}

func bulkDirectoryStart[P string | []byte](ti *trieIndex, path P) (int, *trieNode) {
	b := ti.builder
	if b == nil {
		return 0, ti.root
	}
	if b.generation != ti.root.gen {
		b.directories = b.directories[:0]
		b.generation = ti.root.gen
	}
	matched := bulkDirectoryMatch(b, path)
	b.directories = b.directories[:matched+1]
	if matched < 0 {
		b.prefix = b.prefix[:0]
		return 0, ti.root
	}
	dir := b.directories[matched]
	b.prefix = b.prefix[:dir.end]
	return dir.end, dir.node
}

func rememberBulkDirectory[P string | []byte](ti *trieIndex, path P, end int, node *trieNode) {
	if b := ti.builder; b != nil {
		for i := len(b.prefix); i < end; i++ {
			b.prefix = append(b.prefix, path[i])
		}
		b.directories = append(b.directories, bulkDirectory{end: end, node: node})
	}
}

func (ti *trieIndex) makeNode(label []byte, children *[]*trieNode, generation uint8) *trieNode {
	if ti.builder == nil {
		return &trieNode{c: label, childrens: children, gen: generation}
	}
	ti.builder.nodes++
	n := &bulkTrieNode{node: trieNode{c: label, gen: generation}, children: *children}
	n.node.childrens = &n.children
	return &n.node
}

func copyTrieLabel[P string | []byte](ti *trieIndex, label P) []byte {
	if ti.builder == nil {
		return []byte(string(label))
	}
	b := ti.builder
	if len(b.labels) < len(label) {
		b.labels = make([]byte, max(16*1024, len(label)))
	}
	buf := b.labels[:len(label):len(label)]
	for i := 0; i < len(label); i++ {
		buf[i] = label[i]
	}
	b.labels = b.labels[len(label):]
	return buf
}

func (ti *trieIndex) appendChild(parent, child *trieNode) {
	if ti.builder == nil {
		parent.addChild(child)
		return
	}
	if parent.childrens == emptyTrieNodes {
		parent.childrens = new([]*trieNode)
	}
	*parent.childrens = append(*parent.childrens, child)
}

func (ti *trieIndex) makeFileNode(logicalSize, physicalSize, dataPoints, firstSeenAt int64) *trieNode {
	if ti.builder == nil {
		return newFileNode(ti.root.gen, logicalSize, physicalSize, dataPoints, firstSeenAt)
	}
	// File sentinels never acquire children, so they need no private header.
	n := &trieNode{childrens: emptyTrieNodes, gen: ti.root.gen}
	ti.builder.nodes++
	m := ti.builder.metaBlock.alloc()
	*m = fileMeta{logicalSize: logicalSize, physicalSize: physicalSize, dataPoints: dataPoints, firstSeenAt: firstSeenAt}
	n.meta.Store(m)
	return n
}
