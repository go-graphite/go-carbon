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
}

func (ti *trieIndex) makeNode(label []byte, children *[]*trieNode, generation uint8) *trieNode {
	if ti.builder == nil {
		return &trieNode{c: label, childrens: children, gen: generation}
	}
	ti.builder.nodes++
	return &trieNode{c: label, childrens: children, gen: generation}
}

func (ti *trieIndex) copyLabel(label string) []byte {
	if ti.builder == nil {
		return []byte(label)
	}
	b := ti.builder
	if len(b.labels) < len(label) {
		b.labels = make([]byte, max(16*1024, len(label)))
	}
	buf := b.labels[:len(label):len(label)]
	copy(buf, label)
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
	n := ti.makeNode(nil, emptyTrieNodes, ti.root.gen)
	m := ti.builder.metaBlock.alloc()
	*m = fileMeta{logicalSize: logicalSize, physicalSize: physicalSize, dataPoints: dataPoints, firstSeenAt: firstSeenAt}
	n.meta = m
	return n
}
