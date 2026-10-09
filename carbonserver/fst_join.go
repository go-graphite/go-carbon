package carbonserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/blevesearch/vellum"
)

// vellum v1 layout constants (encoder_v1.go, encoding.go). The joined file is
// a regular v1 FST, so every existing reader and query keeps working.
const (
	fstHeaderSize    = 16
	fstFooterSize    = 16
	fstVersion       = 1
	fstNoneAddr      = 1
	fstStateFinal    = 1 << 6
	fstMaxInlineTran = 1<<6 - 1
)

// fstShard is an FST built over one contiguous key range with values starting
// at zero. vellum stores every transition target relative to the node that
// references it, so a shard's node bytes stay valid when appended elsewhere as
// one block. Only nodes on prefixes shared with neighbouring shards are
// re-encoded; they all lie on the paths of the shard's first and last key.
type fstShard struct {
	count       uint64
	first, last []byte
	left, right []fstNode // nodes along first/last key, indexed by depth
	size        int       // node bytes without header and footer
	offset      int       // position of the node bytes in the joined file
	base        uint64    // joined value of the shard's first key
}

type fstNode struct {
	final    bool
	finalOut uint64
	trans    []fstTransition // ascending input bytes
}

type fstTransition struct {
	in   byte
	out  uint64
	addr int // shard-local, or joined-file address once re-encoded
}

// newFSTShard describes a complete vellum FST of count keys: rows 0..count-1
// in key order, or any values for a value joiner. The joiner appends its node
// bytes, fstShardBody(data).
// leftDepth and rightDepth bound the prefixes of the first and last key that a
// neighbouring shard may share: at most the common prefix with its keys.
func newFSTShard(data, first, last []byte, count uint64, leftDepth, rightDepth int) (*fstShard, error) {
	fst, err := vellum.Load(data)
	if err != nil {
		return nil, err
	}
	defer fst.Close()
	if uint64(fst.Len()) != count || count == 0 {
		return nil, fmt.Errorf("fst shard has %d keys, expected %d", fst.Len(), count)
	}
	s := &fstShard{count: count, first: first, last: last, size: len(fstShardBody(data))}
	if s.left, err = fstPathNodes(fst, first[:min(leftDepth, len(first))]); err != nil {
		return nil, err
	}
	if s.right, err = fstPathNodes(fst, last[:min(rightDepth, len(last))]); err != nil {
		return nil, err
	}
	return s, nil
}

// fstShardBody returns the node bytes of a complete FST.
func fstShardBody(data []byte) []byte {
	return data[fstHeaderSize : len(data)-fstFooterSize]
}

// fstPathNodes describes the nodes reached by every prefix of key.
func fstPathNodes(fst *vellum.FST, key []byte) ([]fstNode, error) {
	nodes := make([]fstNode, 0, len(key)+1)
	addr := fst.Start()
	for depth := 0; ; depth++ {
		var n fstNode
		n.final, n.finalOut = fst.IsMatchWithVal(addr)
		// The public API exposes transitions only by input byte. These paths
		// are short and probed once per shard, in parallel with other shards.
		for c := range 256 {
			if next, out := fst.AcceptWithVal(addr, byte(c)); next != fstNoneAddr {
				n.trans = append(n.trans, fstTransition{byte(c), out, next})
			}
		}
		nodes = append(nodes, n)
		if depth == len(key) {
			return nodes, nil
		}
		next := fst.Accept(addr, key[depth])
		if next == fstNoneAddr {
			return nil, fmt.Errorf("fst shard does not contain its boundary keys")
		}
		addr = next
	}
}

// node returns the shard node reached by prefix, which must be a prefix of its
// first or last key. Ranges shared with another shard always are.
func (s *fstShard) node(prefix []byte) (*fstNode, bool) {
	d := len(prefix)
	if d < len(s.left) && bytes.Equal(s.first[:d], prefix) {
		return &s.left[d], true
	}
	if d < len(s.right) && bytes.Equal(s.last[:d], prefix) {
		return &s.right[d], true
	}
	return nil, false
}

func (s *fstShard) address(local int) int {
	if local == 0 { // the shared empty final state
		return 0
	}
	return s.offset + local - fstHeaderSize
}

// fstJoiner appends shard bodies in key order, then writes the merged nodes
// for shared prefixes, the root and the footer.
type fstJoiner struct {
	w      io.Writer
	pos    int
	count  uint64
	shards []*fstShard
	buf    []byte
	values bool // shard values are kept as they are instead of numbered on
}

// newFSTJoiner joins shards whose values are rows numbered from zero: the
// joined FST numbers rows across all shards.
func newFSTJoiner(w io.Writer) (*fstJoiner, error) {
	return newFSTJoinerMode(w, false)
}

// newFSTValueJoiner joins shards whose values are kept unchanged.
func newFSTValueJoiner(w io.Writer) (*fstJoiner, error) {
	return newFSTJoinerMode(w, true)
}

func newFSTJoinerMode(w io.Writer, values bool) (*fstJoiner, error) {
	j := &fstJoiner{w: w, values: values}
	header := binary.LittleEndian.AppendUint64(nil, fstVersion)
	header = binary.LittleEndian.AppendUint64(header, 0) // type
	return j, j.write(header)
}

func (j *fstJoiner) write(b []byte) error {
	n, err := j.w.Write(b)
	j.pos += n
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}

// add appends the node bytes of s, read from body.
func (j *fstJoiner) add(s *fstShard, body io.Reader) error {
	if len(j.shards) > 0 && bytes.Compare(j.shards[len(j.shards)-1].last, s.first) >= 0 {
		return fmt.Errorf("fst shards are not strictly ordered")
	}
	s.offset, s.base = j.pos, j.count
	if j.values {
		s.base = 0
	}
	n, err := io.CopyN(j.w, body, int64(s.size))
	j.pos += int(n)
	if err != nil {
		return err
	}
	j.count += s.count
	j.shards = append(j.shards, s)
	return nil
}

type fstMergeRef struct {
	shard *fstShard
	node  *fstNode
	acc   uint64 // shard-local output accumulated along the prefix
}

func (j *fstJoiner) finish() error {
	refs := make([]fstMergeRef, 0, len(j.shards))
	for _, s := range j.shards {
		node, _ := s.node(nil)
		refs = append(refs, fstMergeRef{shard: s, node: node})
	}
	root, err := j.merge(nil, refs)
	if err != nil {
		return err
	}
	footer := binary.LittleEndian.AppendUint64(nil, j.count)
	footer = binary.LittleEndian.AppendUint64(footer, uint64(root))
	return j.write(footer)
}

// merge writes the joined node for prefix, children first. A key's joined value
// is its shard base plus its local value, so each shard's outputs below the
// node are raised by the difference between its own accumulated value and the
// joined node's accumulated value, which is the smallest of them.
func (j *fstJoiner) merge(prefix []byte, refs []fstMergeRef) (int, error) {
	var acc uint64
	for i, r := range refs {
		if v := r.shard.base + r.acc; i == 0 || v < acc {
			acc = v
		}
	}
	var node fstNode
	cursor := make([]int, len(refs))
	group := make([]fstMergeRef, 0, len(refs))
	for c := range 256 {
		group = group[:0]
		var only fstTransition
		for i, r := range refs {
			if k := cursor[i]; k < len(r.node.trans) && r.node.trans[k].in == byte(c) {
				cursor[i]++
				only = r.node.trans[k]
				group = append(group, fstMergeRef{shard: r.shard, acc: r.acc + only.out})
				only.out += r.shard.base + r.acc - acc
				only.addr = r.shard.address(only.addr)
			}
		}
		switch len(group) {
		case 0:
			continue
		case 1:
			node.trans = append(node.trans, only)
			continue
		}
		child := append(bytes.Clone(prefix), byte(c))
		childAcc := group[0].shard.base + group[0].acc
		for i := range group {
			var ok bool
			if group[i].node, ok = group[i].shard.node(child); !ok {
				return 0, fmt.Errorf("fst shard boundary does not cover a shared prefix")
			}
			childAcc = min(childAcc, group[i].shard.base+group[i].acc)
		}
		addr, err := j.merge(child, group)
		if err != nil {
			return 0, err
		}
		node.trans = append(node.trans, fstTransition{byte(c), childAcc - acc, addr})
	}
	for _, r := range refs {
		if r.node.final {
			if node.final {
				return 0, fmt.Errorf("fst shards repeat a key")
			}
			node.final, node.finalOut = true, r.node.finalOut+r.shard.base+r.acc-acc
		}
	}
	return j.writeNode(&node)
}

// writeNode encodes n in vellum's multi-transition form and returns its address.
func (j *fstJoiner) writeNode(n *fstNode) (int, error) {
	start := uint64(j.pos)
	transSize, outSize := 1, fstPackedSize(n.finalOut)
	outputs := n.final && n.finalOut != 0
	delta := func(t fstTransition) uint64 {
		if t.addr == 0 {
			return 0
		}
		return start - uint64(t.addr)
	}
	for _, t := range n.trans {
		transSize = max(transSize, fstPackedSize(delta(t)))
		outSize = max(outSize, fstPackedSize(t.out))
		outputs = outputs || t.out != 0
	}
	if !outputs {
		outSize = 0
	}
	b := j.buf[:0]
	if outputs {
		if n.final {
			b = fstAppendPacked(b, n.finalOut, outSize)
		}
		for i := len(n.trans) - 1; i >= 0; i-- {
			b = fstAppendPacked(b, n.trans[i].out, outSize)
		}
	}
	for i := len(n.trans) - 1; i >= 0; i-- {
		b = fstAppendPacked(b, delta(n.trans[i]), transSize)
	}
	for i := len(n.trans) - 1; i >= 0; i-- {
		b = append(b, n.trans[i].in)
	}
	b = append(b, byte(transSize<<4|outSize))
	header := byte(0)
	if len(n.trans) <= fstMaxInlineTran {
		header = byte(len(n.trans))
	}
	if header == 0 {
		// 256 transitions do not fit a byte; vellum stores them as 1, which a
		// count byte otherwise never holds.
		b = append(b, byte(len(n.trans)))
		if len(n.trans) == 256 {
			b[len(b)-1] = 1
		}
	}
	if n.final {
		header |= fstStateFinal
	}
	b = append(b, header)
	j.buf = b
	if err := j.write(b); err != nil {
		return 0, err
	}
	return j.pos - 1, nil
}

func fstPackedSize(v uint64) int {
	size := 1
	for v >>= 8; v > 0; v >>= 8 {
		size++
	}
	return size
}

func fstAppendPacked(b []byte, v uint64, size int) []byte {
	for i := range size {
		b = append(b, byte(v>>(8*i)))
	}
	return b
}
