package carbonserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/blevesearch/vellum"
)

// fstCursor reads the states of a vellum v1 FST directly. vellum exposes
// transitions only one input byte at a time and allocates a decoded state for
// each step; walking a directory's names needs every transition of a state,
// many times per scan. The layout mirrors vellum's decoder_v1.go.
type fstCursor struct {
	data []byte
	root int
}

// fstState is a position in the FST and the output accumulated to reach it.
// A zero addr with ok unset is a missing state.
type fstState struct {
	addr int
	out  uint64
	ok   bool
}

const (
	fstEmptyAddr       = 0
	fstOneTransition   = 1 << 7
	fstTransitionNext  = 1 << 6
	fstMaxCommonInputs = 1<<6 - 1
)

func newFSTCursor(data []byte) (*fstCursor, error) {
	if len(data) < fstHeaderSize+fstFooterSize || binary.LittleEndian.Uint64(data) != fstVersion {
		return nil, fmt.Errorf("unsupported fst encoding")
	}
	root := int(binary.LittleEndian.Uint64(data[len(data)-8:]))
	if root != fstEmptyAddr && root != fstNoneAddr && (root < fstHeaderSize || root >= len(data)-fstFooterSize) {
		return nil, fmt.Errorf("invalid fst root %d", root)
	}
	return &fstCursor{data: data, root: root}, nil
}

func (c *fstCursor) start() fstState {
	return fstState{addr: c.root, ok: c.root != fstNoneAddr}
}

// fstNodeView is one decoded state. Transition arrays are stored in reverse
// input order: index numTrans-1 holds the smallest input byte.
type fstNodeView struct {
	final              bool
	finalOut           uint64
	numTrans           int
	single             bool
	singleIn           byte
	singleAddr         int
	singleOut          uint64
	keys, dests, outs  []byte
	transSize, outSize int
	bottom             int
}

func (c *fstCursor) node(addr int, n *fstNodeView) bool {
	*n = fstNodeView{}
	if addr == fstEmptyAddr {
		n.final = true
		return true
	}
	if addr == fstNoneAddr || addr < fstHeaderSize || addr >= len(c.data) {
		return false
	}
	d := c.data
	top, bottom := addr, addr
	if d[top]&fstOneTransition != 0 {
		n.single, n.numTrans = true, 1
		next := d[top]&fstTransitionNext != 0
		in := d[top] & fstMaxCommonInputs
		if in == 0 {
			bottom--
			in = d[bottom]
		} else {
			in = fstDecodeCommon(in)
		}
		n.singleIn = in
		if next {
			n.singleAddr = bottom - 1
			return true
		}
		bottom--
		transSize, outSize := int(d[bottom]>>4), int(d[bottom]&0xf)
		bottom -= transSize
		delta := fstReadPacked(d[bottom : bottom+transSize])
		if outSize > 0 {
			bottom -= outSize
			n.singleOut = fstReadPacked(d[bottom : bottom+outSize])
		}
		if delta != 0 {
			n.singleAddr = bottom - int(delta)
		}
		return true
	}
	n.final = d[top]&fstStateFinal != 0
	n.numTrans = int(d[top] & fstMaxInlineTran)
	if n.numTrans == 0 {
		bottom--
		n.numTrans = int(d[bottom])
		if n.numTrans == 1 {
			n.numTrans = 256
		}
	}
	bottom--
	n.transSize, n.outSize = int(d[bottom]>>4), int(d[bottom]&0xf)
	n.keys = d[bottom-n.numTrans : bottom]
	bottom -= n.numTrans
	n.dests = d[bottom-n.numTrans*n.transSize : bottom]
	bottom -= n.numTrans * n.transSize
	if n.outSize > 0 {
		n.outs = d[bottom-n.numTrans*n.outSize : bottom]
		bottom -= n.numTrans * n.outSize
		if n.final {
			bottom -= n.outSize
			n.finalOut = fstReadPacked(d[bottom : bottom+n.outSize])
		}
	}
	n.bottom = bottom
	return true
}

// transition returns transition i in ascending input order.
func (n *fstNodeView) transition(i int) (in byte, addr int, out uint64) {
	if n.single {
		return n.singleIn, n.singleAddr, n.singleOut
	}
	pos := n.numTrans - 1 - i
	in = n.keys[pos]
	delta := int(fstReadPacked(n.dests[pos*n.transSize : (pos+1)*n.transSize]))
	if delta != 0 {
		addr = n.bottom - delta
	}
	if n.outSize > 0 {
		out = fstReadPacked(n.outs[pos*n.outSize : (pos+1)*n.outSize])
	}
	return in, addr, out
}

func (c *fstCursor) accept(s fstState, b byte) fstState {
	var n fstNodeView
	if !s.ok || !c.node(s.addr, &n) {
		return fstState{}
	}
	if n.single {
		if n.singleIn != b {
			return fstState{}
		}
		return fstState{addr: n.singleAddr, out: s.out + n.singleOut, ok: true}
	}
	pos := bytes.IndexByte(n.keys, b)
	if pos < 0 {
		return fstState{}
	}
	_, addr, out := n.transition(n.numTrans - 1 - pos)
	return fstState{addr: addr, out: s.out + out, ok: true}
}

func (c *fstCursor) acceptBytes(s fstState, b []byte) fstState {
	for i := 0; i < len(b) && s.ok; i++ {
		s = c.accept(s, b[i])
	}
	return s
}

func (c *fstCursor) acceptString(s fstState, b string) fstState {
	for i := 0; i < len(b) && s.ok; i++ {
		s = c.accept(s, b[i])
	}
	return s
}

// final reports whether s ends a key, and that key's value.
func (c *fstCursor) final(s fstState) (bool, uint64) {
	var n fstNodeView
	if !s.ok || !c.node(s.addr, &n) || !n.final {
		return false, 0
	}
	return true, s.out + n.finalOut
}

// names visits, in byte order, every name that continues s up to a 0 byte or
// the end of a key: the entries of the directory whose children start at s.
// For each name it reports the key's value if one ends there, and the state
// after the following 0 byte if longer keys continue.
// buf is scratch space for names, kept between calls.
func (c *fstCursor) names(s fstState, buf *[]byte, visit func(name []byte, final bool, value uint64, child fstState)) {
	if !s.ok {
		return
	}
	var walk func(s fstState, name []byte)
	walk = func(s fstState, name []byte) {
		if cap(name) > cap(*buf) {
			*buf = name[:0]
		}
		var n fstNodeView
		if !c.node(s.addr, &n) {
			return
		}
		first := 0
		var child fstState
		if n.numTrans > 0 {
			if in, addr, out := n.transition(0); in == 0 {
				child, first = fstState{addr: addr, out: s.out + out, ok: true}, 1
			}
		}
		if len(name) > 0 && (n.final || child.ok) {
			visit(name, n.final, s.out+n.finalOut, child)
		}
		for i := first; i < n.numTrans; i++ {
			in, addr, out := n.transition(i)
			walk(fstState{addr: addr, out: s.out + out, ok: true}, append(name, in))
		}
	}
	walk(s, (*buf)[:0])
}

func fstReadPacked(b []byte) uint64 {
	var v uint64
	for i, c := range b {
		v |= uint64(c) << (8 * i)
	}
	return v
}

var (
	fstCommonOnce sync.Once
	fstCommon     [fstMaxCommonInputs + 1]byte
)

// fstDecodeCommon maps vellum's one-byte encoding of frequent inputs back to
// the input. The table is vellum's own, read back once from tiny FSTs instead
// of being copied here.
func fstDecodeCommon(code byte) byte {
	fstCommonOnce.Do(func() {
		for b := 0; b < 256; b++ {
			var buf bytes.Buffer
			builder, err := vellum.New(&buf, nil)
			if err != nil || builder.Insert([]byte{byte(b)}, 0) != nil || builder.Close() != nil {
				panic("vellum: cannot build a one-key fst")
			}
			data := buf.Bytes()
			if code := data[int(binary.LittleEndian.Uint64(data[len(data)-8:]))] & fstMaxCommonInputs; code != 0 {
				fstCommon[code] = byte(b)
			}
		}
	})
	return fstCommon[code]
}
