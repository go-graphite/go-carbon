package pickle

import (
	"encoding/binary"
	"io"
	"math"
)

var EmptyList = []byte{0x28, 0x6c, 0x70, 0x30, 0xa, 0x2e}

// Pickle encoder
type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (p *Writer) Mark() {
	p.w.Write([]byte{'('})
}

func (p *Writer) Stop() {
	p.w.Write([]byte{'.'})
}

func (p *Writer) Append() {
	p.w.Write([]byte{'a'})
}

func (p *Writer) SetItem() {
	p.w.Write([]byte{'s'})
}

func (p *Writer) List() {
	p.w.Write([]byte{'(', 'l'})
}

func (p *Writer) Dict() {
	p.w.Write([]byte{'(', 'd'})
}

func (p *Writer) TupleEnd() {
	p.w.Write([]byte{'t'})
}

func (p *Writer) Bytes(byt []byte) {
	l := len(byt)

	if l < 256 {
		p.w.Write([]byte{'U', byte(l)})
	} else {
		var b [5]byte
		b[0] = 'T'
		binary.LittleEndian.PutUint32(b[1:5], uint32(l))
		p.w.Write(b[:])
	}

	p.w.Write(byt)
}

func (p *Writer) String(v string) {
	p.Bytes([]byte(v))
}

func (p *Writer) Uint32(v uint32) {
	p.w.Write([]byte{'J'})
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(v))
	p.w.Write(b[:])
}

func (p *Writer) AppendFloat64(v float64) {
	u := math.Float64bits(v)

	var b [10]byte
	b[0] = 'G'
	b[9] = 'a'

	binary.BigEndian.PutUint64(b[1:10], uint64(u))

	p.w.Write(b[:])
}

func (p *Writer) AppendNulls(count int) {
	for i := 0; i < count; i++ {
		p.w.Write([]byte{'N', 'a'})
	}
}

func (p *Writer) Bool(b bool) {
	if b {
		p.w.Write([]byte{'I', '0', '1', '\n'})
	} else {
		p.w.Write([]byte{'I', '0', '0', '\n'})
	}
}
