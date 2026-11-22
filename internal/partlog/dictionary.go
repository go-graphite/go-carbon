package partlog

import (
	"encoding/binary"
	"errors"
)

var errDictionary = errors.New("invalid journal dictionary record")

type dictionaryReader struct {
	b   []byte
	err error
}

func (r *dictionaryReader) u() uint64 {
	n, k := binary.Uvarint(r.b)
	if k <= 0 {
		r.err = errDictionary
		return 0
	}
	r.b = r.b[k:]
	return n
}
func (r *dictionaryReader) i() int64 {
	n, k := binary.Varint(r.b)
	if k <= 0 {
		r.err = errDictionary
		return 0
	}
	r.b = r.b[k:]
	return n
}
func (r *dictionaryReader) bytes() []byte {
	n := r.u()
	if r.err != nil || n > uint64(len(r.b)) {
		r.err = errDictionary
		return nil
	}
	b := r.b[:int(n)]
	r.b = r.b[int(n):]
	return b
}
