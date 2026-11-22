package partlog

import (
	"encoding/binary"
	"errors"

	"github.com/go-graphite/go-carbon/points"
)

// DictionaryFrame preserves one original apply boundary. Definition is opaque
// policy/placement metadata. Key and Definition must remain stable in an epoch.
// ID is the catalog ID CatalogDecoder.Decode resolved; encoders ignore it.
type DictionaryFrame struct {
	ID         uint64
	Key        string
	Definition []byte
	Clock      int64
	Points     []points.Point
}

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
