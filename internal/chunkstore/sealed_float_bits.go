package chunkstore

import (
	"encoding/binary"
	"math"
	"math/bits"
)

func decodeSealedDictionary(body []byte, points []Point) error {
	count, k := binary.Uvarint(body)
	if k <= 0 || count == 0 || count > uint64(len(points)) || count > uint64((len(body)-k)/8) {
		return errInvalidChunk
	}
	dict := body[k : k+int(count)*8]
	body = body[k+len(dict):]
	width := uint(bits.Len64(count - 1))
	if len(body) != (len(points)*int(width)+7)/8 {
		return errInvalidChunk
	}
	r := sealedBitReader{data: body}
	for i := range points {
		id := r.get(width)
		if id >= count {
			return errInvalidChunk
		}
		points[i].Value = math.Float64frombits(binary.LittleEndian.Uint64(dict[id*8:]))
	}
	if !r.done() {
		return errInvalidChunk
	}
	return nil
}

// Little-endian streams use word operations and a bounded copy at the tail.
// Failed reads never panic, allocate or read beyond the frame.
type sealedBitReader struct {
	data   []byte
	bit    uint
	failed bool
}

func (r *sealedBitReader) get(n uint) uint64 {
	if n > 64 || r.bit+n > uint(len(r.data))*8 {
		r.failed = true
		return 0
	}
	if n == 0 {
		return 0
	}
	at, shift := r.bit/8, r.bit%8
	var word uint64
	if uint(len(r.data))-at >= 8 {
		word = binary.LittleEndian.Uint64(r.data[at:])
	} else {
		var tail [8]byte
		copy(tail[:], r.data[at:])
		word = binary.LittleEndian.Uint64(tail[:])
	}
	v := word >> shift
	if n > 64-shift {
		v |= uint64(r.data[at+8]) << (64 - shift)
	}
	r.bit += n
	if n < 64 {
		v &= (uint64(1) << n) - 1
	}
	return v
}
func (r *sealedBitReader) done() bool {
	if r.failed || uint(len(r.data)) != (r.bit+7)/8 {
		return false
	}
	return r.bit%8 == 0 || r.data[len(r.data)-1]>>uint(r.bit%8) == 0
}

type sealedBitWriter struct {
	data []byte
	word uint64
	n    uint
}

func (w *sealedBitWriter) put(v uint64, n uint) {
	if n == 0 {
		return
	}
	w.word |= v << w.n
	if n < 64-w.n {
		w.n += n
		return
	}
	w.data = binary.LittleEndian.AppendUint64(w.data, w.word)
	used := 64 - w.n
	w.word = 0
	if used < 64 {
		w.word = v >> used
	}
	w.n = n - used
}
func (w *sealedBitWriter) finish() []byte {
	for w.n > 0 {
		w.data = append(w.data, byte(w.word))
		w.word >>= 8
		if w.n <= 8 {
			w.n = 0
		} else {
			w.n -= 8
		}
	}
	return w.data
}
