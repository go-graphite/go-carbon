package partlog

import (
	"errors"
	"hash/crc32"
)

// Predictor primitives shared by the catalog-backed PDC4 codec: timestamp
// deltas, Gorilla value windows and their bit writer/reader.
const predictorV3MaxRaw = 64 << 20

var predictorV3CRC = crc32.MakeTable(crc32.Castagnoli)
var errPredictorV3 = errors.New("invalid PDC3 journal block")

type predictorV3State struct {
	timestamp, delta  int64
	clock             int64
	value             uint64
	leading, trailing uint8
	valid, window     bool
}

type predictorV3Entry struct {
	key        string
	definition []byte
	predictorV3State
}

type predictorV3Writer struct {
	b []byte
	n uint64
}

func (w *predictorV3Writer) put(v uint64, n int) {
	for n > 0 {
		if w.n&7 == 0 {
			w.b = append(w.b, 0)
		}
		take := min(n, 8-int(w.n&7))
		shift := n - take
		w.b[len(w.b)-1] |= byte(v>>shift&((1<<take)-1)) << (8 - int(w.n&7) - take)
		w.n += uint64(take)
		n -= take
	}
}

type predictorV3Reader struct {
	b      []byte
	n, off uint64
	err    bool
}

func (r *predictorV3Reader) get(n int) uint64 {
	if n < 0 || n > 64 || uint64(n) > r.n-r.off {
		r.err = true
		return 0
	}
	var v uint64
	for n > 0 {
		take := min(n, 8-int(r.off&7))
		v = v<<take | uint64(r.b[r.off/8]>>(8-int(r.off&7)-take))&((1<<take)-1)
		r.off += uint64(take)
		n -= take
	}
	return v
}
