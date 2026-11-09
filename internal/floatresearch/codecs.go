package floatresearch

// Research-only encoders. No durable encoding tags or production paths change.
// Chimp128 follows Liakos et al., PVLDB 2022, section 4 and the authors'
// ChimpN.java. It uses an explicit count rather than their NaN terminator,
// preserving every NaN payload. The bit order and framing are private here.

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/go-graphite/go-carbon/points"
)

type Point = points.Point

type frWriter struct {
	data   []byte
	offset uint
}

func (w *frWriter) put(v uint64, n uint) {
	for n > 0 {
		if w.offset == 0 {
			w.data = append(w.data, 0)
		}
		k := min(n, 8-w.offset)
		w.data[len(w.data)-1] |= byte(v&((1<<k)-1)) << w.offset
		v >>= k
		n -= k
		w.offset = (w.offset + k) % 8
	}
}

type frReader struct {
	data []byte
	bit  uint
}

func (r *frReader) get(n uint) uint64 {
	if n > 64 || r.bit+n > uint(len(r.data))*8 {
		panic("invalid research bitstream")
	}
	var v uint64
	for shift := uint(0); n > 0; {
		k := min(n, 8-r.bit%8)
		v |= uint64(r.data[r.bit/8]>>(r.bit%8)&byte((1<<k)-1)) << shift
		r.bit += k
		n -= k
		shift += k
	}
	return v
}
func frUnpack(decode func() []float64) (out []float64, err error) {
	defer func() {
		if v := recover(); v != nil {
			out = nil
			err = fmt.Errorf("research decode: %v", v)
		}
	}()
	return decode(), nil
}

func frGorilla(p []Point) []byte {
	w := frWriter{data: make([]byte, 0, len(p)*8)}
	prev := math.Float64bits(p[0].Value)
	w.put(prev, 64)
	l, t := uint(65), uint(0)
	for _, p := range p[1:] {
		v := math.Float64bits(p.Value)
		x := prev ^ v
		prev = v
		if x == 0 {
			w.put(0, 1)
			continue
		}
		lead, trail := min(uint(bits.LeadingZeros64(x)), 31), uint(bits.TrailingZeros64(x))
		if lead >= l && trail >= t {
			w.put(1, 2)
			w.put(x>>t, 64-l-t)
		} else {
			l, t = lead, trail
			s := 64 - l - t
			w.put(3, 2)
			w.put(uint64(l), 5)
			w.put(uint64(s-1), 6)
			w.put(x>>t, s)
		}
	}
	return w.data
}
func frGorillaDecode(data []byte, n int) ([]float64, error) {
	return frUnpack(func() []float64 {
		r := frReader{data: data}
		out := make([]float64, n)
		v := r.get(64)
		out[0] = math.Float64frombits(v)
		l, t := uint(65), uint(0)
		for i := 1; i < n; i++ {
			if r.get(1) != 0 {
				if r.get(1) != 0 {
					l = uint(r.get(5))
					s := uint(r.get(6)) + 1
					if l+s > 64 {
						panic("window")
					}
					t = 64 - l - s
				}
				if l > 64 {
					panic("uninitialized window")
				}
				v ^= r.get(64-l-t) << t
			}
			out[i] = math.Float64frombits(v)
		}
		return out
	})
}

var frLeads = [...]uint{0, 8, 12, 16, 18, 20, 22, 24}

func frChimp(p []Point) []byte {
	w := frWriter{data: make([]byte, 0, len(p)*8)}
	var ring [128]uint64
	var indices [16384]uint16 // one group is <=8192, absolute indices fit
	prev := math.Float64bits(p[0].Value)
	ring[0] = prev
	indices[prev&16383] = 0
	w.put(prev, 64)
	stored := uint(65)
	for i := 1; i < len(p); i++ {
		v := math.Float64bits(p[i].Value)
		at := int(indices[v&16383])
		ref := (i - 1) & 127
		x := prev ^ v
		trail := uint(0)
		if i-1-at < 128 {
			candidate := v ^ ring[at&127]
			tz := uint(bits.TrailingZeros64(candidate))
			if tz > 13 {
				x = candidate
				ref = at & 127
				trail = tz
			}
		}
		if x == 0 {
			w.put(0, 2)
			w.put(uint64(ref), 7)
			stored = 65
		} else {
			lead := uint(bits.LeadingZeros64(x))
			code := 0
			for code < 7 && frLeads[code+1] <= lead {
				code++
			}
			lead = frLeads[code]
			switch {
			case trail > 13:
				s := 64 - lead - trail
				w.put(1, 2)
				w.put(uint64(ref), 7)
				w.put(uint64(code), 3)
				w.put(uint64(s), 6)
				w.put(x>>trail, s)
				stored = 65
			case lead == stored:
				w.put(2, 2)
				w.put(x, 64-lead)
			default:
				w.put(3, 2)
				w.put(uint64(code), 3)
				w.put(x, 64-lead)
				stored = lead
			}
		}
		ring[i&127] = v
		indices[v&16383] = uint16(i)
		prev = v
	}
	return w.data
}
func frChimpDecode(data []byte, n int) ([]float64, error) {
	return frUnpack(func() []float64 {
		r := frReader{data: data}
		out := make([]float64, n)
		var ring [128]uint64
		v := r.get(64)
		ring[0] = v
		out[0] = math.Float64frombits(v)
		lead := uint(65)
		for i := 1; i < n; i++ {
			switch r.get(2) {
			case 0:
				at := int(r.get(7))
				if i < 128 && at >= i {
					panic("reference")
				}
				v = ring[at]
				lead = 65
			case 1:
				at := int(r.get(7))
				if i < 128 && at >= i {
					panic("reference")
				}
				l := frLeads[r.get(3)]
				s := uint(r.get(6))
				if s == 0 {
					s = 64
				}
				if l+s > 64 {
					panic("window")
				}
				v = ring[at] ^ (r.get(s) << (64 - l - s))
				lead = 65
			case 2:
				if lead > 64 {
					panic("window")
				}
				v ^= r.get(64 - lead)
			case 3:
				lead = frLeads[r.get(3)]
				v ^= r.get(64 - lead)
			}
			ring[i&127] = v
			out[i] = math.Float64frombits(v)
		}
		return out
	})
}

// Raw bitwise XOR residuals, transposed to eight byte planes before Zstd.
// Subtraction/addition is deliberately not used on floating-point values.
func frPlanes(p []Point, residual bool) []byte {
	out := make([]byte, len(p)*8)
	var prev uint64
	for i, point := range p {
		v := math.Float64bits(point.Value)
		x := v
		if residual {
			x ^= prev
		}
		prev = v
		for plane := 0; plane < 8; plane++ {
			out[plane*len(p)+i] = byte(x >> uint(plane*8))
		}
	}
	return out
}
func frPlanesDecode(data []byte, n int, residual bool) ([]float64, error) {
	if len(data) != n*8 {
		return nil, fmt.Errorf("planes length")
	}
	out := make([]float64, n)
	var prev uint64
	for i := range out {
		var v uint64
		for plane := 0; plane < 8; plane++ {
			v |= uint64(data[plane*n+i]) << uint(plane*8)
		}
		if residual {
			v ^= prev
		}
		prev = v
		out[i] = math.Float64frombits(v)
	}
	return out, nil
}

// Whole-block raw-bit dictionary, with fixed-width packed IDs. The dictionary
// includes all distinct values, so no exceptions or approximate equality occur.
func frDictionary(p []Point) []byte {
	ids := make([]uint64, len(p))
	dict := make(map[uint64]uint64)
	values := make([]uint64, 0)
	for i, p := range p {
		v := math.Float64bits(p.Value)
		id, ok := dict[v]
		if !ok {
			id = uint64(len(values))
			dict[v] = id
			values = append(values, v)
		}
		ids[i] = id
	}
	out := binary.AppendUvarint(nil, uint64(len(values)))
	for _, v := range values {
		out = binary.LittleEndian.AppendUint64(out, v)
	}
	w := frWriter{data: out}
	width := uint(bits.Len(uint(len(values) - 1)))
	for _, id := range ids {
		w.put(id, width)
	}
	return w.data
}
func frDictionaryDecode(data []byte, n int) ([]float64, error) {
	return frUnpack(func() []float64 {
		count, k := binary.Uvarint(data)
		if k <= 0 || count < 1 || count > uint64(n) || count > uint64((len(data)-k)/8) {
			panic("dictionary")
		}
		dict := make([]uint64, count)
		for i := range dict {
			dict[i] = binary.LittleEndian.Uint64(data[k+i*8:])
		}
		r := frReader{data: data[k+len(dict)*8:]}
		width := uint(bits.Len(uint(count - 1)))
		out := make([]float64, n)
		for i := range out {
			id := r.get(width)
			if id >= count {
				panic("id")
			}
			out[i] = math.Float64frombits(dict[id])
		}
		return out
	})
}
