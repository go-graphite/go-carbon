package cache

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"

	"github.com/go-graphite/go-carbon/points"
)

// compactPoints is an in-memory bit stream in arrival order. The first point
// is a zigzag timestamp, a scale byte and a zigzag integer or raw float bits.
// Subsequent timestamps are Gorilla delta-of-deltas (0, 10+7, 110+9, 1110+12, 1111+64 bits). Values use:
//
//	0       identical float bits
//	10      the same integer delta (zero delta-of-delta)
//	110     a new zigzag integer delta
//	1110    XOR, with a reused or new leading/trailing-zero window
//	11110   a zigzag integer delta-of-delta
//	11111   a new numeric run: two scale bits and a zigzag integer seed
//
// In a raw run, XOR uses 10 for a reused window, 110 for a new window,
// and 111 starts a numeric run.
//
// Numeric runs include integers and exact hundredths/thousandths: conversion
// back must reproduce every float bit, including the sign of zero. Other
// floats always use XOR. Arithmetic wraps identically in both directions.
// This format never leaves memory: dumps retain their existing byte codec.
//
// Only complete words are appended to buf. The unfinished word lives in tail,
// so a struct copied under the shard lock owns its last bits. Appending never
// touches any byte a previous snapshot can read, even in the race detector.
type compactPoints struct {
	buf            []byte
	n              int
	ts, delta      int64
	bits           uint64
	integerDelta   int64
	tail           uint64
	used           uint8
	scale          uint8 // 0: raw, 1: integer, 2: hundredths, 3: thousandths
	leading, width uint8 // last XOR window; width zero means no window yet
}

func zigzag(v int64) uint64   { return uint64(v<<1) ^ uint64(v>>63) }
func unzigzag(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }

// reserve grows slowly for short series, retaining the allocator's rounded
// capacity so subsequent appends can use the entire allocation.
func (c *compactPoints) reserve(extra int) {
	if cap(c.buf)-len(c.buf) >= extra {
		return
	}
	capacity := max(len(c.buf)+extra, cap(c.buf)+max(cap(c.buf)/4, 8))
	// Appending to nil exposes the runtime's size-class rounding. A make
	// with the requested capacity would hide that padding from the writer.
	b := append([]byte(nil), make([]byte, capacity)...)
	b = b[:len(c.buf)]
	copy(b, c.buf)
	c.buf = b
}

// put writes low bits first, publishing only complete, immutable words.
// Callers provide a value that fits in n bits.
func (c *compactPoints) put(v uint64, n uint8) {
	c.tail |= v << c.used
	if n < 64-c.used {
		c.used += n
		return
	}
	c.putFull(v, n)
}

// Keep word publication and allocation off the common partial-word path so
// short writes inline into the timestamp and value encoders.
func (c *compactPoints) putFull(v uint64, n uint8) {
	c.reserve(8)
	c.buf = binary.LittleEndian.AppendUint64(c.buf, c.tail)
	left := 64 - c.used
	c.used = n - left
	c.tail = 0
	if c.used != 0 {
		c.tail = v >> left
	}
}

func (c *compactPoints) putTimestamp(change int64) {
	u := zigzag(change)
	switch {
	case u == 0:
		c.put(0, 1)
	case u < 1<<7:
		c.put(u<<2|1, 9)
	case u < 1<<9:
		c.put(u<<3|3, 12)
	case u < 1<<12:
		c.put(u<<4|7, 16)
	default:
		c.put(15, 4)
		c.put(u, 64)
	}
}

func (c *compactPoints) putInteger(v int64) {
	u := zigzag(v)
	switch {
	case u < 16:
		c.put(u<<1, 5)
	case u < 256:
		c.put(u<<2|1, 10)
	case u < 65536:
		c.put(u<<3|3, 19)
	default:
		c.put(7, 3)
		c.put(u, 64)
	}
}

func integerSize(v int64) int {
	u := zigzag(v)
	switch {
	case u < 16:
		return 5
	case u < 256:
		return 10
	case u < 65536:
		return 19
	default:
		return 67
	}
}

func normalized(value float64, scale uint8) (int64, bool) {
	if scale == 0 {
		return 0, false
	}
	v := value
	if scale == 2 {
		v = math.Round(value * 100)
	} else if scale == 3 {
		v = math.Round(value * 1000)
	}
	// Conversion outside the int64 range is implementation dependent. NaNs
	// also fail this comparison, and the bit check below excludes negative zero.
	if !(v >= -0x1p63 && v < 0x1p63) {
		return 0, false
	}
	i := int64(v)
	return i, math.Float64bits(denormalized(i, scale)) == math.Float64bits(value)
}

func denormalized(v int64, scale uint8) float64 {
	switch scale {
	case 2:
		return float64(v) / 100
	case 3:
		return float64(v) / 1000
	default:
		return float64(v)
	}
}

func numericScale(value float64) (int64, uint8) {
	for scale := uint8(1); scale <= 3; scale++ {
		if v, ok := normalized(value, scale); ok {
			return v, scale
		}
	}
	return 0, 0
}

func (c *compactPoints) append(value float64, timestamp int64) {
	v := math.Float64bits(value)
	if c.n == 0 {
		var header [2*binary.MaxVarintLen64 + 1]byte
		k := binary.PutUvarint(header[:], zigzag(timestamp))
		integer, scale := numericScale(value)
		header[k] = scale
		k++
		if scale == 0 {
			binary.LittleEndian.PutUint64(header[k:], v)
			k += 8
		} else {
			k += binary.PutUvarint(header[k:], zigzag(integer))
		}
		c.reserve(k)
		c.buf = append(c.buf, header[:k]...)
		c.scale = scale
		c.n, c.ts, c.bits = 1, timestamp, v
		return
	}

	delta := timestamp - c.ts
	x := v ^ c.bits
	if delta == c.delta && x == 0 {
		c.put(0, 2)
		c.n, c.ts, c.integerDelta = c.n+1, timestamp, 0
		return
	}
	previous := math.Float64frombits(c.bits)
	if x == 0 {
		c.putTimestamp(delta - c.delta)
		c.put(0, 1)
		c.integerDelta = 0
	} else {
		integer, ok := normalized(value, c.scale)
		// A numeric run ends on every XOR escape, so its previous value is
		// already known to be exactly representable at this scale.
		prev := int64(0)
		if c.scale != 0 {
			prev = int64(previous)
		}
		if c.scale == 2 {
			prev = int64(math.Round(previous * 100))
		}
		if c.scale == 3 {
			prev = int64(math.Round(previous * 1000))
		}
		d := integer - prev
		if ok && d == c.integerDelta {
			if delta == c.delta {
				c.put(2, 3)
			} else {
				c.putTimestamp(delta - c.delta)
				c.put(1, 2)
			}
			c.n, c.ts, c.delta, c.bits = c.n+1, timestamp, delta, v
			return
		}
		if delta == c.delta {
			c.put(0, 1)
		} else {
			c.putTimestamp(delta - c.delta)
		}
		dd := d - c.integerDelta
		leading, trailing := uint8(bits.LeadingZeros64(x)), uint8(bits.TrailingZeros64(x))
		width := 64 - leading - trailing
		reuse := c.width != 0 && leading >= c.leading && trailing >= 64-c.leading-c.width && int(c.width) <= int(width)+12
		xorSize := 5 + 12 + int(width)
		if reuse {
			xorSize = 5 + int(c.width)
		}
		if c.scale == 0 {
			xorSize = 3 + 12 + int(width)
			if reuse {
				xorSize = 2 + int(c.width)
			}
		}
		switch {
		case ok && 5+integerSize(dd) < 3+integerSize(d) && 5+integerSize(dd) < xorSize:
			c.put(15, 5)
			c.putInteger(dd)
			c.integerDelta = d
		case ok && 3+integerSize(d) < xorSize:
			c.put(3, 3)
			c.putInteger(d)
			c.integerDelta = d
		default:
			// Numeric runs can promote to exact decimals. Raw runs probe
			// decimals occasionally, so one unusual value does not prevent
			// a rounded gauge from returning to numeric coding.
			seed, scale := int64(0), uint8(0)
			if c.scale != 0 || c.n&15 == 0 {
				seed, scale = numericScale(value)
			} else if i, yes := normalized(value, 1); yes {
				seed, scale = i, 1
			}
			seedControl := 7
			if c.scale == 0 {
				seedControl = 5
			}
			if scale != 0 && scale != c.scale && seedControl+integerSize(seed) < xorSize {
				if c.scale == 0 {
					c.put(7, 3)
				} else {
					c.put(31, 5)
				}
				c.put(uint64(scale), 2)
				c.putInteger(seed)
				c.scale, c.integerDelta = scale, 0
			} else {
				if c.scale != 0 {
					if reuse {
						c.put(7, 5)
					} else {
						c.put(23, 5)
					}
				} else {
					if reuse {
						c.put(1, 2)
					} else {
						c.put(3, 3)
					}
				}
				if !reuse {
					c.put(uint64(leading), 6)
					c.put(uint64(width-1), 6)
					c.leading, c.width = leading, width
				}
				c.put(x>>(64-c.leading-c.width), c.width)
				c.scale, c.integerDelta = 0, 0
			}
		}
	}
	c.n, c.ts, c.delta, c.bits = c.n+1, timestamp, delta, v
}

func (c *compactPoints) appendPoints(data []points.Point) {
	for _, p := range data {
		c.append(p.Value, p.Timestamp)
	}
}

// encodedBytes includes the partial word; the tail's storage is in the struct.
func (c *compactPoints) encodedBytes() int { return len(c.buf) + (int(c.used)+7)/8 }

// bitReader never reads past the immutable buf prefix into spare capacity.
type bitReader struct {
	buf        []byte
	tail, word uint64
	pos        int
	left       uint8
}

// Most fields are one-bit controls. Keep their common path small enough to
// inline; generic reads also handle fields that cross a word boundary.
func (r *bitReader) bit() uint64 {
	if r.left == 0 {
		r.refill()
	}
	v := r.word & 1
	r.word >>= 1
	r.left--
	return v
}

func (r *bitReader) refill() {
	if r.pos < len(r.buf) {
		r.word = binary.LittleEndian.Uint64(r.buf[r.pos:])
		r.pos += 8
	} else {
		r.word = r.tail
	}
	r.left = 64
}

func (r *bitReader) get(n uint8) uint64 {
	if n <= r.left {
		v := r.word & (^uint64(0) >> (64 - n))
		r.word >>= n
		r.left -= n
		return v
	}
	v := r.word
	old := r.left
	r.refill()
	n -= old
	v |= (r.word & (^uint64(0) >> (64 - n))) << old
	r.word >>= n
	r.left = 64 - n
	return v
}
func (r *bitReader) timestamp() int64 {
	if r.bit() == 0 {
		return 0
	}
	return r.timestampNonzero()
}

func (r *bitReader) timestampNonzero() int64 {
	if r.bit() == 0 {
		return unzigzag(r.get(7))
	}
	if r.bit() == 0 {
		return unzigzag(r.get(9))
	}
	if r.bit() == 0 {
		return unzigzag(r.get(12))
	}
	return unzigzag(r.get(64))
}
func (r *bitReader) integer() int64 {
	if r.bit() == 0 {
		return unzigzag(r.get(4))
	}
	if r.bit() == 0 {
		return unzigzag(r.get(8))
	}
	if r.bit() == 0 {
		return unzigzag(r.get(16))
	}
	return unzigzag(r.get(64))
}

func (c *compactPoints) appendDecoded(dst []points.Point) []points.Point {
	if c.n == 0 {
		return dst
	}
	dst = slices.Grow(dst, c.n)
	out := dst[len(dst) : len(dst)+c.n]
	u, k := binary.Uvarint(c.buf)
	ts := unzigzag(u)
	scale := c.buf[k]
	k++
	var v uint64
	var integer int64
	if scale == 0 {
		v = binary.LittleEndian.Uint64(c.buf[k:])
		k += 8
	} else {
		u, j := binary.Uvarint(c.buf[k:])
		k += j
		integer = unzigzag(u)
		v = math.Float64bits(denormalized(integer, scale))
	}
	out[0] = points.Point{Value: math.Float64frombits(v), Timestamp: ts}
	var delta, integerDelta int64
	var leading, width uint8
	r := bitReader{buf: c.buf[k:], tail: c.tail}
	for i := 1; i < c.n; i++ {
		// Regular timestamps plus repeated values/numeric deltas dominate
		// ordinary series. Consume their complete controls in one step.
		if r.left >= 3 {
			if r.word&3 == 0 {
				r.word >>= 2
				r.left -= 2
				ts += delta
				integerDelta = 0
				out[i] = points.Point{Value: math.Float64frombits(v), Timestamp: ts}
				continue
			}
			if scale != 0 && r.word&7 == 2 {
				r.word >>= 3
				r.left -= 3
				ts += delta
				integer += integerDelta
				v = math.Float64bits(denormalized(integer, scale))
				out[i] = points.Point{Value: math.Float64frombits(v), Timestamp: ts}
				continue
			}
		}
		// Numeric deltas in 4 or 8 bits use a single bounded word read.
		// Larger fields and controls crossing a word use the general decoder.
		if scale != 0 && r.left >= 14 && r.word&15 == 6 {
			var used uint8
			var d int64
			if r.word&16 == 0 {
				d, used = unzigzag(r.word>>5&15), 9
			} else if r.word&32 == 0 {
				d, used = unzigzag(r.word>>6&255), 14
			}
			if used != 0 {
				r.word >>= used
				r.left -= used
				ts += delta
				integerDelta = d
				integer += d
				v = math.Float64bits(denormalized(integer, scale))
				out[i] = points.Point{Value: math.Float64frombits(v), Timestamp: ts}
				continue
			}
		}
		if r.bit() != 0 {
			delta += r.timestampNonzero()
		}
		ts += delta
		// Each r.bit() consumes the next prefix bit, so this chain cannot be a switch.
		if r.bit() == 0 { //nolint:gocritic // ifElseChain: conditions read the stream in order
			integerDelta = 0
		} else if scale == 0 {
			if r.bit() != 0 {
				if r.bit() != 0 {
					scale = uint8(r.get(2))
					integer = r.integer()
					v = math.Float64bits(denormalized(integer, scale))
					integerDelta = 0
					out[i] = points.Point{Value: math.Float64frombits(v), Timestamp: ts}
					continue
				}
				leading = uint8(r.get(6))
				width = uint8(r.get(6)) + 1
			}
			v ^= r.get(width) << (64 - leading - width)
			integerDelta = 0
		} else if r.bit() == 0 {
			integer += integerDelta
			v = math.Float64bits(denormalized(integer, scale))
		} else if r.bit() == 0 {
			integerDelta = r.integer()
			integer += integerDelta
			v = math.Float64bits(denormalized(integer, scale))
		} else if r.bit() == 0 {
			if r.bit() != 0 {
				leading = uint8(r.get(6))
				width = uint8(r.get(6)) + 1
			}
			v ^= r.get(width) << (64 - leading - width)
			scale, integerDelta = 0, 0
		} else if r.bit() == 0 {
			integerDelta += r.integer()
			integer += integerDelta
			v = math.Float64bits(denormalized(integer, scale))
		} else {
			scale = uint8(r.get(2))
			integer = r.integer()
			v = math.Float64bits(denormalized(integer, scale))
			integerDelta = 0
		}
		out[i] = points.Point{Value: math.Float64frombits(v), Timestamp: ts}
	}
	return dst[:len(dst)+c.n]
}

func (c *compactPoints) firstTimestamp() int64 {
	u, _ := binary.Uvarint(c.buf)
	return unzigzag(u)
}
