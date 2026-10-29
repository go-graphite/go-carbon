package cache

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"

	"github.com/go-graphite/go-carbon/points"
)

// compactPoints holds points in arrival order in a few bytes each. Graphite
// series mostly arrive at a regular step, and their values often repeat or
// change only some bytes, so each point is encoded against the previous one:
//
//   - A header byte. Bit 7 is set when the timestamp delta differs from the
//     previous delta; bits 0-6 are the value code.
//   - If bit 7 is set, the change of delta as a zigzag uvarint.
//   - Value code 0 repeats the previous value's bits. Otherwise the value's
//     bits XOR the previous ones have tz trailing zero bytes and n
//     significant bytes, code-1 is tz<<3|(n-1), and the n bytes follow,
//     least significant first.
//
// The first point is encoded against timestamp, delta and value bits zero,
// and leaves delta zero: its timestamp is not a step. Timestamp arithmetic
// wraps identically in the encoder and decoder, so any sequence decodes to
// exactly the points and float bits appended.
//
// Bytes once appended are never modified, so a reader may copy the struct
// under the shard lock and decode the copy after releasing it.
type compactPoints struct {
	buf   []byte
	n     int    // points in buf
	ts    int64  // last timestamp
	delta int64  // last timestamp delta
	bits  uint64 // last value's bits
}

const (
	deltaChanged = 0x80
	// maxPointBytes is a header, a 64-bit uvarint and 8 value bytes.
	maxPointBytes = 1 + binary.MaxVarintLen64 + 8
)

func (c *compactPoints) append(value float64, timestamp int64) {
	delta := timestamp - c.ts
	change := delta - c.delta
	if c.n == 0 {
		delta = 0
	}
	v := math.Float64bits(value)
	x := v ^ c.bits
	c.n, c.ts, c.delta, c.bits = c.n+1, timestamp, delta, v

	var header byte
	var tz, n int
	if x != 0 {
		tz = bits.TrailingZeros64(x) >> 3
		n = 8 - tz - bits.LeadingZeros64(x)>>3
		header = byte(tz<<3|(n-1)) + 1
	}
	if change != 0 {
		header |= deltaChanged
	}
	l := len(c.buf)
	if cap(c.buf)-l < maxPointBytes {
		c.appendSlow(header, change, x>>(8*tz), n)
		return
	}
	// Bytes written past the point are spare capacity that no reader decodes.
	b := c.buf[l : l+maxPointBytes]
	b[0] = header
	k := 1
	if change != 0 {
		k += binary.PutUvarint(b[1:], uint64(change<<1)^uint64(change>>63))
	}
	binary.LittleEndian.PutUint64(b[k:], x>>(8*tz))
	c.buf = c.buf[:l+k+n]
}

// appendSlow appends a point near the end of buf's capacity without
// reserving more than it needs. n is zero for a repeated value.
func (c *compactPoints) appendSlow(header byte, change int64, low uint64, n int) {
	c.buf = append(c.buf, header)
	if change != 0 {
		c.buf = binary.AppendUvarint(c.buf, uint64(change<<1)^uint64(change>>63))
	}
	for ; n > 0; n-- {
		c.buf = append(c.buf, byte(low))
		low >>= 8
	}
}

func (c *compactPoints) appendPoints(data []points.Point) {
	for _, p := range data {
		c.append(p.Value, p.Timestamp)
	}
}

// readLow reads the n value bytes of a point.
func readLow(buf []byte, n int) uint64 {
	if len(buf) >= 8 {
		return binary.LittleEndian.Uint64(buf) & (^uint64(0) >> (64 - 8*n))
	}
	var v uint64
	for i := n - 1; i >= 0; i-- {
		v = v<<8 | uint64(buf[i])
	}
	return v
}

// readChange reads the zigzag uvarint following a header with deltaChanged.
func readChange(buf []byte) (int64, int) {
	u, k := uint64(buf[0]), 1
	if u >= 0x80 {
		u, k = binary.Uvarint(buf)
	}
	return int64(u>>1) ^ -int64(u&1), k
}

// appendDecoded appends the n points encoded in buf to dst.
func appendDecoded(dst []points.Point, buf []byte, n int) []points.Point {
	dst = slices.Grow(dst, n)
	out := dst[len(dst) : len(dst)+n]
	var ts, delta int64
	var v uint64
	pos := 0
	for i := range out {
		header := buf[pos]
		pos++
		if header&deltaChanged != 0 {
			change, k := readChange(buf[pos:])
			delta += change
			pos += k
		}
		ts += delta
		if code := int(header&^deltaChanged) - 1; code >= 0 {
			tz, size := code>>3, code&7+1
			v ^= readLow(buf[pos:], size) << (8 * tz)
			pos += size
		}
		out[i] = points.Point{Value: math.Float64frombits(v), Timestamp: ts}
		if i == 0 {
			delta = 0
		}
	}
	return dst[:len(dst)+n]
}

// firstTimestamp returns the timestamp of the first point in a nonempty buf.
func firstTimestamp(buf []byte) int64 {
	if buf[0]&deltaChanged == 0 {
		return 0
	}
	ts, _ := readChange(buf[1:])
	return ts
}
