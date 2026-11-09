// Private sealed framing and decimal integer primitives retained from the
// fa56f19d ALP reference. These offline helpers do not install durable codecs.
package floatresearch

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const chunkSlots = 128
const exactIntegerLimit = int64(1 << 53)
const (
	decimalConstant = iota
	decimalLinear
	decimalDelta
	decimalDelta2
)

var errInvalidChunk = errors.New("invalid research frame")

const (
	MaxSealedPoints = 8192
	sealedHeader    = 16 // version, flags, count, first timestamp, step, CRC
	sealedInteger   = 1
	sealedDoD       = 2
	sealedImplicit  = 4
	sealedZstd      = 8
	sealedDecimal   = 16
	maxSealedBody   = MaxSealedPoints * 20
)

var sealedEncoders = sync.Pool{New: func() any {
	z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(128<<10))
	if err != nil {
		panic(err)
	}
	return z
}}
var sealedDecoders = sync.Pool{New: func() any {
	z, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(1<<20), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		panic(err)
	}
	return z
}}

func encodeSealedBody(count int, first int64, step int, flags byte, body []byte) []byte {

	out := make([]byte, sealedHeader, sealedHeader+len(body))
	out[0], out[1] = 3, flags
	binary.LittleEndian.PutUint16(out[2:4], uint16(count))
	binary.LittleEndian.PutUint32(out[4:8], uint32(first))
	binary.LittleEndian.PutUint32(out[8:12], uint32(step))
	checksum := crc32.Update(0, crc32.IEEETable, out[:12])
	binary.LittleEndian.PutUint32(out[12:16], crc32.Update(checksum, crc32.IEEETable, body))
	if len(body) >= 128 {
		z := sealedEncoders.Get().(*zstd.Encoder)
		compressed := z.EncodeAll(body, nil)
		sealedEncoders.Put(z)
		if len(compressed) < len(body) {
			body = compressed
			out[1] |= sealedZstd
		}
	}
	return append(out, body...)
}

func appendDecimalIntegers(dst []byte, values []int64, exponent int) []byte {
	dst = binary.AppendVarint(dst, int64(exponent))
	var smallDelta, smallDelta2 [chunkSlots * binary.MaxVarintLen64]byte
	delta, delta2 := smallDelta[:0], smallDelta2[:0]
	if len(values) > chunkSlots {
		delta, delta2 = make([]byte, 0, len(values)*2), make([]byte, 0, len(values)*2)
	}
	var previous, previousDelta, first, step int64
	count, constant, linear := 0, true, true
	for _, v := range values {
		d := v - previous // Reversible modulo-2^64, including delta-of-delta.
		delta = binary.AppendVarint(delta, d)
		if count == 0 {
			first = v
			delta2 = binary.AppendVarint(delta2, v)
		} else {
			constant = constant && v == first
			if count == 1 {
				step = d
			} else {
				linear = linear && d == step
			}
			delta2 = binary.AppendVarint(delta2, d-previousDelta)
		}
		previous, previousDelta = v, d
		count++
	}
	if constant {
		dst = append(dst, decimalConstant)
		return binary.AppendVarint(dst, first)
	}
	if linear {
		dst = append(dst, decimalLinear)
		dst = binary.AppendVarint(dst, first)
		return binary.AppendVarint(dst, step)
	}
	if len(delta2) < len(delta) {
		dst = append(dst, decimalDelta2)
		return append(dst, delta2...)
	}
	dst = append(dst, decimalDelta)
	return append(dst, delta...)
}

func decodeDecimalPoints(payload []byte, points []Point) error {
	offset := 0
	exponent, n := binary.Varint(payload[offset:])
	if n <= 0 || exponent < -308 || exponent > 308 {
		return errInvalidChunk
	}
	offset += n
	if offset >= len(payload) {
		return errInvalidChunk
	}
	mode := payload[offset]
	offset++
	if mode < decimalConstant || mode > decimalDelta2 {
		return errInvalidChunk
	}
	var previous, previousDelta, step int64
	if mode == decimalConstant || mode == decimalLinear {
		previous, n = binary.Varint(payload[offset:])
		if n <= 0 {
			return errInvalidChunk
		}
		offset += n
		if mode == decimalLinear {
			step, n = binary.Varint(payload[offset:])
			if n <= 0 {
				return errInvalidChunk
			}
			offset += n
		}
	}
	scale, divide := decimalScale(int(exponent))
	count := 0
	for slot := range points {
		switch mode {
		case decimalLinear:
			if count > 0 {
				previous += step
			}
		case decimalDelta, decimalDelta2:
			d, n := binary.Varint(payload[offset:])
			if n <= 0 {
				return errInvalidChunk
			}
			offset += n
			if mode == decimalDelta2 && count > 0 {
				d += previousDelta
			}
			previous += d
			previousDelta = d
		}
		if previous <= -1<<62 || previous >= 1<<62 {
			return errInvalidChunk
		}
		v := decimalValue(previous, scale, divide)
		if math.IsInf(v, 0) || previous != 0 && (v == 0 || math.Abs(v) < 0x1p-1022) {
			return errInvalidChunk
		}
		points[slot].Value = v
		count++
	}
	if offset != len(payload) {
		return errInvalidChunk
	}
	return nil
}
func decimalScale(exponent int) (float64, bool) {
	if exponent < 0 && exponent >= -22 {
		return math.Pow10(-exponent), true
	}
	return math.Pow10(exponent), false
}
func decimalValue(m int64, scale float64, divide bool) float64 {
	if divide {
		return float64(m) / scale
	}
	return float64(m) * scale
}
