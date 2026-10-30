package chunkstore

import (
	"encoding/binary"
	"math"
)

const decimalRelativeError = 5.1e-12
const (
	decimalConstant = iota
	decimalLinear
	decimalDelta
	decimalDelta2
)

// decimalExponent and decimalMantissa are shared by sparse and sealed blocks.
func decimalExponent(v float64) (int, bool) {
	a := math.Abs(v)
	if math.IsNaN(v) || math.IsInf(v, 0) || v == 0 && math.Signbit(v) || a != 0 && a < 0x1p-1022 {
		return 0, false
	}
	e := 0
	if a > float64(exactIntegerLimit) || math.Trunc(v) != v {
		e = int(math.Floor(math.Log10(a))) - 11
		if e < -308 || e > 308 {
			return 0, false
		}
		m := int64(math.Round(v / math.Pow10(e)))
		for m != 0 && m%10 == 0 {
			m /= 10
			e++
		}
	}
	return e, true
}
func decimalMantissa(v, scale float64) (int64, bool) {
	scaled := math.Round(v / scale)
	if !(scaled > -0x1p62 && scaled < 0x1p62) {
		return 0, false
	}
	m := int64(scaled)
	restored := float64(m) * scale
	if v != 0 && (m == 0 || math.Abs((restored-v)/v) > decimalRelativeError) {
		return 0, false
	}
	return m, true
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
	scale := math.Pow10(int(exponent))
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
		v := float64(previous) * scale
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
