package chunkstore

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/bits"
	"sync"

	"github.com/klauspost/compress/zstd"
)

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

// EncodeSealedPoints encodes an ordered, dense ordinal block. Missing archive
// intervals use unsigned second deltas; timestamps need no presence bitmap. Values stay
// exact so layout experiments do not include a change in numeric precision.
func EncodeSealedPoints(points []Point, step int) ([]byte, error) {
	if len(points) == 0 || len(points) > MaxSealedPoints || step <= 0 || uint64(step) > math.MaxUint32 {
		return nil, errInvalidChunk
	}
	flags := byte(sealedImplicit)
	integer, linear := true, true
	var valueStep int64
	for i, p := range points {
		if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || i > 0 && p.Timestamp <= points[i-1].Timestamp {
			return nil, errInvalidChunk
		}
		if i > 0 && p.Timestamp-points[i-1].Timestamp != int64(step) {
			flags &^= sealedImplicit
		}
		if !(p.Value >= -float64(exactIntegerLimit) && p.Value <= float64(exactIntegerLimit)) || math.Float64bits(float64(int64(p.Value))) != math.Float64bits(p.Value) {
			integer = false
		}
		if integer && i == 1 {
			valueStep = int64(p.Value) - int64(points[0].Value)
		} else if i > 1 && integer && linear && int64(p.Value)-int64(points[i-1].Value) != valueStep {
			linear = false
		}
	}
	// Exponent zero reuses the existing decimal modes without rounding. The
	// integer preflight excludes -0, nonfinite values and inexact conversions.
	// For very short blocks the ordinary delta representation can be smaller.
	run := integer && linear && len(points) >= 4
	capacity := len(points) * 9
	if run {
		capacity = 2 + 2*binary.MaxVarintLen64
		if flags&sealedImplicit == 0 {
			capacity += len(points) - 1
		}
	}
	body := make([]byte, 0, capacity)
	if flags&sealedImplicit == 0 {
		for i := 1; i < len(points); i++ {
			body = binary.AppendUvarint(body, uint64(points[i].Timestamp-points[i-1].Timestamp))
		}
	}
	if run {
		body = binary.AppendVarint(body, 0) // exact integer exponent
		mode := byte(decimalLinear)
		if valueStep == 0 {
			mode = decimalConstant
		}
		body = append(body, mode)
		body = binary.AppendVarint(body, int64(points[0].Value))
		if mode == decimalLinear {
			body = binary.AppendVarint(body, valueStep)
		}
		return encodeSealedBody(len(points), points[0].Timestamp, step, flags|sealedDecimal, body), nil
	}
	timeEnd := len(body)
	tryBoth := integer
	if integer && len(points) >= 256 {
		useDoD, ambiguous := sealedDeltaChoice(points)
		tryBoth = ambiguous
		if useDoD {
			flags |= sealedDoD
		}
	}
	if integer {
		flags |= sealedInteger
		var previous, previousDelta int64
		for _, p := range points {
			value := int64(p.Value)
			delta := value - previous
			residual := delta
			if flags&sealedDoD != 0 {
				residual -= previousDelta
			}
			body = binary.AppendVarint(body, residual)
			previous, previousDelta = value, delta
		}
	} else {
		for plane := uint(0); plane < 8; plane++ {
			for _, p := range points {
				body = append(body, byte(math.Float64bits(p.Value)>>(plane*8)))
			}
		}
	}
	encode := func(flags byte, body []byte) []byte {
		return encodeSealedBody(len(points), points[0].Timestamp, step, flags, body)
	}
	best := encode(flags, body)
	if tryBoth {
		dod := append([]byte(nil), body[:timeEnd]...)
		var previous, delta int64
		for _, p := range points {
			value := int64(p.Value)
			next := value - previous
			dod = binary.AppendVarint(dod, next-delta)
			previous, delta = value, next
		}
		if candidate := encode(flags|sealedDoD, dod); len(candidate) < len(best) {
			best = candidate
		}
	}
	return best, nil
}

// sealedDeltaChoice estimates varint bytes without constructing either stream.
// A close estimate says little about Zstd's preference (notably for smooth
// gauges), so retain the compressed-size comparison within one byte per 128
// rows, with an eight-byte allowance for predictor startup. Small blocks bypass
// this scan entirely. points have already passed the exact-integer preflight.
func sealedDeltaChoice(points []Point) (useDoD, ambiguous bool) {
	var previous, previousDelta int64
	var deltaBytes, dodBytes int
	for _, p := range points {
		value := int64(p.Value)
		delta := value - previous
		deltaBytes += sealedVarintLen(delta)
		dodBytes += sealedVarintLen(delta - previousDelta)
		previous, previousDelta = value, delta
	}
	difference := deltaBytes - dodBytes
	margin := max(8, len(points)>>7)
	if difference >= -margin && difference <= margin {
		return false, true
	}
	return difference > 0, false
}

func sealedVarintLen(v int64) int {
	zigzag := uint64(v)<<1 ^ uint64(v>>63)
	return max(1, (bits.Len64(zigzag)+6)/7)
}

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

// EncodeSealedPointsDecimal applies a bounded twelve-digit decimal policy and keeps the exact candidate unless the complete encoding is smaller.
func EncodeSealedPointsDecimal(points []Point, step int) ([]byte, error) {
	exact, err := EncodeSealedPoints(points, step)
	if err != nil {
		return nil, err
	}
	// Exact integer runs already use the exponent-zero decimal representation.
	// Rebuilding the same mantissas and streams cannot make them smaller.
	if exact[1]&sealedDecimal != 0 {
		return exact, nil
	}
	exponent := 309
	for _, p := range points {
		e, ok := decimalExponent(p.Value)
		if !ok {
			return exact, nil
		}
		if p.Value != 0 && e < exponent {
			exponent = e
		}
	}
	if exponent == 309 {
		exponent = 0
	}
	values := make([]int64, len(points))
	scale := math.Pow10(exponent)
	for i, p := range points {
		m, ok := decimalMantissa(p.Value, scale)
		if !ok {
			return exact, nil
		}
		values[i] = m
	}
	flags := exact[1]&sealedImplicit | sealedDecimal
	var body []byte
	if flags&sealedImplicit == 0 {
		for i := 1; i < len(points); i++ {
			body = binary.AppendUvarint(body, uint64(points[i].Timestamp-points[i-1].Timestamp))
		}
	}
	body = appendDecimalIntegers(body, values, exponent)
	candidate := encodeSealedBody(len(points), points[0].Timestamp, step, flags, body)
	if len(candidate) < len(exact) {
		return candidate, nil
	}
	return exact, nil
}

// SealedPointBuffer reuses bounded point and decompression storage. Its zero
// value is ready to use. It is not safe for concurrent use. Returned points are
// borrowed until the next Decode, including a failed Decode; callers must copy
// anything they retain. At most MaxSealedPoints points and maxSealedBody bytes
// are retained, and no input bytes or pooled codec storage are retained.
type SealedPointBuffer struct {
	points []Point
	body   []byte
}

// DecodeSealedPoints returns independent points, with bounded decompression and
// point allocation. Use SealedPointBuffer when the result is consumed before
// decoding another block.
func DecodeSealedPoints(data []byte) ([]Point, error) {
	var scratch SealedPointBuffer
	return scratch.Decode(data)
}

// Decode replaces the borrowed points with one sealed block. On error it returns
// nil; previously returned points may have been overwritten.
func (scratch *SealedPointBuffer) Decode(data []byte) ([]Point, error) {
	first, step, count, err := sealedHeaderFields(data)
	if err != nil {
		return nil, err
	}
	body := data[sealedHeader:]
	if data[1]&sealedZstd != 0 {
		limit := count * 20
		var frame zstd.Header
		if err := frame.Decode(body); err != nil {
			return nil, errInvalidChunk
		}
		if frame.HasFCS {
			if frame.FrameContentSize > uint64(limit) {
				return nil, errInvalidChunk
			}
			limit = int(frame.FrameContentSize)
		}
		if cap(scratch.body) < limit {
			scratch.body = make([]byte, 0, limit)
		}
		z := sealedDecoders.Get().(*zstd.Decoder)
		// A previous large block must not relax this frame's expansion bound,
		// including frames without FCS and concatenated frames.
		decoded, err := z.DecodeAll(body, scratch.body[:0:limit])
		sealedDecoders.Put(z)
		if err != nil {
			return nil, errInvalidChunk
		}
		body = decoded
	}
	if len(body) > maxSealedBody {
		return nil, errInvalidChunk
	}
	header := [12]byte{}
	copy(header[:], data[:12])
	header[1] &^= sealedZstd
	checksum := crc32.Update(0, crc32.IEEETable, header[:])
	if crc32.Update(checksum, crc32.IEEETable, body) != binary.LittleEndian.Uint32(data[12:16]) {
		return nil, errInvalidChunk
	}
	if cap(scratch.points) < count {
		scratch.points = make([]Point, count)
	}
	points := scratch.points[:count]
	points[0].Timestamp = first
	offset := 0
	for i := 1; i < count; i++ {
		delta := uint64(step)
		if data[1]&sealedImplicit == 0 {
			var n int
			delta, n = binary.Uvarint(body[offset:])
			if n <= 0 {
				return nil, errInvalidChunk
			}
			offset += n
		}
		if delta == 0 || delta > math.MaxUint32 || uint64(points[i-1].Timestamp)+delta > math.MaxUint32 {
			return nil, errInvalidChunk
		}
		points[i].Timestamp = points[i-1].Timestamp + int64(delta)
	}
	if data[1]&sealedDecimal != 0 {
		if err := decodeDecimalPoints(body[offset:], points); err != nil {
			return nil, err
		}
		return points, nil
	}
	if data[1]&sealedInteger != 0 {
		var previous, delta int64
		for i := range points {
			value, n := binary.Varint(body[offset:])
			if n <= 0 {
				return nil, errInvalidChunk
			}
			offset += n
			if data[1]&sealedDoD != 0 {
				delta += value
			} else {
				delta = value
			}
			previous += delta
			if previous < -exactIntegerLimit || previous > exactIntegerLimit {
				return nil, errInvalidChunk
			}
			points[i].Value = float64(previous)
		}
	} else {
		if len(body)-offset != 8*count {
			return nil, errInvalidChunk
		}
		for i := range points {
			var value uint64
			for plane := 0; plane < 8; plane++ {
				value |= uint64(body[offset+plane*count+i]) << uint(plane*8)
			}
			points[i].Value = math.Float64frombits(value)
		}
		offset += 8 * count
	}
	if offset != len(body) {
		return nil, errInvalidChunk
	}
	return points, nil
}
