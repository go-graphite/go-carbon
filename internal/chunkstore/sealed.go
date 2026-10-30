package chunkstore

import (
	"encoding/binary"
	"hash/crc32"
	"math"
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
	integer := true
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
	}
	body := make([]byte, 0, len(points)*9)
	if flags&sealedImplicit == 0 {
		for i := 1; i < len(points); i++ {
			body = binary.AppendUvarint(body, uint64(points[i].Timestamp-points[i-1].Timestamp))
		}
	}
	timeEnd := len(body)
	if integer {
		flags |= sealedInteger
		var previous int64
		for _, p := range points {
			value := int64(p.Value)
			body = binary.AppendVarint(body, value-previous)
			previous = value
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
	if integer {
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

// DecodeSealedPoints bounds both decompression and point allocation. The caller
// retains no scratch owned by a pooled decoder, including on malformed input.
func DecodeSealedPoints(data []byte) ([]Point, error) {
	if len(data) < sealedHeader || len(data) > maxSealedBody+sealedHeader || data[0] != 3 || data[1]&^byte(31) != 0 || data[1]&sealedDecimal != 0 && data[1]&(sealedInteger|sealedDoD) != 0 || data[1]&sealedDoD != 0 && data[1]&sealedInteger == 0 {
		return nil, errInvalidChunk
	}
	count := int(binary.LittleEndian.Uint16(data[2:4]))
	step := int64(binary.LittleEndian.Uint32(data[8:12]))
	first := int64(binary.LittleEndian.Uint32(data[4:8]))
	if count == 0 || count > MaxSealedPoints || step == 0 || first == 0 {
		return nil, errInvalidChunk
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
		z := sealedDecoders.Get().(*zstd.Decoder)
		decoded, err := z.DecodeAll(body, make([]byte, 0, limit))
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
	points := make([]Point, count)
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
