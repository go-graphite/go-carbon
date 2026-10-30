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
	exactChunkVersion = 2
	exactChunkInteger = 2
	exactChunkZstd    = 4
	exactIntegerLimit = int64(1 << 53)
	maxExactPayload   = binary.MaxVarintLen32 + 16 + chunkSlots*2*binary.MaxVarintLen64
)

// Scratch belongs to active operations, not to every metric in the catalog.
var exactEncoders = sync.Pool{New: func() any {
	z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(1024))
	if err != nil {
		panic(err) // All options are constants and are validated by tests.
	}
	return z
}}

var exactDecoders = sync.Pool{New: func() any {
	z, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(64<<10), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		panic(err)
	}
	return z
}}

// Reuse serialization scratch while keeping each encoded result independently owned.
var exactBodyBuffers = sync.Pool{New: func() any { return new(exactBodyBuffer) }}

type exactBodyBuffer struct {
	body       [maxExactPayload]byte
	compressed [maxExactPayload + 64]byte // Zstd's frame overhead for one small block.
}

// A decoder owns this scratch until all scalar fields have been copied into chunk.
var exactReadBuffers = sync.Pool{New: func() any { return new([maxExactPayload + 2]byte) }}

// encodeExactChunk uses integer deltas only after bitwise Float64 reconstruction
// succeeds for every present value. Other chunks retain all Float64 bits through
// byte-stream splitting. Neither path rounds values or drops exceptional values.
func encodeExactChunk(c *chunk) []byte {
	flags := byte(0)
	if exactIntegers(c) {
		flags |= exactChunkInteger
	}
	scratch := exactBodyBuffers.Get().(*exactBodyBuffer)
	defer exactBodyBuffers.Put(scratch)
	body := scratch.body[:0]
	body = binary.AppendUvarint(body, uint64(c.Deltas))
	for _, word := range c.Present {
		body = binary.LittleEndian.AppendUint64(body, word)
	}
	var previousTime int64
	for slot := range c.Points {
		if c.has(slot) {
			timestamp := c.Points[slot].Timestamp
			body = binary.AppendVarint(body, timestamp-previousTime)
			previousTime = timestamp
		}
	}
	if flags&exactChunkInteger != 0 {
		var previousValue int64
		for slot := range c.Points {
			if c.has(slot) {
				value := int64(c.Points[slot].Value)
				body = binary.AppendVarint(body, value-previousValue)
				previousValue = value
			}
		}
	} else {
		for plane := uint(0); plane < 8; plane++ {
			for slot := range c.Points {
				if c.has(slot) {
					body = append(body, byte(math.Float64bits(c.Points[slot].Value)>>(plane*8)))
				}
			}
		}
	}
	checksum := exactChunkChecksum(flags, body)
	if len(body) >= 128 {
		z := exactEncoders.Get().(*zstd.Encoder)
		compressed := z.EncodeAll(body, scratch.compressed[:0])
		exactEncoders.Put(z)
		if len(compressed) < len(body) {
			body = compressed
			flags |= exactChunkZstd
		}
	}
	out := make([]byte, 6, 6+len(body))
	out[0], out[1] = exactChunkVersion, flags
	binary.LittleEndian.PutUint32(out[2:6], checksum)
	return append(out, body...)
}

func exactIntegers(c *chunk) bool {
	for slot := range c.Points {
		if !c.has(slot) {
			continue
		}
		value := c.Points[slot].Value
		if !(value >= -float64(exactIntegerLimit) && value <= float64(exactIntegerLimit)) ||
			math.Float64bits(float64(int64(value))) != math.Float64bits(value) {
			return false
		}
	}
	return true
}

// Protect the interpretation as well as the content. Compression is a transport
// detail; a different valid frame with identical decoded bytes remains valid.
var exactHeaderChecksums = [2]uint32{
	crc32.Update(0, crc32.IEEETable, []byte{exactChunkVersion, 0}),
	crc32.Update(0, crc32.IEEETable, []byte{exactChunkVersion, exactChunkInteger}),
}

func exactChunkChecksum(flags byte, body []byte) uint32 {
	checksum := exactHeaderChecksums[0]
	if flags&exactChunkInteger != 0 {
		checksum = exactHeaderChecksums[1]
	}
	return crc32.Update(checksum, crc32.IEEETable, body)
}

func exactChunkPayload(data []byte) ([]byte, error) {
	scratch := exactReadBuffers.Get().(*[maxExactPayload + 2]byte)
	defer exactReadBuffers.Put(scratch)
	payload, err := exactChunkPayloadInto(data, scratch[:])
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), payload...), nil
}

func exactChunkPayloadInto(data, scratch []byte) ([]byte, error) {
	if len(data) < 6 || len(data) > maxExactPayload+6 ||
		data[1]&^byte(exactChunkInteger|exactChunkZstd) != 0 {
		return nil, errInvalidChunk
	}
	body := data[6:]
	if data[1]&exactChunkZstd != 0 {
		z := exactDecoders.Get().(*zstd.Decoder)
		decoded, err := z.DecodeAll(body, scratch[2:2])
		exactDecoders.Put(z)
		if err != nil {
			return nil, errInvalidChunk
		}
		body = decoded
	}
	if len(body) < 17 || len(body) > maxExactPayload ||
		exactChunkChecksum(data[1], body) != binary.LittleEndian.Uint32(data[2:6]) {
		return nil, errInvalidChunk
	}
	payload := scratch[:2+len(body)]
	payload[0], payload[1] = exactChunkVersion, data[1]&exactChunkInteger
	copy(payload[2:], body)
	return payload, nil
}

func decodeExactChunkPoints(payload []byte, offset int, parsed *chunk) error {
	var previousTime int64
	for slot := range parsed.Points {
		if !parsed.has(slot) {
			continue
		}
		delta, n := binary.Varint(payload[offset:])
		if n <= 0 {
			return errInvalidChunk
		}
		offset += n
		previousTime += delta // Reversible modulo-2^64 timestamp deltas.
		parsed.Points[slot].Timestamp = previousTime
	}
	if payload[1]&exactChunkInteger != 0 {
		var previousValue int64
		for slot := range parsed.Points {
			if !parsed.has(slot) {
				continue
			}
			delta, n := binary.Varint(payload[offset:])
			if n <= 0 || delta > 0 && previousValue > math.MaxInt64-delta ||
				delta < 0 && previousValue < math.MinInt64-delta {
				return errInvalidChunk
			}
			offset += n
			previousValue += delta
			if previousValue < -exactIntegerLimit || previousValue > exactIntegerLimit {
				return errInvalidChunk
			}
			parsed.Points[slot].Value = float64(previousValue)
		}
	} else {
		count := bits.OnesCount64(parsed.Present[0]) + bits.OnesCount64(parsed.Present[1])
		if len(payload)-offset != 8*count {
			return errInvalidChunk
		}
		ordinal := 0
		for slot := range parsed.Points {
			if !parsed.has(slot) {
				continue
			}
			p := payload[offset+ordinal:]
			value := uint64(p[0]) | uint64(p[count])<<8 | uint64(p[2*count])<<16 | uint64(p[3*count])<<24 |
				uint64(p[4*count])<<32 | uint64(p[5*count])<<40 | uint64(p[6*count])<<48 | uint64(p[7*count])<<56
			parsed.Points[slot].Value = math.Float64frombits(value)
			ordinal++
		}
		offset += 8 * count
	}
	if offset != len(payload) {
		return errInvalidChunk
	}
	return nil
}

func (s *Store) encodeChunk(c *chunk) []byte {
	if s.exactBlocks {
		return encodeExactChunk(c)
	}
	return encodeChunk(c)
}
