// Package chunkstore contains the Pebble-backed chunk storage format.
package chunkstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/points"
	"github.com/golang/snappy"
)

const (
	chunkSlots    = 128
	materializeAt = 32

	chunkVersion    = 1
	chunkCompressed = 1
	chunkHeaderSize = 2 + 16 // version, flags, and the presence bitmap
	// Each point is a signed varint timestamp plus an eight byte float.
	maxChunkSize = chunkHeaderSize + binary.MaxVarintLen64 + chunkSlots*(binary.MaxVarintLen64+8)
)

var (
	errInvalidChunk  = errors.New("chunkstore: invalid chunk")
	errTooManyDeltas = errors.New("chunkstore: too many chunk deltas")
)

// chunk holds a sparse set of slots. Deltas is the number of merge operands
// represented by the value; a materialized Pebble Set has Deltas == 0.
type chunk struct {
	Deltas  uint32
	Present [2]uint64
	Points  [chunkSlots]points.Point
}

func (c *chunk) has(slot int) bool {
	return slot >= 0 && slot < chunkSlots && c.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0
}

func (c *chunk) set(slot int, p points.Point) {
	if slot < 0 || slot >= chunkSlots {
		panic("chunkstore: slot out of range")
	}
	c.Present[slot/64] |= uint64(1) << uint(slot%64)
	c.Points[slot] = p
}

// encodeChunk returns a self-contained chunk. The payload is compressed only
// when Snappy makes it smaller, so a decoder never has to guess its format.
func encodeChunk(c *chunk) []byte {
	payload := make([]byte, 0, maxChunkSize)
	payload = append(payload, chunkVersion, 0)
	var deltas [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(deltas[:], uint64(c.Deltas))
	payload = append(payload, deltas[:n]...)
	for _, word := range c.Present {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], word)
		payload = append(payload, b[:]...)
	}
	var timestamp [binary.MaxVarintLen64]byte
	var value [8]byte
	var previousTimestamp int64
	for slot := 0; slot < chunkSlots; slot++ {
		if !c.has(slot) {
			continue
		}
		// Signed integer arithmetic is deliberately modulo 2^64 here. It
		// makes deltas reversible even for a sequence spanning int64's ends.
		delta := c.Points[slot].Timestamp - previousTimestamp
		n = binary.PutVarint(timestamp[:], delta)
		payload = append(payload, timestamp[:n]...)
		binary.LittleEndian.PutUint64(value[:], math.Float64bits(c.Points[slot].Value))
		payload = append(payload, value[:]...)
		previousTimestamp = c.Points[slot].Timestamp
	}

	compressed := snappy.Encode(nil, payload[2:])
	if len(compressed) >= len(payload)-2 {
		return payload
	}
	result := make([]byte, 2+len(compressed))
	result[0] = chunkVersion
	result[1] = chunkCompressed
	copy(result[2:], compressed)
	return result
}

// decodeChunk validates the whole input before publishing it into dst.
func decodeChunk(data []byte, dst *chunk) error {
	if len(data) < 2 || data[0] != chunkVersion || data[1]&^byte(chunkCompressed) != 0 {
		return errInvalidChunk
	}
	payload := data
	if data[1]&chunkCompressed != 0 {
		decodedLen, err := snappy.DecodedLen(data[2:])
		if err != nil || decodedLen < chunkHeaderSize-1 || decodedLen > maxChunkSize-2 {
			return errInvalidChunk
		}
		payload = make([]byte, 2+decodedLen)
		payload[0] = chunkVersion
		payload[1] = 0
		if _, err := snappy.Decode(payload[2:], data[2:]); err != nil {
			return errInvalidChunk
		}
	} else if len(payload) > maxChunkSize {
		return errInvalidChunk
	}

	var parsed chunk
	offset := 2
	deltas, n := binary.Uvarint(payload[offset:])
	if n <= 0 || deltas >= materializeAt {
		return errInvalidChunk
	}
	offset += n
	if len(payload)-offset < 16 {
		return errInvalidChunk
	}
	parsed.Deltas = uint32(deltas)
	for i := range parsed.Present {
		parsed.Present[i] = binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
	}
	var previousTimestamp int64
	for slot := 0; slot < chunkSlots; slot++ {
		if !parsed.has(slot) {
			continue
		}
		timestamp, n := binary.Varint(payload[offset:])
		if n <= 0 {
			return errInvalidChunk
		}
		offset += n
		if len(payload)-offset < 8 {
			return errInvalidChunk
		}
		parsed.Points[slot] = points.Point{
			Timestamp: previousTimestamp + timestamp,
			Value:     math.Float64frombits(binary.LittleEndian.Uint64(payload[offset : offset+8])),
		}
		previousTimestamp = parsed.Points[slot].Timestamp
		offset += 8
	}
	if offset != len(payload) {
		return errInvalidChunk
	}
	*dst = parsed
	return nil
}

// chunkMerger is deliberately pure: it copies operands while decoding them and
// makes no assumptions about the time represented by a slot.
var chunkMerger = &pebble.Merger{
	Name: "go-carbon.chunks.v1",
	Merge: func(_ []byte, value []byte) (pebble.ValueMerger, error) {
		var c chunk
		if err := decodeChunk(value, &c); err != nil {
			return nil, err
		}
		return &chunkValueMerger{chunk: c}, nil
	},
}

type chunkValueMerger struct {
	chunk chunk
}

func (m *chunkValueMerger) MergeNewer(value []byte) error {
	var newer chunk
	if err := decodeChunk(value, &newer); err != nil {
		return err
	}
	return m.combine(&newer, true)
}

func (m *chunkValueMerger) MergeOlder(value []byte) error {
	var older chunk
	if err := decodeChunk(value, &older); err != nil {
		return err
	}
	return m.combine(&older, false)
}

func (m *chunkValueMerger) combine(other *chunk, otherIsNewer bool) error {
	if other.Deltas >= materializeAt || m.chunk.Deltas >= materializeAt || uint64(other.Deltas)+uint64(m.chunk.Deltas) >= materializeAt {
		return errTooManyDeltas
	}
	for slot := 0; slot < chunkSlots; slot++ {
		if !other.has(slot) {
			continue
		}
		if otherIsNewer || !m.chunk.has(slot) {
			m.chunk.set(slot, other.Points[slot])
		}
	}
	m.chunk.Deltas += other.Deltas
	return nil
}

func (m *chunkValueMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	if m.chunk.Deltas >= materializeAt {
		return nil, nil, fmt.Errorf("%w: %d", errTooManyDeltas, m.chunk.Deltas)
	}
	return encodeChunk(&m.chunk), nil, nil
}
