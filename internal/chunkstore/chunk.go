// Package chunkstore contains the Pebble-backed chunk storage format.
package chunkstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

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
	var payload []byte
	var err error
	if len(data) >= 2 && data[0] == exactChunkVersion {
		scratch := exactReadBuffers.Get().(*[maxExactPayload + 2]byte)
		defer exactReadBuffers.Put(scratch)
		payload, err = exactChunkPayloadInto(data, scratch[:])
	} else {
		payload, err = chunkPayload(data)
	}
	if err != nil {
		return err
	}
	var parsed chunk
	offset, err := decodeChunkHeader(payload, &parsed)
	if err != nil {
		return err
	}
	if err := decodeChunkPoints(payload, offset, &parsed); err != nil {
		return err
	}
	*dst = parsed
	return nil
}

func chunkPayload(data []byte) ([]byte, error) {
	if len(data) >= 2 && data[0] == exactChunkVersion {
		return exactChunkPayload(data)
	}
	if len(data) < 2 || data[0] != chunkVersion || data[1]&^byte(chunkCompressed) != 0 {
		return nil, errInvalidChunk
	}
	payload := data
	if data[1]&chunkCompressed != 0 {
		decodedLen, err := snappy.DecodedLen(data[2:])
		if err != nil || decodedLen < chunkHeaderSize-1 || decodedLen > maxChunkSize-2 {
			return nil, errInvalidChunk
		}
		payload = make([]byte, 2+decodedLen)
		payload[0] = chunkVersion
		payload[1] = 0
		if _, err := snappy.Decode(payload[2:], data[2:]); err != nil {
			return nil, errInvalidChunk
		}
	} else if len(payload) > maxChunkSize {
		return nil, errInvalidChunk
	}
	return payload, nil
}

func decodeChunkHeader(payload []byte, parsed *chunk) (int, error) {
	offset := 2
	deltas, n := binary.Uvarint(payload[offset:])
	if n <= 0 || deltas >= materializeAt {
		return 0, errInvalidChunk
	}
	offset += n
	if len(payload)-offset < 16 {
		return 0, errInvalidChunk
	}
	parsed.Deltas = uint32(deltas)
	for i := range parsed.Present {
		parsed.Present[i] = binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
	}
	return offset, nil
}

func decodeChunkPoints(payload []byte, offset int, parsed *chunk) error {
	if payload[0] == exactChunkVersion {
		return decodeExactChunkPoints(payload, offset, parsed)
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
	return nil
}

// chunkMerger is deliberately pure: it copies operands while decoding them and
// makes no assumptions about the time represented by a slot.
var chunkMerger = newChunkMerger(false)
var exactChunkMerger = newChunkMerger(true)

// Workspaces belong to active merges, never to metric/catalog entries. Finish
// returns independently owned bytes and is the last ValueMerger call, so the
// decoded workspace can be reused as soon as serialization finishes.
var chunkValueMergers = sync.Pool{New: func() any { return new(chunkValueMerger) }}

func newChunkMerger(exact bool) *pebble.Merger {
	name := "go-carbon.chunks.v1"
	if exact {
		name = "go-carbon.chunks.exact.v1"
	}
	return &pebble.Merger{Name: name, Merge: func(_ []byte, value []byte) (pebble.ValueMerger, error) {
		m := chunkValueMergers.Get().(*chunkValueMerger)
		m.exact = exact
		if err := decodeChunk(value, &m.chunk); err != nil {
			chunkValueMergers.Put(m)
			return nil, err
		}
		return m, nil
	}}
}

type chunkValueMerger struct {
	chunk chunk
	exact bool
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
	defer chunkValueMergers.Put(m)
	if m.chunk.Deltas >= materializeAt {
		return nil, nil, fmt.Errorf("%w: %d", errTooManyDeltas, m.chunk.Deltas)
	}
	if m.exact {
		return encodeExactChunk(&m.chunk), nil, nil
	}
	return encodeChunk(&m.chunk), nil, nil
}
