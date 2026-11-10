package chunkstore

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// CanonicalPointRun proves a finite, dense prefix in an exact physical group.
// It verifies CRC/decompression and scalar bounds, but never reconstructs floats
// or allocates point arrays. Sparse/off-grid/special-value groups use the decoder.
func CanonicalPointRun(data []byte, archiveStep int) (first, last int64, count int, known bool) {
	if len(data) < 2 || data[0] != exactChunkVersion || archiveStep <= 0 {
		return
	}
	scratch := exactReadBuffers.Get().(*[maxExactPayload + 2]byte)
	defer exactReadBuffers.Put(scratch)
	payload, err := exactChunkPayloadInto(data, scratch[:])
	if err != nil {
		return
	}
	deltas, n := binary.Uvarint(payload[2:])
	if n <= 0 || deltas != 0 || len(payload)-(2+n) < 16 {
		return
	}
	offset := 2 + n
	present := [2]uint64{binary.LittleEndian.Uint64(payload[offset:]), binary.LittleEndian.Uint64(payload[offset+8:])}
	offset += 16
	count = bits.OnesCount64(present[0]) + bits.OnesCount64(present[1])
	if count == 0 {
		return 0, 0, 0, false
	}
	expected := [2]uint64{^uint64(0) >> uint(64-min(count, 64)), 0}
	if count > 64 {
		expected[1] = ^uint64(0) >> uint(128-count)
	}
	if expected != present {
		return 0, 0, 0, false
	}
	for i := 0; i < count; i++ {
		delta, n := binary.Varint(payload[offset:])
		if n <= 0 {
			return 0, 0, 0, false
		}
		offset += n
		if i > 0 && delta != int64(archiveStep) {
			return 0, 0, 0, false
		}
		last += delta
		if last <= 0 || last > math.MaxUint32 || last%int64(archiveStep) != 0 {
			return 0, 0, 0, false
		}
		if i == 0 {
			first = last
		}
	}
	if payload[1]&exactChunkInteger != 0 {
		var previous int64
		for i := 0; i < count; i++ {
			delta, n := binary.Varint(payload[offset:])
			if n <= 0 || delta > 0 && previous > math.MaxInt64-delta || delta < 0 && previous < math.MinInt64-delta {
				return 0, 0, 0, false
			}
			offset += n
			previous += delta
			if previous < -exactIntegerLimit || previous > exactIntegerLimit {
				return 0, 0, 0, false
			}
		}
	} else {
		if len(payload)-offset != 8*count {
			return 0, 0, 0, false
		}
		planes := payload[offset:]
		for i := 0; i < count; i++ {
			if planes[7*count+i]&0x7f == 0x7f && planes[6*count+i]&0xf0 == 0xf0 {
				return 0, 0, 0, false
			}
		}
		offset += 8 * count
	}
	return first, last, count, offset == len(payload)
}
