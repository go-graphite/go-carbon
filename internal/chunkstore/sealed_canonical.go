package chunkstore

import (
	"encoding/binary"
	"hash/crc32"
)

// CanonicalRun also proves legacy byte-plane groups with implicit timestamps.
// It verifies the complete frame and checks the two exponent planes for NaN/Inf
// without reconstructing any timestamp or float. Unsupported encodings still
// require Decode. Neither path publishes a partially validated group.
func (scratch *SealedPointBuffer) CanonicalRun(data []byte, archiveStep int) (first, last int64, count int, known bool) {
	if first, last, count, known = CanonicalSealedRun(data, archiveStep); known {
		return
	}
	if len(data) < sealedHeader || data[0] != 3 || data[1]&^byte(sealedImplicit|sealedZstd) != 0 || data[1]&sealedImplicit == 0 {
		return 0, 0, 0, false
	}
	first, step, count, body, err := scratch.sealedBody(data)
	if err != nil || step != int64(archiveStep) || first%step != 0 || len(body) != 8*count {
		return 0, 0, 0, false
	}
	last = first + int64(count-1)*step
	if last > 1<<32-1 {
		return 0, 0, 0, false
	}
	for i := range count {
		if body[7*count+i]&0x7f == 0x7f && body[6*count+i]&0xf0 == 0xf0 {
			return 0, 0, 0, false
		}
	}
	return first, last, count, true
}

// CanonicalSealedRun proves a dense exact-integer constant/linear run from its
// checksummed scalar encoding, without expanding its timestamps or values. Other
// encodings need the ordinary decoder; an unknown result is never eligibility.
func CanonicalSealedRun(data []byte, archiveStep int) (first, last int64, count int, known bool) {
	first, step, count, err := sealedHeaderFields(data)
	if err != nil || data[0] != 3 || data[1] != sealedImplicit|sealedDecimal || step != int64(archiveStep) || first%step != 0 {
		return 0, 0, 0, false
	}
	last = first + int64(count-1)*step
	checksum := crc32.Update(crc32.ChecksumIEEE(data[:12]), crc32.IEEETable, data[16:])
	if last > 1<<32-1 || checksum != binary.LittleEndian.Uint32(data[12:16]) {
		return 0, 0, 0, false
	}
	body := data[sealedHeader:]
	exponent, n := binary.Varint(body)
	if n <= 0 || exponent != 0 || n == len(body) {
		return 0, 0, 0, false
	}
	body = body[n:]
	mode := body[0]
	if mode != decimalConstant && mode != decimalLinear {
		return 0, 0, 0, false
	}
	value, n := binary.Varint(body[1:])
	if n <= 0 || value <= -1<<62 || value >= 1<<62 {
		return 0, 0, 0, false
	}
	body = body[1+n:]
	if mode == decimalLinear {
		delta, n := binary.Varint(body)
		if n <= 0 {
			return 0, 0, 0, false
		}
		body = body[n:]
		// Bound the product before adding: all intermediate values are then
		// inside the same valid convex integer range as the endpoints.
		if count > 1 {
			steps := int64(count - 1)
			if delta > 0 && delta > ((1<<62-1)-value)/steps || delta < 0 && delta < ((-1<<62+1)-value)/steps {
				return 0, 0, 0, false
			}
		}
	}
	return first, last, count, len(body) == 0
}
