package chunkstore

import (
	"encoding/binary"
	"hash/crc32"
)

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
