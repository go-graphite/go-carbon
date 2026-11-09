package chunkstore

import (
	"encoding/binary"
	"math"
)

// SealedTimeBounds reads conservative inclusive time bounds from the existing
// sealed header, without decompressing values. Irregular timestamps have no
// stored maximum, so their upper bound is MaxUint32. These are pruning hints:
// the payload CRC is checked only by DecodeSealedPoints, for selected groups.
func SealedTimeBounds(data []byte) (first, last int64, count int, err error) {
	first, step, count, err := sealedHeaderFields(data)
	if err != nil {
		return 0, 0, 0, err
	}
	last = math.MaxUint32
	if data[1]&sealedImplicit != 0 {
		last = first + int64(count-1)*step
		if last > math.MaxUint32 {
			return 0, 0, 0, errInvalidChunk
		}
	}
	return first, last, count, nil
}

// Keep header validation shared with the full decoder, including codec flags.
func sealedHeaderFields(data []byte) (first, step int64, count int, err error) {
	if len(data) < sealedHeader || len(data) > maxSealedBody+sealedHeader {
		return 0, 0, 0, errInvalidChunk
	}
	validFlags := data[0] == 3 && data[1]&^byte(31) == 0 && !(data[1]&sealedDecimal != 0 && data[1]&(sealedInteger|sealedDoD) != 0) && !(data[1]&sealedDoD != 0 && data[1]&sealedInteger == 0)
	if data[0] == 4 {
		validFlags = data[1]&^byte(sealedImplicit|sealedZstd) == 0
	}
	if !validFlags {
		return 0, 0, 0, errInvalidChunk
	}
	count = int(binary.LittleEndian.Uint16(data[2:4]))
	first = int64(binary.LittleEndian.Uint32(data[4:8]))
	step = int64(binary.LittleEndian.Uint32(data[8:12]))
	if count == 0 || count > MaxSealedPoints || first == 0 || step == 0 {
		return 0, 0, 0, errInvalidChunk
	}
	return first, step, count, nil
}
