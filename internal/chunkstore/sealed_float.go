package chunkstore

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	sealedFloatALPMode        = 1
	sealedFloatDictionaryMode = 2
	sealedFloatChimpMode      = 3
)

// IsLegacySealedFloat identifies the v3 byte-plane mode, including raw bodies.
// Full validation and CRC verification still belong to the decoder.
func IsLegacySealedFloat(data []byte) bool {
	return len(data) >= sealedHeader && data[0] == 3 && data[1]&^byte(sealedImplicit|sealedZstd) == 0
}

// EncodeSealedPointsFloat uses the first 64 points to select an exact value
// codec, then compares its complete group frame with today's encoder. Groups
// below 129 points retain the old format. Selection never enters the read path.
func EncodeSealedPointsFloat(points []Point, step int) ([]byte, error) {
	return EncodeSealedPointsFloatPrefix(points, step, 64)
}

// EncodeSealedPointsFloatMin is the exhaustive offline transcode control.
// Integer/decimal groups stay on the existing format. A new frame must be
// strictly smaller than the complete legacy frame, including timestamps/CRC.
func EncodeSealedPointsFloatMin(points []Point, step int) ([]byte, error) {
	base, err := EncodeSealedPoints(points, step)
	if err != nil || base[1]&(sealedInteger|sealedDecimal) != 0 || len(points) < 129 {
		return base, err
	}
	best, _ := encodeSealedFloatCandidates(points, step, base, 0)
	return best, nil
}

// EncodeSealedPointsFloatPrefix shares the production selector with offline
// transcode controls that compare different prefix lengths.
func EncodeSealedPointsFloatPrefix(points []Point, step, prefixPoints int) ([]byte, error) {
	if prefixPoints < 1 || prefixPoints > MaxSealedPoints {
		return nil, errInvalidChunk
	}
	base, err := EncodeSealedPoints(points, step)
	if err != nil || base[1]&(sealedInteger|sealedDecimal) != 0 || len(points) < 129 {
		return base, err
	}
	selected := byte(0)
	if len(points) > prefixPoints {
		prefix, err := EncodeSealedPoints(points[:prefixPoints], step)
		if err != nil {
			return nil, err
		}
		_, selected = encodeSealedFloatCandidates(points[:prefixPoints], step, prefix, 0)
		if selected == 0 {
			return base, nil
		}
	}
	best, _ := encodeSealedFloatCandidates(points, step, base, selected)
	return best, nil
}

func encodeSealedFloatCandidates(points []Point, step int, base []byte, only byte) ([]byte, byte) {
	best, winner := base, byte(0)
	if only == 0 || only == sealedFloatALPMode {
		best = sealedFloatALP(points, step, base)
		if len(best) < len(base) {
			winner = sealedFloatALPMode
		}
	}

	flags := base[1] & sealedImplicit
	var body []byte
	if flags == 0 {
		for i := 1; i < len(points); i++ {
			body = binary.AppendUvarint(body, uint64(points[i].Timestamp-points[i-1].Timestamp))
		}
	}
	timeEnd := len(body)
	if only == 0 || only == sealedFloatDictionaryMode {
		body = append(body, sealedFloatDictionaryMode)
		body = appendSealedDictionary(body, points)
		if candidate := encodeSealedBodyVersion(len(points), points[0].Timestamp, step, flags, body, 4); len(candidate) < len(best) {
			best, winner = candidate, sealedFloatDictionaryMode
		}
	}
	// Reuse the timestamp prefix for the recent-value XOR candidate.
	if only == 0 || only == sealedFloatChimpMode {
		body = body[:timeEnd]
		body = append(body, sealedFloatChimpMode)
		body = appendSealedChimp(body, points)
		// Dense frames decode cheaply as planes. Admit a Chimp replacement only
		// with a material (at least 12.5%) saving there; small wins failed the
		// capture decode budget. ALP and dictionary need no such restriction.
		if candidate := encodeSealedBodyVersion(len(points), points[0].Timestamp, step, flags, body, 4); len(candidate) < len(best) && (len(base) <= 5*len(points) || len(candidate)*8 <= len(base)*7) {
			best, winner = candidate, sealedFloatChimpMode
		}
	}
	return best, winner
}

func appendSealedDictionary(dst []byte, points []Point) []byte {
	dict := make(map[uint64]uint64)
	values := make([]uint64, 0)
	ids := make([]uint64, len(points))
	for i, p := range points {
		v := math.Float64bits(p.Value)
		id, ok := dict[v]
		if !ok {
			id = uint64(len(values))
			dict[v] = id
			values = append(values, v)
		}
		ids[i] = id
	}
	dst = binary.AppendUvarint(dst, uint64(len(values)))
	for _, v := range values {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}
	w := sealedBitWriter{data: dst}
	width := uint(bits.Len(uint(len(values) - 1)))
	for _, id := range ids {
		w.put(id, width)
	}
	return w.finish()
}

func decodeSealedFloat(body []byte, points []Point) error {
	if len(body) == 0 {
		return errInvalidChunk
	}
	switch body[0] {
	case sealedFloatALPMode:
		return decodeSealedALP(body[1:], points)
	case sealedFloatDictionaryMode:
		return decodeSealedDictionary(body[1:], points)
	case sealedFloatChimpMode:
		return decodeSealedChimp(body[1:], points)
	default:
		return errInvalidChunk
	}
}
