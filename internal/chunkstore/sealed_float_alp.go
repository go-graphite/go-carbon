package chunkstore

// Exact decimal scaling uses raw IEEE exceptions for every value that cannot
// be reconstructed bit for bit, including signed zero and all nonfinite values.
import (
	"encoding/binary"
	"math"
)

// sealedFloatALP tries common and complete scales, retaining the complete fallback.
func sealedFloatALP(points []Point, step int, best []byte) []byte {
	// The integer codec already represents these values compactly and exactly.
	if best[1]&sealedInteger != 0 {
		return best
	}
	var counts [19]int
	unconvertible := 0
	for i, p := range points {
		e := sealedALPExponent(p.Value)
		if e < 0 {
			unconvertible++
		} else {
			counts[e]++
		}
		if i+1 == min(32, len(points)) && unconvertible*2 > i+1 {
			return best
		}
	}
	all, common, covered := -1, -1, 0
	for e, n := range counts {
		if n > 0 {
			all = e
		}
		covered += n
		if common < 0 && covered >= (len(points)*9+9)/10 {
			common = e
		}
	}
	if all < 0 {
		return best
	}
	if common < 0 {
		common = all
	}
	for _, e := range []int{all, common} {
		flags := best[1] & sealedImplicit
		var body []byte
		if flags&sealedImplicit == 0 {
			for i := 1; i < len(points); i++ {
				body = binary.AppendUvarint(body, uint64(points[i].Timestamp-points[i-1].Timestamp))
			}
		}
		body = append(body, sealedFloatALPMode)
		body = appendSealedALP(body, points, e)
		candidate := encodeSealedBodyVersion(len(points), points[0].Timestamp, step, flags, body, 4)
		if len(candidate) < len(best) {
			best = candidate
		}
		if all == common {
			break
		}
	}
	return best
}

// A mantissa is admitted only when the decoder's division reconstructs all bits.
// In particular -0, NaN payloads, infinities and subnormals become raw exceptions.
func sealedALPMantissa(v float64, e int) (int64, bool) {
	scaled := math.Round(v * math.Pow10(e))
	if !(scaled >= -float64(exactIntegerLimit) && scaled <= float64(exactIntegerLimit)) {
		return 0, false
	}
	m := int64(scaled)
	return m, math.Float64bits(float64(m)/math.Pow10(e)) == math.Float64bits(v)
}
func sealedALPExponent(v float64) int {
	for e := 0; e <= 18; e++ {
		if _, ok := sealedALPMantissa(v, e); ok {
			return e
		}
	}
	return -1
}
func appendSealedALP(dst []byte, points []Point, e int) []byte {
	values := make([]int64, len(points))
	exceptions := make([]byte, 0)
	previous, count, first := 0, 0, -1
	var last int64
	for i, p := range points {
		if m, ok := sealedALPMantissa(p.Value, e); ok {
			if first < 0 {
				first = i
				for j := 0; j < i; j++ {
					values[j] = m
				}
			}
			last = m
		} else {
			exceptions = binary.AppendUvarint(exceptions, uint64(i-previous))
			exceptions = binary.LittleEndian.AppendUint64(exceptions, math.Float64bits(p.Value))
			previous, count = i, count+1
		}
		values[i] = last
	}
	dst = binary.AppendUvarint(dst, uint64(count))
	dst = append(dst, exceptions...)
	return appendDecimalIntegers(dst, values, -e)
}

func decodeSealedALP(payload []byte, points []Point) error {
	count, n := binary.Uvarint(payload)
	if n <= 0 || count >= uint64(len(points)) {
		return errInvalidChunk
	}
	offset, at := n, uint64(0)
	for k := uint64(0); k < count; k++ {
		gap, size := binary.Uvarint(payload[offset:])
		if size <= 0 || k > 0 && gap == 0 || gap >= uint64(len(points))-at {
			return errInvalidChunk
		}
		at += gap
		offset += size
		if len(payload)-offset < 8 {
			return errInvalidChunk
		}
		offset += 8
	}
	exponent, size := binary.Varint(payload[offset:])
	if size <= 0 || exponent > 0 || exponent < -18 {
		return errInvalidChunk
	}
	if err := decodeDecimalPoints(payload[offset:], points); err != nil {
		return err
	}
	offset, at = n, 0
	for k := uint64(0); k < count; k++ {
		gap, size := binary.Uvarint(payload[offset:])
		at += gap
		offset += size
		points[at].Value = math.Float64frombits(binary.LittleEndian.Uint64(payload[offset:]))
		offset += 8
	}
	return nil
}
