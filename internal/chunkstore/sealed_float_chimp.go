package chunkstore

import (
	"math"
	"math/bits"
)

// Chimp128's four reference/window forms, with an explicit count rather than a
// NaN terminator. Integer XORs preserve every IEEE word. See Liakos et al.,
// PVLDB 15 (2022), section 4. State resets at each sealed group.
var sealedChimpLeads = [...]uint{0, 8, 12, 16, 18, 20, 22, 24}

func appendSealedChimp(dst []byte, p []Point) []byte {
	w := sealedBitWriter{data: dst}
	var ring [128]uint64
	var indices [16384]uint16
	prev := math.Float64bits(p[0].Value)
	ring[0] = prev
	indices[prev&16383] = 0
	w.put(prev, 64)
	stored := uint(65)
	for i := 1; i < len(p); i++ {
		v := math.Float64bits(p[i].Value)
		at := int(indices[v&16383])
		ref := (i - 1) & 127
		x := prev ^ v
		trail := uint(0)
		if i-1-at < 128 {
			candidate := v ^ ring[at&127]
			tz := uint(bits.TrailingZeros64(candidate))
			if tz > 13 {
				x = candidate
				ref = at & 127
				trail = tz
			}
		}
		if x == 0 {
			w.put(0, 2)
			w.put(uint64(ref), 7)
			stored = 65
		} else {
			lead := uint(bits.LeadingZeros64(x))
			code := 0
			for code < 7 && sealedChimpLeads[code+1] <= lead {
				code++
			}
			lead = sealedChimpLeads[code]
			switch {
			case trail > 13:
				s := 64 - lead - trail
				w.put(1, 2)
				w.put(uint64(ref), 7)
				w.put(uint64(code), 3)
				w.put(uint64(s), 6)
				w.put(x>>trail, s)
				stored = 65
			case lead == stored:
				w.put(2, 2)
				w.put(x, 64-lead)
			default:
				w.put(3, 2)
				w.put(uint64(code), 3)
				w.put(x, 64-lead)
				stored = lead
			}
		}
		ring[i&127] = v
		indices[v&16383] = uint16(i)
		prev = v
	}
	return w.finish()
}
func decodeSealedChimp(body []byte, p []Point) error {
	r := sealedBitReader{data: body}
	var ring [128]uint64
	v := r.get(64)
	ring[0] = v
	p[0].Value = math.Float64frombits(v)
	lead := uint(65)
	for i := 1; i < len(p); i++ {
		switch r.get(2) {
		case 0:
			at := int(r.get(7))
			if i < 128 && at >= i {
				return errInvalidChunk
			}
			v = ring[at]
			lead = 65
		case 1:
			at := int(r.get(7))
			if i < 128 && at >= i {
				return errInvalidChunk
			}
			l := sealedChimpLeads[r.get(3)]
			s := uint(r.get(6))
			if s == 0 {
				s = 64
			}
			if l+s > 64 {
				return errInvalidChunk
			}
			v = ring[at] ^ (r.get(s) << (64 - l - s))
			lead = 65
		case 2:
			if lead > 64 {
				return errInvalidChunk
			}
			v ^= r.get(64 - lead)
		case 3:
			lead = sealedChimpLeads[r.get(3)]
			v ^= r.get(64 - lead)
		}
		ring[i&127] = v
		p[i].Value = math.Float64frombits(v)
	}
	if !r.done() {
		return errInvalidChunk
	}
	return nil
}
