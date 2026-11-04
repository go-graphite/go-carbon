package floatresearch

// Additional offline controls; these frames must never enter a live store.
import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var VariantKinds = []string{"sealed", "chimp128", "alp", "dictionary", "minfour", "minfast", "prefix64", "prefix256", "prefix1024", "spread256", "block256", "xor-planes", "planes-separate", "planes-level3", "planes-mixed", "whole-level3"}

var variantSlow = sync.Pool{New: func() any {
	z, e := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithWindowSize(128<<10))
	if e != nil {
		panic(e)
	}
	return z
}}

func (c Codec) Variant(kind string, g Group) ([]byte, string, error) {
	base, err := c.EncodeCurrent(g.Points, g.Step)
	if err != nil || kind == "sealed" || base[1]&(sealedInteger|sealedDecimal) != 0 {
		return base, "sealed", err
	}
	best, winner := base, "sealed"
	var kinds []string
	switch kind {
	case "minfour":
		kinds = []string{"chimp128", "alp", "dictionary"}
	case "minfast":
		kinds = []string{"alp", "dictionary"}
	case "prefix64", "prefix256", "prefix1024", "spread256":
		n := 256
		if kind == "prefix64" {
			n = 64
		}
		if kind == "prefix1024" {
			n = 1024
		}
		p := g.Points[:min(n, len(g.Points))]
		if kind == "spread256" {
			p = make([]Point, min(n, len(g.Points)))
			for i := range p {
				p[i] = g.Points[i*(len(g.Points)-1)/max(1, len(p)-1)]
			}
		}
		_, selected, e := c.Variant("minfour", Group{Points: p, Step: g.Step})
		if e != nil {
			return nil, "", e
		}
		if selected != "sealed" {
			kinds = []string{selected}
		}
	case "block256":
		b := []byte{253}
		for first := 0; first < len(g.Points); first += 256 {
			v, k, e := c.Variant("minfour", Group{Points: g.Points[first:min(first+256, len(g.Points))], Step: g.Step})
			if e != nil {
				return nil, "", e
			}
			tag := byte(0)
			for i, s := range []string{"sealed", "chimp128", "alp", "dictionary"} {
				if s == k {
					tag = byte(i)
				}
			}
			b = append(b, tag)
			b = binary.AppendUvarint(b, uint64(len(v)))
			b = append(b, v...)
		}
		if len(b) < len(best) {
			best, winner = b, kind
		}
		return best, winner, nil
	default:
		kinds = []string{kind}
	}
	for _, k := range kinds {
		var b []byte
		var e error
		if k == "planes-separate" || k == "planes-level3" || k == "planes-mixed" || k == "whole-level3" {
			b, e = c.planeVariant(k, g, base)
		} else {
			b, e = c.Encode(k, g)
		}
		if e != nil {
			return nil, "", e
		}
		if len(b) < len(best) {
			best, winner = b, k
		}
	}
	return best, winner, nil
}

func (c Codec) DecodeVariant(kind string, data []byte) ([]Point, error) {
	if kind == "sealed" {
		return c.DecodeCurrent(data)
	}
	if kind == "block256" {
		var out []Point
		data = data[1:]
		for len(data) > 0 {
			tag := int(data[0])
			n, k := binary.Uvarint(data[1:])
			if tag > 3 || k <= 0 || n > uint64(len(data)-1-k) {
				return nil, fmt.Errorf("sub-block")
			}
			data = data[1+k:]
			p, e := c.DecodeVariant([]string{"sealed", "chimp128", "alp", "dictionary"}[tag], data[:n])
			if e != nil {
				return nil, e
			}
			out = append(out, p...)
			data = data[n:]
		}
		return out, nil
	}
	if kind == "planes-separate" || kind == "planes-level3" || kind == "planes-mixed" || kind == "whole-level3" {
		n := int(binary.LittleEndian.Uint16(data[2:4]))
		body := data[16:]
		var raw []byte
		z := sealedDecoders.Get().(*zstd.Decoder)
		defer sealedDecoders.Put(z)
		if kind == "whole-level3" {
			var e error
			raw, e = z.DecodeAll(body, make([]byte, 0, n*20))
			if e != nil {
				return nil, e
			}
		} else {
			for len(body) > 0 {
				size, k := binary.Uvarint(body)
				if k <= 0 || size > uint64(len(body)-k) {
					return nil, fmt.Errorf("plane")
				}
				body = body[k:]
				v, e := z.DecodeAll(body[:size], make([]byte, 0, n*20))
				if e != nil {
					return nil, e
				}
				raw = append(raw, v...)
				body = body[size:]
			}
		}
		plain := append([]byte(nil), data[:16]...)
		plain[0] = 3
		plain[1] &^= sealedZstd
		plain = append(plain, raw...)
		return c.DecodeCurrent(plain)
	}
	return c.Decode(kind, data)
}

func (c Codec) planeVariant(kind string, g Group, base []byte) ([]byte, error) {
	flags := base[1] & sealedImplicit
	var times []byte
	if flags == 0 {
		for i := 1; i < len(g.Points); i++ {
			times = binary.AppendUvarint(times, uint64(g.Points[i].Timestamp-g.Points[i-1].Timestamp))
		}
	}
	raw := append(times, frPlanes(g.Points, false)...)
	out := make([]byte, 16)
	copy(out, base[:12])
	out[0] = 3
	out[1] = flags
	sum := crc32.Update(0, crc32.IEEETable, out[:12])
	binary.LittleEndian.PutUint32(out[12:], crc32.Update(sum, crc32.IEEETable, raw))
	fast := sealedEncoders.Get().(*zstd.Encoder)
	defer sealedEncoders.Put(fast)
	slow := variantSlow.Get().(*zstd.Encoder)
	defer variantSlow.Put(slow)
	if kind == "whole-level3" {
		return append(out, slow.EncodeAll(raw, nil)...), nil
	}
	streams := [][]byte{times}
	for i := 0; i < 8; i++ {
		streams = append(streams, raw[len(times)+i*len(g.Points):len(times)+(i+1)*len(g.Points)])
	}
	for i, p := range streams {
		z := fast
		if kind == "planes-level3" || kind == "planes-mixed" && i >= 5 {
			z = slow
		}
		b := z.EncodeAll(p, nil)
		out = binary.AppendUvarint(out, uint64(len(b)))
		out = append(out, b...)
	}
	return out, nil
}

// IEEEEqual compares payloads, including signed zero and NaN payload bits.
func IEEEEqual(a, b []Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i, p := range a {
		if p.Timestamp != b[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(b[i].Value) {
			return false
		}
	}
	return true
}
