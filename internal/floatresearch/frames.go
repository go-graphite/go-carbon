// Package floatresearch holds offline experiments. No production path imports it.
// Candidate frames are private and must never be installed in a live store.
package floatresearch

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
)

type Group struct {
	Points []Point
	Step   int
}
type Codec struct {
	EncodeCurrent func([]Point, int) ([]byte, error)
	DecodeCurrent func([]byte) ([]Point, error)
}

func (c Codec) Encode(kind string, g Group) ([]byte, error) {
	p := g.Points
	if kind == "sealed" {
		return c.EncodeCurrent(p, g.Step)
	}
	if kind == "alp" {
		return c.alpEncode(p, g.Step)
	}
	if kind == "best-chimp" {
		base, err := c.EncodeCurrent(p, g.Step)
		if err != nil || base[1]&(sealedInteger|sealedDecimal) != 0 {
			return base, err
		}
		candidate, err := c.Encode("chimp128", g)
		if err != nil {
			return nil, err
		}
		if len(candidate) < len(base) {
			return candidate, nil
		}
		return base, nil
	}
	flags := byte(sealedImplicit)
	var body []byte
	for i := 1; i < len(p); i++ {
		if p[i].Timestamp-p[i-1].Timestamp != int64(g.Step) {
			flags = 0
			break
		}
	}
	if flags == 0 {
		for i := 1; i < len(p); i++ {
			body = binary.AppendUvarint(body, uint64(p[i].Timestamp-p[i-1].Timestamp))
		}
	}
	var values []byte
	switch kind {
	case "gorilla", "gorilla-zstd":
		values = frGorilla(p)
	case "chimp128":
		values = frChimp(p)
	case "xor-planes":
		values = frPlanes(p, true)
	case "dictionary":
		values = frDictionary(p)
	default:
		return nil, fmt.Errorf("unknown codec")
	}
	body = append(body, values...)
	// Private version 254, intentionally rejected by every durable reader.
	if kind == "gorilla" { // streaming baseline without an inner Zstd pass
		out := make([]byte, 16)
		out[1] = flags
		binary.LittleEndian.PutUint16(out[2:4], uint16(len(p)))
		binary.LittleEndian.PutUint32(out[4:8], uint32(p[0].Timestamp))
		binary.LittleEndian.PutUint32(out[8:12], uint32(g.Step))
		out = append(out, body...)
		out[0] = 254
		sum := crc32.Update(0, crc32.IEEETable, out[:12])
		binary.LittleEndian.PutUint32(out[12:16], crc32.Update(sum, crc32.IEEETable, body))
		return out, nil
	}
	return frALPBody(len(p), p[0].Timestamp, g.Step, flags, body, 254), nil
}
func (c Codec) Decode(kind string, data []byte) ([]Point, error) {
	if kind == "best-chimp" {
		if len(data) > 0 && data[0] == 3 {
			return c.DecodeCurrent(data)
		}
		return c.Decode("chimp128", data)
	}
	if kind == "sealed" || kind == "alp" && len(data) > 0 && data[0] == 3 {
		return c.DecodeCurrent(data)
	}
	if len(data) < 16 {
		return nil, fmt.Errorf("short header")
	}
	n := int(binary.LittleEndian.Uint16(data[2:4]))
	if n < 1 || n > MaxSealedPoints {
		return nil, fmt.Errorf("count")
	}
	body := data[16:]
	if data[1]&sealedZstd != 0 {
		limit := n * 20
		var frame zstd.Header
		if err := frame.Decode(body); err != nil {
			return nil, err
		}
		if frame.HasFCS {
			if frame.FrameContentSize > uint64(limit) {
				return nil, fmt.Errorf("expansion")
			}
			limit = int(frame.FrameContentSize)
		}
		z := sealedDecoders.Get().(*zstd.Decoder)
		var err error
		body, err = z.DecodeAll(body, make([]byte, 0, limit))
		sealedDecoders.Put(z)
		if err != nil {
			return nil, err
		}
	}
	header := [12]byte{}
	copy(header[:], data[:12])
	header[1] &^= sealedZstd
	sum := crc32.Update(0, crc32.IEEETable, header[:])
	if crc32.Update(sum, crc32.IEEETable, body) != binary.LittleEndian.Uint32(data[12:16]) {
		return nil, fmt.Errorf("checksum")
	}
	out := make([]Point, n)
	out[0].Timestamp = int64(binary.LittleEndian.Uint32(data[4:8]))
	step := int64(binary.LittleEndian.Uint32(data[8:12]))
	for i := 1; i < n; i++ {
		delta := step
		if data[1]&sealedImplicit == 0 {
			v, k := binary.Uvarint(body)
			if k <= 0 {
				return nil, fmt.Errorf("timestamps")
			}
			body = body[k:]
			delta = int64(v)
		}
		out[i].Timestamp = out[i-1].Timestamp + delta
	}
	if kind == "alp" {
		if err := frDecodeALP(body, out); err != nil {
			return nil, err
		}
		return out, nil
	}
	var vals []float64
	var err error
	switch kind {
	case "gorilla", "gorilla-zstd":
		vals, err = frGorillaDecode(body, n)
	case "chimp128":
		vals, err = frChimpDecode(body, n)
	case "xor-planes":
		vals, err = frPlanesDecode(body, n, true)
	case "dictionary":
		vals, err = frDictionaryDecode(body, n)
	}
	if err != nil {
		return nil, err
	}
	for i, v := range vals {
		out[i].Value = v
	}
	return out, nil
}
