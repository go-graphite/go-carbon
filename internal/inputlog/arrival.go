package inputlog

import (
	"encoding/binary"
	"errors"
)

// Arrival metadata uses a zstd skippable frame: older readers ignore it and
// decode identical point bytes. The ordinary input-log payload CRC covers it.
const arrivalMagic = 0x184d2a50

func appendArrivalFrame(dst, ages []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, arrivalMagic)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(ages)+12))
	dst = append(dst, "AGE1"...)
	dst = append(dst, ages...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(ages)+12))
	return append(dst, "AGE!"...)
}

func arrivalFrame(payload []byte) ([]int64, error) {
	if len(payload) < 20 || string(payload[len(payload)-4:]) != "AGE!" {
		return nil, nil
	}
	n := int(binary.LittleEndian.Uint32(payload[len(payload)-8:]))
	if n < 12 || n > len(payload)-8 {
		return nil, errors.New("invalid arrival frame length")
	}
	start := len(payload) - n - 8
	if binary.LittleEndian.Uint32(payload[start:]) != arrivalMagic || int(binary.LittleEndian.Uint32(payload[start+4:])) != n || string(payload[start+8:start+12]) != "AGE1" {
		return nil, errors.New("invalid arrival frame")
	}
	raw := payload[start+12 : len(payload)-8]
	var ages []int64
	for len(raw) > 0 {
		at, k := binary.Varint(raw)
		if k <= 0 {
			return nil, errors.New("invalid arrival timestamp")
		}
		ages = append(ages, at)
		raw = raw[k:]
	}
	return ages, nil
}
