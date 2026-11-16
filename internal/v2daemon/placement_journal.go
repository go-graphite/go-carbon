package v2daemon

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"

	"github.com/go-graphite/go-carbon/internal/v2placement"
)

var placementMagic = []byte{'V', '2', 'P', 1}

// The outer checksum binds the exact admitted authority and producer lane to
// the original application packet. Replay never substitutes the current map.
func encodePlacementEnvelope(stamp v2placement.Stamp, applications []byte) []byte {
	metadata, _ := json.Marshal(stamp)
	b := append([]byte(nil), placementMagic...)
	b = binary.AppendUvarint(b, uint64(len(metadata)))
	b = append(b, metadata...)
	b = append(b, applications...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, applicationCRC))
}

func decodePlacementEnvelope(record []byte) ([]byte, *v2placement.Stamp, error) {
	if len(record) < 4 || !bytes.Equal(record[:4], placementMagic) {
		return record, nil, nil
	}
	invalid := errors.New("invalid placed application envelope")
	if len(record) < 10 || crc32.Checksum(record[:len(record)-4], applicationCRC) != binary.LittleEndian.Uint32(record[len(record)-4:]) {
		return nil, nil, invalid
	}
	b := record[4 : len(record)-4]
	size, n := binary.Uvarint(b)
	if n <= 0 || size == 0 || size >= uint64(len(b)-n) {
		return nil, nil, invalid
	}
	b = b[n:]
	metadata := b[:int(size)]
	var stamp v2placement.Stamp
	if json.Unmarshal(metadata, &stamp) != nil {
		return nil, nil, invalid
	}
	canonical, err := json.Marshal(stamp)
	if err != nil || !bytes.Equal(metadata, canonical) {
		return nil, nil, invalid
	}
	inner := b[int(size):]
	if len(inner) < 4 || !bytes.Equal(inner[:4], applicationMagic) {
		return nil, nil, invalid
	}
	return inner, &stamp, nil
}
