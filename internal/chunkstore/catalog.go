package chunkstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

const catalogHeaderSize = 29

// Exact stores keep names in catalog keys and immutable policy in a compact
// value. Revisions and activity remain in their existing transactional record.
// This avoids JSON parsing on every read/write without retaining a second
// in-memory catalog. Legacy stores retain their existing JSON representation.
func (s *Store) encodeCatalog(m Metadata) ([]byte, error) {
	// This precedes every catalog publication, including snapshot replacement
	// and migration staging. Encoding errors merely leave extra positive bits.
	s.catalogMembership.add(m.Name)
	if m.Continuation && s.continuation == nil {
		return nil, ErrFormat
	}
	if m.FileMigration > FileReclaimed || (!s.exactBlocks || s.incrementalSource == "") && m.FileMigration != FileUnowned {
		return nil, errors.New("invalid file-migration catalog state")
	}
	if !s.exactBlocks {
		return json.Marshal(m)
	}
	b := make([]byte, catalogHeaderSize+8*len(m.Retentions))
	b[0] = 1 + byte(m.FileMigration)
	if m.Continuation {
		b[0] |= 0x80
	}
	binary.LittleEndian.PutUint64(b[1:], m.ID)
	binary.LittleEndian.PutUint64(b[9:], m.Generation)
	binary.LittleEndian.PutUint32(b[17:], uint32(m.AggregationMethod))
	binary.LittleEndian.PutUint32(b[21:], math.Float32bits(m.XFilesFactor))
	binary.LittleEndian.PutUint32(b[25:], uint32(len(m.Retentions)))
	for i, r := range m.Retentions {
		binary.LittleEndian.PutUint32(b[catalogHeaderSize+8*i:], uint32(r.Step))
		binary.LittleEndian.PutUint32(b[catalogHeaderSize+8*i+4:], uint32(r.Count))
	}
	return b, nil
}

func decodeCatalog(name string, b []byte) (Metadata, error) {
	if len(b) == 0 || b[0]&0x7f < 1 || b[0]&0x7f > 3 {
		var m Metadata
		if err := json.Unmarshal(b, &m); err != nil {
			return Metadata{}, err
		}
		return m, nil
	}
	if len(b) < catalogHeaderSize || (len(b)-catalogHeaderSize)%8 != 0 ||
		uint64(binary.LittleEndian.Uint32(b[25:])) != uint64((len(b)-catalogHeaderSize)/8) {
		return Metadata{}, errors.New("invalid binary catalog size")
	}
	var m Metadata
	m.FileMigration = FileMigrationState((b[0] & 0x7f) - 1)
	m.Continuation = b[0]&0x80 != 0
	m.Name = name
	m.ID = binary.LittleEndian.Uint64(b[1:])
	m.Generation = binary.LittleEndian.Uint64(b[9:])
	m.AggregationMethod = AggregationMethod(binary.LittleEndian.Uint32(b[17:]))
	m.XFilesFactor = math.Float32frombits(binary.LittleEndian.Uint32(b[21:]))
	m.Retentions = make([]Retention, (len(b)-catalogHeaderSize)/8)
	for i := range m.Retentions {
		m.Retentions[i] = Retention{
			Step:  int(binary.LittleEndian.Uint32(b[catalogHeaderSize+8*i:])),
			Count: int(binary.LittleEndian.Uint32(b[catalogHeaderSize+8*i+4:])),
		}
	}
	if m.ID == 0 || m.Generation == 0 {
		return Metadata{}, errors.New("invalid binary catalog identity")
	}
	if err := validate(m.MetricConfig); err != nil {
		return Metadata{}, err
	}
	return m, nil
}
