package v2core

import (
	"encoding/binary"
	"errors"

	"github.com/go-graphite/go-carbon/internal/partsrun"
)

// The raw archive field is a persisted codec discriminator: 0 is legacy
// triples, 1 is this versioned multi-metric run. Blocks never cross partitions.
// Numeric identities remain local to the offline experiment; this is not the
// portable placement-group/name key contract.
const coreCompactRows = 4096

type coreCompactWorkspace struct {
	run     partsrun.Workspace
	encoded []byte
}

func (w *coreCompactWorkspace) encode(rows []row) ([]byte, error) {
	if len(rows) == 0 || len(rows) > coreCompactRows {
		return nil, errors.New("invalid core compact row count")
	}
	b := w.encoded[:0]
	b = append(b, 'G', 'C', 'R', 1, 0, 0, 0, 0)
	var previous uint64
	for _, r := range rows {
		b = binary.AppendUvarint(b, r.ID-previous)
		previous = r.ID
	}
	binary.BigEndian.PutUint32(b[4:8], uint32(len(b)-8))
	var err error
	b, err = partsrun.AppendWithWorkspace(b, rows[0].Time, len(rows), func(i int) partsrun.Sample {
		r := rows[i]
		return partsrun.Sample{Time: r.Time, Bits: r.Bits, Sequence: r.Seq}
	}, &w.run)
	w.encoded = b
	return b, err
}
func (w *coreCompactWorkspace) visit(b []byte, visit func(row) error) error {
	if len(b) < 8 || string(b[:4]) != "GCR\x01" {
		return errors.New("invalid core compact header")
	}
	size := uint64(binary.BigEndian.Uint32(b[4:8]))
	if size > uint64(len(b)-8) || size > 10*coreCompactRows {
		return errors.New("invalid core compact identities")
	}
	ids := b[8 : 8+int(size)]
	var id uint64
	err := partsrun.VisitWithWorkspace(b[8+int(size):], coreCompactRows, func(p partsrun.Sample) error {
		delta, n := binary.Uvarint(ids)
		if n <= 0 {
			return errors.New("invalid core compact identity")
		}
		ids = ids[n:]
		id += delta
		return visit(row{ID: id, Time: p.Time, Bits: p.Bits, Seq: p.Sequence})
	}, &w.run)
	if err != nil {
		return err
	}
	if len(ids) != 0 {
		return errors.New("trailing core compact identities")
	}
	return nil
}

// RawDecoder decodes both numeric raw formats with reusable bounded scratch.
// One caller owns each decoder. Keys/values remain borrowed and do not escape.
// It is not a decoder for the portable production format.
type RawDecoder struct{ compact coreCompactWorkspace }

func (d *RawDecoder) Visit(key, value []byte, visit func(Row) error) error {
	if len(key) != 41 || (key[0] != 1 && key[0] != 2) || visit == nil {
		return errors.New("invalid core raw key/visitor")
	}
	idAt, codecAt := 9, 17
	if key[0] == 2 {
		idAt, codecAt = 17, 25
	}
	switch binary.BigEndian.Uint32(key[codecAt : codecAt+4]) {
	case 0:
		if len(value) == 0 || len(value)%24 != 0 {
			return errors.New("invalid core raw triples")
		}
		id := binary.BigEndian.Uint64(key[idAt : idAt+8])
		for len(value) > 0 {
			if err := visit(row{ID: id, Time: int64(binary.LittleEndian.Uint64(value)), Bits: binary.LittleEndian.Uint64(value[8:]), Seq: binary.LittleEndian.Uint64(value[16:])}); err != nil {
				return err
			}
			value = value[24:]
		}
		return nil
	case 1:
		return d.compact.visit(value, visit)
	default:
		return errors.New("unknown core raw codec")
	}
}
