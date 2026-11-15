package partlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/go-graphite/go-carbon/points"
)

// DictionaryFrame preserves one original apply boundary. Definition is opaque
// policy/placement metadata. Key and Definition must remain stable in an epoch.
type DictionaryFrame struct {
	Key        string
	Definition []byte
	Clock      int64
	Points     []points.Point
}

type dictionaryEntry struct {
	key        string
	definition []byte
}

// DictionaryEncoder belongs to one journal stream owner. The first record of
// each encoder starts an epoch. Definitions precede references in the SAME
// checksummed journal record, so no durable reference can outlive its definition.
// Append failure must fail the stream: encoder state is advanced by Encode.
// Journal retirement requires a dictionary checkpoint or an epoch boundary.
type DictionaryEncoder struct {
	ids         map[string]uint64
	definitions map[string][]byte
}

var dictionaryMagic = []byte{'P', 'D', 'C', 1}
var errDictionary = errors.New("invalid journal dictionary record")

func IsDictionaryRecord(b []byte) bool { return bytes.HasPrefix(b, dictionaryMagic) }

func (e *DictionaryEncoder) Encode(fs []DictionaryFrame) ([]byte, error) {
	if len(fs) == 0 {
		return nil, errDictionary
	}
	reset := e.ids == nil
	if reset {
		e.ids = make(map[string]uint64)
		e.definitions = make(map[string][]byte)
	}
	b := append([]byte(nil), dictionaryMagic...)
	if reset {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(len(fs)))
	b = binary.AppendVarint(b, fs[0].Clock)
	for _, f := range fs {
		if f.Key == "" || len(f.Points) == 0 || len(f.Key) > maxPayload || len(f.Definition) > maxPayload {
			return nil, errDictionary
		}
		id, ok := e.ids[f.Key]
		if !ok {
			id = uint64(len(e.ids) + 1)
			e.ids[f.Key] = id
			e.definitions[f.Key] = bytes.Clone(f.Definition)
			b = append(b, 0) // definition tag; references are positive
			b = binary.AppendUvarint(b, id)
			b = binary.AppendUvarint(b, uint64(len(f.Key)))
			b = append(b, f.Key...)
			b = binary.AppendUvarint(b, uint64(len(f.Definition)))
			b = append(b, f.Definition...)
		} else if !bytes.Equal(e.definitions[f.Key], f.Definition) {
			return nil, errors.New("journal dictionary definition changed")
		}
		b = binary.AppendUvarint(b, id)
		b = binary.AppendVarint(b, f.Clock-fs[0].Clock)
		b = binary.AppendUvarint(b, uint64(len(f.Points)))
		stamp := f.Clock
		for _, p := range f.Points {
			if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 {
				return nil, errDictionary
			}
			b = binary.AppendVarint(b, p.Timestamp-stamp)
			stamp = p.Timestamp
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(p.Value))
		}
		if len(b) > maxPayload {
			return nil, errors.New("dictionary record too large")
		}
	}
	return b, nil
}

// DictionaryDecoder must consume records from the start of an epoch, even if
// the consumer has already checkpointed their applies. Recreate it on restart.
// A decode error is fatal; its partially advanced state must not be reused.
type DictionaryDecoder struct{ entries []dictionaryEntry }

func (d *DictionaryDecoder) Decode(b []byte) ([]DictionaryFrame, error) {
	if len(b) < 5 || !IsDictionaryRecord(b) || b[4] > 1 {
		return nil, errDictionary
	}
	if b[4] == 1 {
		d.entries = []dictionaryEntry{{}}
	} else if len(d.entries) == 0 {
		return nil, errors.New("missing journal dictionary epoch")
	}
	r := dictionaryReader{b: b[5:]}
	n := r.u()
	clock := r.i()
	if n == 0 || n > uint64(len(r.b))/3 {
		return nil, errDictionary
	}
	fs := make([]DictionaryFrame, 0, int(n))
	for range n {
		id := r.u()
		if id == 0 {
			id = r.u()
			key := r.bytes()
			definition := r.bytes()
			if r.err != nil || id != uint64(len(d.entries)) || len(key) == 0 {
				return nil, errDictionary
			}
			d.entries = append(d.entries, dictionaryEntry{string(key), bytes.Clone(definition)})
			id = r.u()
		}
		if id == 0 || id >= uint64(len(d.entries)) {
			return nil, errDictionary
		}
		entry := d.entries[id]
		f := DictionaryFrame{Key: entry.key, Definition: entry.definition, Clock: clock + r.i()}
		count := r.u()
		if r.err != nil || count == 0 || count > uint64(len(r.b))/9 {
			return nil, errDictionary
		}
		f.Points = make([]points.Point, int(count))
		stamp := f.Clock
		for i := range f.Points {
			stamp += r.i()
			if r.err != nil || len(r.b) < 8 || stamp <= 0 || stamp > math.MaxUint32 {
				return nil, errDictionary
			}
			f.Points[i] = points.Point{Timestamp: stamp, Value: math.Float64frombits(binary.LittleEndian.Uint64(r.b))}
			r.b = r.b[8:]
		}
		fs = append(fs, f)
	}
	if r.err != nil || len(r.b) != 0 {
		return nil, errDictionary
	}
	return fs, nil
}

type dictionaryReader struct {
	b   []byte
	err error
}

func (r *dictionaryReader) u() uint64 {
	n, k := binary.Uvarint(r.b)
	if k <= 0 {
		r.err = errDictionary
		return 0
	}
	r.b = r.b[k:]
	return n
}
func (r *dictionaryReader) i() int64 {
	n, k := binary.Varint(r.b)
	if k <= 0 {
		r.err = errDictionary
		return 0
	}
	r.b = r.b[k:]
	return n
}
func (r *dictionaryReader) bytes() []byte {
	n := r.u()
	if r.err != nil || n > uint64(len(r.b)) {
		r.err = errDictionary
		return nil
	}
	b := r.b[:int(n)]
	r.b = r.b[int(n):]
	return b
}
