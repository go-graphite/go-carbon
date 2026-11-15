package partlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"github.com/go-graphite/go-carbon/points"
)

var dictionaryDeltaMagic = []byte{'P', 'D', 'X', 1}

// DictionaryDeltaEncoder predicts the exact value bits from the preceding
// point for the same name, then compresses the XORs with the existing bounded
// block codec. It preserves frame boundaries and all clocks, policies and bits.
// One encoder owns one ordered journal stream; failure is fatal to that stream.
type DictionaryDeltaEncoder struct {
	block    DictionaryBlockEncoder
	previous map[string]uint64
	frames   []DictionaryFrame
	points   []points.Point
}

func (e *DictionaryDeltaEncoder) Encode(fs []DictionaryFrame) ([]byte, error) {
	if len(fs) == 0 {
		return nil, errDictionary
	}
	n := 0
	for _, f := range fs {
		if len(f.Points) > maxPayload/9-n {
			return nil, errDictionary
		}
		n += len(f.Points)
	}
	reset := e.previous == nil
	if reset {
		e.previous = make(map[string]uint64)
	}
	if cap(e.frames) < len(fs) {
		e.frames = make([]DictionaryFrame, len(fs))
	} else {
		e.frames = e.frames[:len(fs)]
	}
	if cap(e.points) < n {
		e.points = make([]points.Point, n)
	} else {
		e.points = e.points[:n]
	}
	defer clear(e.frames)
	offset := 0
	for i, f := range fs {
		e.frames[i] = f
		x := e.points[offset : offset+len(f.Points) : offset+len(f.Points)]
		e.frames[i].Points = x
		previous := e.previous[f.Key]
		for j, p := range f.Points {
			bits := math.Float64bits(p.Value)
			x[j] = points.Point{Timestamp: p.Timestamp, Value: math.Float64frombits(bits ^ previous)}
			previous = bits
		}
		e.previous[f.Key] = previous
		offset += len(x)
	}
	b, err := e.block.Encode(e.frames)
	if err != nil {
		return nil, err
	}
	if len(b) > maxPayload-9 {
		return nil, errDictionary
	}
	out := append([]byte(nil), dictionaryDeltaMagic...)
	if reset {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = append(out, b...)
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out)), nil
}

func (e *DictionaryDeltaEncoder) Close() { e.block.Close() }

// DictionaryDeltaDecoder also accepts legacy PDC1/PDC2 records. Replay must
// start at an epoch boundary; trimming still requires dictionary checkpoints.
type DictionaryDeltaDecoder struct {
	block    DictionaryBlockDecoder
	previous map[string]uint64
}

func IsDictionaryDeltaRecord(b []byte) bool {
	return bytes.HasPrefix(b, dictionaryDeltaMagic) || IsDictionaryBlockRecord(b)
}

func (d *DictionaryDeltaDecoder) Decode(b []byte) ([]DictionaryFrame, error) {
	if !bytes.HasPrefix(b, dictionaryDeltaMagic) {
		d.previous = nil
		return d.block.Decode(b)
	}
	if len(b) < 14 || len(b) > maxPayload || b[4] > 1 {
		return nil, errDictionary
	}
	body := b[:len(b)-4]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, errors.New("journal predictor checksum mismatch")
	}
	if b[4] == 1 {
		d.previous = make(map[string]uint64)
	} else if d.previous == nil {
		return nil, errors.New("missing journal predictor epoch")
	}
	fs, err := d.block.Decode(body[5:])
	if err != nil {
		return nil, err
	}
	for _, f := range fs {
		previous := d.previous[f.Key]
		for i := range f.Points {
			bits := math.Float64bits(f.Points[i].Value) ^ previous
			f.Points[i].Value = math.Float64frombits(bits)
			previous = bits
		}
		d.previous[f.Key] = previous
	}
	return fs, nil
}

func (d *DictionaryDeltaDecoder) Close() { d.block.Close() }
