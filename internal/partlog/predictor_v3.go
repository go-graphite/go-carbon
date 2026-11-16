package partlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/bits"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

// PDC3 epochs carry dictionary, timestamp deltas and Gorilla windows. Start a
// fresh encoder at each segment boundary. Independent makes every block its own
// epoch instead. The caller supplies complete, ordered apply groups.
const predictorV3MaxRaw = 64 << 20

var predictorV3Magic = []byte{'P', 'D', 'C', 3}
var predictorV3CRC = crc32.MakeTable(crc32.Castagnoli)
var errPredictorV3 = errors.New("invalid PDC3 journal block")

type predictorV3State struct {
	timestamp, delta  int64
	clock, clockDelta int64
	value             uint64
	leading, trailing uint8
	valid, window     bool
}

type predictorV3Entry struct {
	key        string
	definition []byte
	predictorV3State
}

// PredictorV3Encoder belongs to one stream owner. Encode preserves arbitrary
// int64 timestamps/clocks, exact IEEE bits, metadata and apply order. Close
// releases compressor memory. Point slices are not retained; dictionary metadata
// is copied and retained for the epoch. The input is never modified.
type PredictorV3Encoder struct {
	Independent  bool
	SeriesClocks bool
	// ClockDeltaOfDelta retains every clock bit; requires SeriesClocks.
	ClockDeltaOfDelta bool
	// Accounting optionally collects exact pre-zstd field bytes.
	Accounting  *PredictorV3Accounting
	encoder     *zstd.Encoder
	ids         map[string]int
	entries     []predictorV3Entry
	clock       int64
	sequence    uint64
	previousCRC uint32
}

func IsPredictorV3Record(b []byte) bool { return bytes.HasPrefix(b, predictorV3Magic) }

func (e *PredictorV3Encoder) Encode(fs []DictionaryFrame) ([]byte, error) {
	if e.ClockDeltaOfDelta && !e.SeriesClocks {
		return nil, errPredictorV3
	}
	if len(fs) == 0 || len(fs) > predictorV3MaxRaw/3 {
		return nil, errPredictorV3
	}
	reset := e.ids == nil || e.Independent
	if reset {
		e.ids = make(map[string]int)
		e.entries = []predictorV3Entry{{}}
		e.clock = 0
		e.sequence = 0
		e.previousCRC = 0
	}
	ids, entries := e.ids, e.entries
	meta := binary.AppendUvarint(nil, uint64(len(fs)))
	var account PredictorV3Accounting
	account.Framing += int64(len(meta))
	var values predictorV3Writer
	clock := e.clock
	for _, f := range fs {
		if f.Key == "" || len(f.Key) > predictorV3MaxRaw || len(f.Definition) > predictorV3MaxRaw || len(f.Points) == 0 || len(f.Points) > predictorV3MaxRaw {
			return nil, errPredictorV3
		}
		id, ok := ids[f.Key]
		if !ok {
			id = len(entries)
			ids[f.Key] = id
			entries = append(entries, predictorV3Entry{key: f.Key, definition: bytes.Clone(f.Definition)})
			start := len(meta)
			meta = binary.AppendUvarint(meta, uint64(id)<<1|1)
			account.IDs += int64(len(meta) - start)
			start = len(meta)
			meta = binary.AppendUvarint(meta, uint64(len(f.Key)))
			meta = append(meta, f.Key...)
			account.Names += int64(len(meta) - start)
			start = len(meta)
			meta = binary.AppendUvarint(meta, uint64(len(f.Definition)))
			meta = append(meta, f.Definition...)
			account.Definitions += int64(len(meta) - start)
		} else {
			if !bytes.Equal(entries[id].definition, f.Definition) {
				return nil, errPredictorV3
			}
			start := len(meta)
			meta = binary.AppendUvarint(meta, uint64(id)<<1)
			account.IDs += int64(len(meta) - start)
		}
		s := &entries[id].predictorV3State
		predictedClock := clock
		if e.SeriesClocks {
			predictedClock = s.clock
		}
		clockDelta := f.Clock - predictedClock
		encodedClock := clockDelta
		if e.ClockDeltaOfDelta {
			encodedClock -= s.clockDelta
		}
		s.clockDelta = clockDelta
		start := len(meta)
		meta = binary.AppendVarint(meta, encodedClock)
		account.Clocks += int64(len(meta) - start)
		s.clock = f.Clock
		clock = f.Clock
		start = len(meta)
		meta = binary.AppendUvarint(meta, uint64(len(f.Points)))
		account.Framing += int64(len(meta) - start)
		account.Points += int64(len(f.Points))
		for _, p := range f.Points {
			delta := p.Timestamp - s.timestamp
			start = len(meta)
			meta = binary.AppendVarint(meta, delta-s.delta)
			account.Timestamps += int64(len(meta) - start)
			s.timestamp = p.Timestamp
			s.delta = delta
			v := math.Float64bits(p.Value)
			if !s.valid {
				values.put(v, 64)
				s.valid = true
			} else {
				x := v ^ s.value
				if x == 0 {
					values.put(0, 1)
				} else {
					values.put(1, 1)
					l, t := uint8(bits.LeadingZeros64(x)), uint8(bits.TrailingZeros64(x))
					if s.window && l >= s.leading && t >= s.trailing && 64-int(s.leading)-int(s.trailing) <= 76-int(l)-int(t) {
						values.put(0, 1)
						values.put(x>>s.trailing, 64-int(s.leading)-int(s.trailing))
					} else {
						values.put(1, 1)
						values.put(uint64(l), 6)
						n := 64 - int(l) - int(t)
						values.put(uint64(n&63), 6)
						values.put(x>>t, n)
						s.leading = l
						s.trailing = t
						s.window = true
					}
				}
			}
			s.value = v
		}
		if len(meta)+len(values.b) > predictorV3MaxRaw-20 {
			return nil, errPredictorV3
		}
	}
	e.entries = entries
	e.clock = clock
	raw := binary.AppendUvarint(nil, uint64(len(meta)))
	raw = append(raw, meta...)
	raw = binary.AppendUvarint(raw, values.n)
	raw = append(raw, values.b...)
	account.Values = int64(len(values.b))
	account.Framing += int64(len(raw) - len(meta) - len(values.b))
	var err error
	if e.encoder == nil {
		e.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(1<<20), zstd.WithEncoderCRC(true))
		if err != nil {
			return nil, err
		}
	}
	b := append([]byte(nil), predictorV3Magic...)
	if reset {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	if e.SeriesClocks {
		b[4] |= 2
	}
	if e.ClockDeltaOfDelta {
		b[4] |= 4
	}
	b = binary.LittleEndian.AppendUint64(b, e.sequence)
	b = binary.LittleEndian.AppendUint32(b, e.previousCRC)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(raw)))
	b = e.encoder.EncodeAll(raw, b)
	e.previousCRC = crc32.Checksum(b, predictorV3CRC)
	e.sequence++
	account.Headers = 25 + 32 // PDC3 header/checksum plus outer journal header.
	account.Encoded = int64(len(b) + 4 + 32)
	if e.Accounting != nil {
		e.Accounting.add(account)
	}
	return binary.LittleEndian.AppendUint32(b, e.previousCRC), nil
}

func (e *PredictorV3Encoder) Close() {
	if e.encoder != nil {
		_ = e.encoder.Close()
		e.encoder = nil
	}
}

// PredictorV3Decoder accepts only PDC3; dispatch by discriminator at the journal
// boundary. Replay must begin with an epoch record. A failed Encode or Decode
// is fatal to its stream: discard that instance after an error.
type PredictorV3Decoder struct {
	decoder     *zstd.Decoder
	entries     []predictorV3Entry
	clock       int64
	sequence    uint64
	previousCRC uint32
}

func (d *PredictorV3Decoder) Decode(b []byte) ([]DictionaryFrame, error) {
	if len(b) < 25 || len(b) > predictorV3MaxRaw+1<<20 || !IsPredictorV3Record(b) || b[4] > 7 || b[4]&4 != 0 && b[4]&2 == 0 {
		return nil, errPredictorV3
	}
	n := int(binary.LittleEndian.Uint32(b[17:]))
	if n <= 0 || n > predictorV3MaxRaw || crc32.Checksum(b[:len(b)-4], predictorV3CRC) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, errPredictorV3
	}
	sequence, previousCRC := binary.LittleEndian.Uint64(b[5:]), binary.LittleEndian.Uint32(b[13:])
	if b[4]&1 != 0 {
		if sequence != 0 || previousCRC != 0 {
			return nil, errPredictorV3
		}
	} else if len(d.entries) == 0 || sequence != d.sequence || previousCRC != d.previousCRC {
		return nil, errPredictorV3
	}
	var err error
	if d.decoder == nil {
		d.decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(predictorV3MaxRaw), zstd.WithDecoderMaxWindow(1<<20), zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			return nil, err
		}
	}
	raw, err := d.decoder.DecodeAll(b[21:len(b)-4], make([]byte, 0, n))
	if err != nil {
		return nil, err
	}
	if len(raw) != n {
		return nil, errPredictorV3
	}
	r := dictionaryReader{b: raw}
	meta := r.bytes()
	nbits := r.u()
	if r.err != nil || nbits > uint64(len(r.b))*8 || (nbits+7)/8 != uint64(len(r.b)) {
		return nil, errPredictorV3
	}
	v := predictorV3Reader{b: r.b, n: nbits}
	r = dictionaryReader{b: meta}
	count := r.u()
	if count == 0 || count > uint64(len(meta))/3 {
		return nil, errPredictorV3
	}
	fs := make([]DictionaryFrame, 0, int(count))
	if b[4]&1 != 0 {
		d.entries = []predictorV3Entry{{}}
		d.clock = 0
	} else if len(d.entries) == 0 {
		return nil, errPredictorV3
	}
	entries := d.entries
	clock := d.clock
	for range count {
		tag := r.u()
		id := tag >> 1
		if tag&1 != 0 {
			key, definition := r.bytes(), r.bytes()
			if r.err != nil || id != uint64(len(entries)) || len(key) == 0 {
				return nil, errPredictorV3
			}
			entries = append(entries, predictorV3Entry{key: string(key), definition: bytes.Clone(definition)})
		}
		if r.err != nil || id == 0 || id >= uint64(len(entries)) {
			return nil, errPredictorV3
		}
		s := &entries[id]
		if b[4]&2 != 0 {
			clock = s.clock
		}
		clockDelta := r.i()
		if b[4]&4 != 0 {
			clockDelta += s.clockDelta
		}
		s.clockDelta = clockDelta
		clock += clockDelta
		s.clock = clock
		np := r.u()
		// At least one timestamp byte and one value bit exist per point.
		if r.err != nil || np == 0 || np > uint64(len(r.b)) || np > v.n-v.off {
			return nil, errPredictorV3
		}
		f := DictionaryFrame{Key: s.key, Definition: s.definition, Clock: clock, Points: make([]points.Point, int(np))}
		for i := range f.Points {
			s.delta += r.i()
			s.timestamp += s.delta
			var value uint64
			if !s.valid {
				value = v.get(64)
				s.valid = true
			} else {
				value = s.value
				if v.get(1) != 0 {
					if v.get(1) != 0 {
						s.leading = uint8(v.get(6))
						significant := int(v.get(6))
						if significant == 0 {
							significant = 64
						}
						if int(s.leading)+significant > 64 {
							return nil, errPredictorV3
						}
						s.trailing = uint8(64 - int(s.leading) - significant)
						s.window = true
					} else if !s.window {
						return nil, errPredictorV3
					}
					value ^= v.get(64-int(s.leading)-int(s.trailing)) << s.trailing
				}
			}
			s.value = value
			f.Points[i] = points.Point{Timestamp: s.timestamp, Value: math.Float64frombits(value)}
		}
		if r.err != nil || v.err {
			return nil, errPredictorV3
		}
		fs = append(fs, f)
	}
	if r.err != nil || len(r.b) != 0 || v.off != v.n || v.err {
		return nil, errPredictorV3
	}
	d.entries = entries
	d.clock = clock
	d.sequence = sequence + 1
	d.previousCRC = binary.LittleEndian.Uint32(b[len(b)-4:])
	return fs, nil
}

func (d *PredictorV3Decoder) Close() {
	if d.decoder != nil {
		d.decoder.Close()
		d.decoder = nil
	}
}

type predictorV3Writer struct {
	b []byte
	n uint64
}

func (w *predictorV3Writer) put(v uint64, n int) {
	for n > 0 {
		if w.n&7 == 0 {
			w.b = append(w.b, 0)
		}
		take := min(n, 8-int(w.n&7))
		shift := n - take
		w.b[len(w.b)-1] |= byte(v>>shift&((1<<take)-1)) << (8 - int(w.n&7) - take)
		w.n += uint64(take)
		n -= take
	}
}

type predictorV3Reader struct {
	b      []byte
	n, off uint64
	err    bool
}

func (r *predictorV3Reader) get(n int) uint64 {
	if n < 0 || n > 64 || uint64(n) > r.n-r.off {
		r.err = true
		return 0
	}
	var v uint64
	for n > 0 {
		take := min(n, 8-int(r.off&7))
		v = v<<take | uint64(r.b[r.off/8]>>(8-int(r.off&7)-take))&((1<<take)-1)
		r.off += uint64(take)
		n -= take
	}
	return v
}
