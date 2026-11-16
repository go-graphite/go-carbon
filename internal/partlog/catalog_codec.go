package partlog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/bits"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

// Catalog records carry stable, delta-coded catalog IDs and independent predictor
// epochs. A new encoder per segment permits trimming preceding segments.

var catalogCodecMagic = []byte{'P', 'D', 'C', 4}

// CatalogEncoder belongs to one stream owner. Encode preserves arbitrary
// int64 timestamps/clocks, exact IEEE bits, metadata and apply order. Close
// releases compressor memory. Point slices are not retained; dictionary metadata
// is persisted in the catalog; predictors are retained for the segment epoch. The input is never modified.
type CatalogEncoder struct {
	Catalog      *NameCatalog
	Independent  bool
	SeriesClocks bool
	// SparseClocks uses a presence bitmap for zero arrival-clock deltas.
	SparseClocks bool
	// Accounting optionally collects exact pre-zstd field bytes.
	Accounting  *PredictorV3Accounting
	encoder     *zstd.Encoder
	entries     map[uint64]*predictorV3Entry
	clock       int64
	sequence    uint64
	previousCRC uint32
}

func IsCatalogRecord(b []byte) bool { return bytes.HasPrefix(b, catalogCodecMagic) }

func (e *CatalogEncoder) Encode(fs []DictionaryFrame) ([]byte, error) {
	if len(fs) == 0 || len(fs) > predictorV3MaxRaw/3 {
		return nil, errPredictorV3
	}
	if e.Catalog == nil {
		return nil, errPredictorV3
	}
	reset := e.entries == nil || e.Independent
	if reset {
		e.entries = make(map[uint64]*predictorV3Entry)
		e.clock = 0
		e.sequence = 0
		e.previousCRC = 0
	}
	entries := e.entries
	var previousID int64
	meta := binary.AppendUvarint(nil, uint64(len(fs)))
	var account PredictorV3Accounting
	account.Framing += int64(len(meta))
	var values, clocks predictorV3Writer
	clock := e.clock
	for _, f := range fs {
		if f.Key == "" || len(f.Key) > predictorV3MaxRaw || len(f.Definition) > predictorV3MaxRaw || len(f.Points) == 0 || len(f.Points) > predictorV3MaxRaw {
			return nil, errPredictorV3
		}
		id, err := e.Catalog.Intern(f.Key, f.Definition)
		if err != nil {
			return nil, err
		}
		start := len(meta)
		meta = binary.AppendVarint(meta, int64(id)-previousID)
		previousID = int64(id)
		account.IDs += int64(len(meta) - start)
		entry := entries[id]
		if entry == nil {
			entry = &predictorV3Entry{}
			entries[id] = entry
		}
		s := &entry.predictorV3State
		predictedClock := clock
		if e.SeriesClocks {
			predictedClock = s.clock
		}
		clockDelta := f.Clock - predictedClock
		encodedClock := clockDelta
		start = len(meta)
		if e.SparseClocks {
			if encodedClock != 0 {
				clocks.put(1, 1)
				meta = binary.AppendVarint(meta, encodedClock)
			} else {
				clocks.put(0, 1)
			}
		} else {
			meta = binary.AppendVarint(meta, encodedClock)
		}
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
		if len(meta)+len(values.b)+len(clocks.b) > predictorV3MaxRaw-20 {
			return nil, errPredictorV3
		}
	}
	e.entries = entries
	e.clock = clock
	raw := binary.AppendUvarint(nil, uint64(len(meta)))
	raw = append(raw, meta...)
	if e.SparseClocks {
		raw = binary.AppendUvarint(raw, clocks.n)
		raw = append(raw, clocks.b...)
		account.Clocks += int64(len(clocks.b))
	}
	raw = binary.AppendUvarint(raw, values.n)
	raw = append(raw, values.b...)
	account.Values = int64(len(values.b))
	account.Framing += int64(len(raw) - len(meta) - len(values.b) - len(clocks.b))
	var err error
	if e.encoder == nil {
		e.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(1<<20), zstd.WithEncoderCRC(true))
		if err != nil {
			return nil, err
		}
	}
	b := append([]byte(nil), catalogCodecMagic...)
	if reset {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	if e.SeriesClocks {
		b[4] |= 2
	}
	if e.SparseClocks {
		b[4] |= 8
	}
	b = binary.LittleEndian.AppendUint64(b, e.sequence)
	b = binary.LittleEndian.AppendUint32(b, e.previousCRC)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(raw)))
	b = append(b, e.Catalog.epoch[:]...)
	b = e.encoder.EncodeAll(raw, b)
	e.previousCRC = crc32.Checksum(b, predictorV3CRC)
	e.sequence++
	account.Headers = 41 + 32 // catalog-backed PDC4 header/checksum plus outer journal header.
	account.Encoded = int64(len(b) + 4 + 32)
	if e.Accounting != nil {
		e.Accounting.add(account)
	}
	return binary.LittleEndian.AppendUint32(b, e.previousCRC), nil
}

func (e *CatalogEncoder) Close() {
	if e.encoder != nil {
		_ = e.encoder.Close()
		e.encoder = nil
	}
}

// CatalogDecoder accepts only catalog-backed PDC4; dispatch by discriminator at the journal
// boundary. Replay must begin with an epoch record. A failed Encode or Decode
// is fatal to its stream: discard that instance after an error.
type CatalogDecoder struct {
	Catalog     *NameCatalog
	decoder     *zstd.Decoder
	entries     map[uint64]*predictorV3Entry
	clock       int64
	sequence    uint64
	previousCRC uint32
}

func (d *CatalogDecoder) Decode(b []byte) ([]DictionaryFrame, error) {
	if d.Catalog == nil || len(b) < 41 || len(b) > predictorV3MaxRaw+1<<20 || !IsCatalogRecord(b) || b[4]&^byte(1|2|8) != 0 {
		return nil, errPredictorV3
	}
	if !bytes.Equal(b[21:37], d.Catalog.epoch[:]) {
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
	} else if d.entries == nil || sequence != d.sequence || previousCRC != d.previousCRC {
		return nil, errPredictorV3
	}
	var err error
	if d.decoder == nil {
		d.decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(predictorV3MaxRaw), zstd.WithDecoderMaxWindow(1<<20), zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			return nil, err
		}
	}
	raw, err := d.decoder.DecodeAll(b[37:len(b)-4], make([]byte, 0, n))
	if err != nil {
		return nil, err
	}
	if len(raw) != n {
		return nil, errPredictorV3
	}
	r := dictionaryReader{b: raw}
	meta := r.bytes()
	var clocks predictorV3Reader
	if b[4]&8 != 0 {
		clocks.n = r.u()
		size := (clocks.n + 7) / 8
		if r.err != nil || size > uint64(len(r.b)) || clocks.n == 0 {
			return nil, errPredictorV3
		}
		clocks.b = r.b[:size]
		r.b = r.b[size:]
	}
	nbits := r.u()
	if r.err != nil || nbits > uint64(len(r.b))*8 || (nbits+7)/8 != uint64(len(r.b)) {
		return nil, errPredictorV3
	}
	v := predictorV3Reader{b: r.b, n: nbits}
	r = dictionaryReader{b: meta}
	count := r.u()
	if count == 0 || count > uint64(len(meta))/2 || b[4]&8 != 0 && clocks.n != count {
		return nil, errPredictorV3
	}
	fs := make([]DictionaryFrame, 0, int(count))
	if b[4]&1 != 0 {
		d.entries = make(map[uint64]*predictorV3Entry)
		d.clock = 0
	} else if d.entries == nil {
		return nil, errPredictorV3
	}
	entries := d.entries
	clock := d.clock
	var previousID int64
	for range count {
		delta := r.i()
		id := uint64(previousID + delta)
		previousID = int64(id)
		key, definition, ok := d.Catalog.Lookup(id)
		if r.err != nil || !ok {
			return nil, errPredictorV3
		}
		s := entries[id]
		if s == nil {
			s = &predictorV3Entry{key: key, definition: definition}
			entries[id] = s
		}
		if b[4]&2 != 0 {
			clock = s.clock
		}
		var clockDelta int64
		if b[4]&8 == 0 || clocks.get(1) != 0 {
			clockDelta = r.i()
		}
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
	if r.err != nil || len(r.b) != 0 || v.off != v.n || v.err || b[4]&8 != 0 && (clocks.off != clocks.n || clocks.err) {
		return nil, errPredictorV3
	}
	d.entries = entries
	d.clock = clock
	d.sequence = sequence + 1
	d.previousCRC = binary.LittleEndian.Uint32(b[len(b)-4:])
	return fs, nil
}

func (d *CatalogDecoder) Close() {
	if d.decoder != nil {
		d.decoder.Close()
		d.decoder = nil
	}
}

// EncodeDurable batches any new definitions into one catalog fsync, before the
// caller appends the returned segment record. A clean catalog incurs no fsync.
// The journal must still durably sync its record before acknowledging points.
func (e *CatalogEncoder) EncodeDurable(fs []DictionaryFrame) ([]byte, error) {
	b, err := e.Encode(fs)
	if err != nil {
		return nil, err
	}
	if err = e.Catalog.Sync(); err != nil {
		return nil, err
	}
	return b, nil
}
