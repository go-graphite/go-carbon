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

// catalogPlain flags predictor bytes stored directly, without a zstd pass.
// The decoder retains support for compressed PDC4 records.
const catalogPlain byte = 16

var catalogCodecMagic = []byte{'P', 'D', 'C', 4}

// CatalogEncoder belongs to one stream owner. Encode preserves arbitrary
// int64 timestamps/clocks, exact IEEE bits, metadata and apply order. Point slices
// are not retained; dictionary metadata is persisted in the catalog; predictors
// are retained for the segment epoch. The input is never modified.
type CatalogEncoder struct {
	Catalog      *NameCatalog
	Independent  bool
	SeriesClocks bool
	// SparseClocks uses a presence bitmap for zero arrival-clock deltas.
	SparseClocks bool
	// Accounting optionally collects exact pre-zstd field bytes.
	Accounting                *PredictorV3Accounting
	pages                     []*[1024]predictorV3State
	meta, raw, values, clocks []byte
	clock                     int64
	sequence                  uint64
	previousCRC               uint32
}

func IsCatalogRecord(b []byte) bool { return bytes.HasPrefix(b, catalogCodecMagic) }

// CatalogFrame carries a previously interned stream-local numeric identity.
// Names and immutable definitions were validated by NameCatalog.Intern at
// admission; only the ID range and application payload need checking here.
type CatalogFrame struct {
	ID     uint64
	Clock  int64
	Points []points.Point
}

func (e *CatalogEncoder) EncodeFrames(fs []CatalogFrame) ([]byte, error) {
	return e.encode(nil, nil, nil, 0, fs)
}

func (e *CatalogEncoder) Encode(fs []DictionaryFrame) ([]byte, error) {
	return e.encode(fs, nil, nil, 0, nil)
}

// EncodeIDs accepts IDs already resolved in this encoder's catalog. It validates
// each ID's name and definition without hashing the name again. Zero requests
// normal interning, for example for new names mixed with resolved input frames.
// IDs are stream-local handles, never routing or replica identities. Durability
// ordering is identical to Encode: sync the catalog before its journal records.
func (e *CatalogEncoder) EncodeIDs(fs []DictionaryFrame, ids []uint64) ([]byte, error) {
	if len(ids) != len(fs) {
		return nil, errPredictorV3
	}
	return e.encode(fs, ids, nil, 0, nil)
}

// EncodePoints journals one point per already-resolved catalog ID at a common
// arrival clock. It borrows the input arrays for the call, avoiding transient
// name/definition/point-slice frames. It uses the same PDC4 encoder and produces
// the same bytes as equivalent singleton DictionaryFrames passed to EncodeIDs.
// IDs must belong to Catalog; catalog sync must precede journal durability.
func (e *CatalogEncoder) EncodePoints(ids []uint64, data []points.Point, clock int64) ([]byte, error) {
	if len(ids) != len(data) || len(data) == 0 {
		return nil, errPredictorV3
	}
	return e.encode(nil, ids, data, clock, nil)
}

func (e *CatalogEncoder) encode(fs []DictionaryFrame, ids []uint64, data []points.Point, arrivalClock int64, resolved []CatalogFrame) ([]byte, error) {
	count := len(fs)
	if resolved != nil {
		count = len(resolved)
	}
	if data != nil {
		count = len(data)
	}
	if count == 0 || count > predictorV3MaxRaw/3 {
		return nil, errPredictorV3
	}
	if e.Catalog == nil {
		return nil, errPredictorV3
	}
	if e.Catalog.failed != nil {
		return nil, e.Catalog.failed
	}
	reset := e.pages == nil || e.Independent
	if reset {
		e.pages = make([]*[1024]predictorV3State, 1)
		e.clock = 0
		e.sequence = 0
		e.previousCRC = 0
	}
	var previousID int64
	meta := binary.AppendUvarint(e.meta[:0], uint64(count))
	var account PredictorV3Accounting
	account.Framing += int64(len(meta))
	values := predictorV3Writer{b: e.values[:0]}
	clocks := predictorV3Writer{b: e.clocks[:0]}
	clock := e.clock
	for frame := 0; frame < count; frame++ {
		var f DictionaryFrame
		var id uint64
		if resolved != nil {
			r := resolved[frame]
			id = r.ID
			if id == 0 || id > e.Catalog.Count() || len(r.Points) == 0 || len(r.Points) > predictorV3MaxRaw {
				return nil, errCatalog
			}
			f.Clock, f.Points = r.Clock, r.Points
		} else if data != nil {
			id = ids[frame]
			if _, _, ok := e.Catalog.Lookup(id); !ok {
				return nil, errCatalog
			}
			f.Clock, f.Points = arrivalClock, data[frame:frame+1]
		} else {
			f = fs[frame]
			if f.Key == "" || len(f.Key) > predictorV3MaxRaw || len(f.Definition) > predictorV3MaxRaw || len(f.Points) == 0 || len(f.Points) > predictorV3MaxRaw {
				return nil, errPredictorV3
			}
			if ids != nil {
				id = ids[frame]
			}
			if id == 0 {
				var err error
				id, err = e.Catalog.Intern(f.Key, f.Definition)
				if err != nil {
					return nil, err
				}
			} else {
				key, definition, ok := e.Catalog.Lookup(id)
				if !ok || key != f.Key || !bytes.Equal(definition, f.Definition) {
					return nil, errCatalog
				}
			}
		}
		start := len(meta)
		meta = binary.AppendVarint(meta, int64(id)-previousID)
		previousID = int64(id)
		account.IDs += int64(len(meta) - start)
		// Catalog IDs are dense and stream-local. Pointer-free pages avoid one
		// map lookup and heap object per series without allocating untouched pages.
		page := int(id / 1024)
		for len(e.pages) <= page {
			e.pages = append(e.pages, nil)
		}
		if e.pages[page] == nil {
			e.pages[page] = new([1024]predictorV3State)
		}
		s := &e.pages[page][id%1024]
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
	e.clock = clock
	raw := binary.AppendUvarint(e.raw[:0], uint64(len(meta)))
	raw = append(raw, meta...)
	if e.SparseClocks {
		raw = binary.AppendUvarint(raw, clocks.n)
		raw = append(raw, clocks.b...)
		account.Clocks += int64(len(clocks.b))
	}
	raw = binary.AppendUvarint(raw, values.n)
	raw = append(raw, values.b...)
	e.meta, e.raw, e.values, e.clocks = meta, raw, values.b, clocks.b
	account.Values = int64(len(values.b))
	account.Framing += int64(len(raw) - len(meta) - len(values.b) - len(clocks.b))
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
	b[4] |= catalogPlain
	b = append(b, raw...)
	e.previousCRC = crc32.Checksum(b, predictorV3CRC)
	e.sequence++
	account.Headers = 41 + 32 // catalog-backed PDC4 header/checksum plus outer journal header.
	account.Encoded = int64(len(b) + 4 + 32)
	if e.Accounting != nil {
		e.Accounting.add(account)
	}
	return binary.LittleEndian.AppendUint32(b, e.previousCRC), nil
}

// Close is retained for codec callers; the plain encoder owns no compressor.
func (e *CatalogEncoder) Close() {}

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
	// raw is reused per record: decoded frames never reference it.
	raw []byte
}

func (d *CatalogDecoder) Decode(b []byte) ([]DictionaryFrame, error) {
	if d.Catalog == nil || len(b) < 41 || len(b) > predictorV3MaxRaw+1<<20 || !IsCatalogRecord(b) || b[4]&^byte(1|2|8|catalogPlain) != 0 {
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
	raw := b[37 : len(b)-4]
	if b[4]&catalogPlain == 0 {
		var err error
		if d.decoder == nil {
			d.decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(predictorV3MaxRaw), zstd.WithDecoderMaxWindow(1<<20), zstd.WithDecodeAllCapLimit(true))
			if err != nil {
				return nil, err
			}
		}
		if cap(d.raw) < n {
			d.raw = make([]byte, 0, n)
		}
		raw, err = d.decoder.DecodeAll(raw, d.raw[:0])
		if err != nil {
			return nil, err
		}
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
		if r.err != nil {
			return nil, errPredictorV3
		}
		// Bindings are immutable: resolve each ID once per segment.
		s := entries[id]
		if s == nil {
			key, definition, ok := d.Catalog.Lookup(id)
			if !ok {
				return nil, errPredictorV3
			}
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
