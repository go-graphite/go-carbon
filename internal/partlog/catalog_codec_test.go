package partlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

// Catalog records carry stable, delta-coded catalog IDs and independent predictor
// epochs. A new encoder per segment permits trimming preceding segments.

// referenceCatalogEncoder belongs to one stream owner. Encode preserves arbitrary
// int64 timestamps/clocks, exact IEEE bits, metadata and apply order. Close
// releases compressor memory. Point slices are not retained; dictionary metadata
// is persisted in the catalog; predictors are retained for the segment epoch. The input is never modified.
type referenceCatalogEncoder struct {
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

func (e *referenceCatalogEncoder) Encode(fs []DictionaryFrame) ([]byte, error) {
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

func (e *referenceCatalogEncoder) Close() {
	if e.encoder != nil {
		_ = e.encoder.Close()
		e.encoder = nil
	}
}

// referenceCatalogEncoder freezes the pre-optimization PDC4 wire format.
func TestCatalogEncoderByteOracle(t *testing.T) {
	for _, independent := range []bool{false, true} {
		for _, series := range []bool{false, true} {
			for _, sparse := range []bool{false, true} {
				t.Run(fmt.Sprint(independent, series, sparse), func(t *testing.T) {
					c := newTestCatalog(t)
					a := referenceCatalogEncoder{Catalog: c, Independent: independent, SeriesClocks: series, SparseClocks: sparse}
					b := CatalogEncoder{Catalog: c, Independent: independent, SeriesClocks: series, SparseClocks: sparse}
					defer a.Close()
					defer b.Close()
					bIDs := CatalogEncoder{Catalog: c, Independent: independent, SeriesClocks: series, SparseClocks: sparse}
					defer bIDs.Close()
					rng := rand.New(rand.NewSource(87153))
					var kept, saved [][]byte
					for record := 0; record < 17; record++ {
						fs := make([]DictionaryFrame, 301)
						for i := range fs {
							// Cross predictor-page boundaries, revisit IDs and add names between records.
							id := rng.Intn(3000)
							fs[i] = DictionaryFrame{Key: fmt.Sprintf("metric/%d/\x00\xff", id), Definition: []byte{byte(id), 0, 255}, Clock: int64(rng.Uint64())}
							for j := 0; j < 1+rng.Intn(9); j++ {
								fs[i].Points = append(fs[i].Points, points.Point{Timestamp: int64(rng.Uint64()), Value: math.Float64frombits(rng.Uint64())})
							}
						}
						if record == 0 {
							fs = append(fs, v3Fixture()...)
						}
						want, e := a.Encode(fs)
						if e != nil {
							t.Fatal(e)
						}
						got, e := b.Encode(fs)
						if e != nil {
							t.Fatal(e)
						}
						if !bytes.Equal(want, got) {
							t.Fatalf("record %d changed bytes", record)
						}
						ids := make([]uint64, len(fs))
						for i, f := range fs {
							if i%3 != 0 {
								ids[i], e = c.Intern(f.Key, f.Definition)
								if e != nil {
									t.Fatal(e)
								}
							}
						}
						gotIDs, e := bIDs.EncodeIDs(fs, ids)
						if e != nil || !bytes.Equal(want, gotIDs) {
							t.Fatalf("record %d IDs changed bytes: %v", record, e)
						}
						kept = append(kept, gotIDs)
						saved = append(saved, bytes.Clone(gotIDs))

						kept = append(kept, got)
						saved = append(saved, bytes.Clone(got))
					}
					for i := range kept {
						if !bytes.Equal(kept[i], saved[i]) {
							t.Fatal("later encode changed retained record", i)
						}
					}
				})
			}
		}
	}
}

func BenchmarkCatalogEncoderRealShape(b *testing.B) {
	for _, ref := range []bool{true, false} {
		b.Run(fmt.Sprint("reference=", ref), func(b *testing.B) {
			c := newTestCatalog(b)
			fs := make([]DictionaryFrame, 4096)
			for i := range fs {
				fs[i] = DictionaryFrame{Key: fmt.Sprintf("metrics/system/component/%d/requests", i), Clock: 1794800000123456789, Points: []points.Point{{Timestamp: 1794800000, Value: float64(i) / 7}}}
			}
			var enc interface {
				Encode([]DictionaryFrame) ([]byte, error)
				Close()
			}
			if ref {
				enc = &referenceCatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
			} else {
				enc = &CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
			}
			defer enc.Close()
			if _, err := enc.Encode(fs); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := enc.Encode(fs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestCatalogEncoderRejectsWrongIDs(t *testing.T) {
	for _, kind := range []string{"length", "missing", "key", "definition", "failed"} {
		t.Run(kind, func(t *testing.T) {
			c := newTestCatalog(t)
			id, err := c.Intern("one", []byte{1})
			if err != nil {
				t.Fatal(err)
			}
			enc := CatalogEncoder{Catalog: c}
			defer enc.Close()
			fs := []DictionaryFrame{{Key: "one", Definition: []byte{1}, Points: []points.Point{{Timestamp: -1, Value: math.Float64frombits(1 << 63)}}}}
			ids := []uint64{id}
			switch kind {
			case "length":
				ids = nil
			case "missing":
				ids[0] = id + 1
			case "key":
				fs[0].Key = "other"
			case "definition":
				fs[0].Definition = []byte{2}
			case "failed":
				c.failed = errCatalog
			}
			if _, err = enc.EncodeIDs(fs, ids); err == nil {
				t.Fatal("accepted invalid resolved ID")
			}
			// Restore the test-only failure so cleanup can sync and close the catalog.
			c.failed = nil
		})
	}
}
