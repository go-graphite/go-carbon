package v2part

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestDenseBitExact(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 7))
	for _, n := range []int{1, 2, 127, 128, 4096} {
		for mode := 0; mode < 4; mode++ {
			// Bit 0: integer values; bit 1: every record shares one order.
			integers, shared := mode&1 != 0, mode&2 != 0
			rs := make([]sealrollup.Record, n)
			var slot int64
			for i := range rs {
				slot += int64(1 + rng.Uint64N(30))
				b := rng.Uint64()
				if integers {
					b = math.Float64bits(float64(int64(i)*119 - 100000))
				}
				edges := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
				if !integers && i < len(edges) {
					b = edges[i]
				}
				order := rng.Uint64() | 1
				if shared {
					order = 1<<40 + uint64(i)*977 + rng.Uint64N(3)
				}
				rs[i] = sealrollup.Record{Delta: sealrollup.Delta{Archive: 2, Slot: slot, Order: order, Sample: sealrollup.Sample{Time: 1700000000 + int64((i+400)%4096)*86400, Bits: b}}, Correction: i%3 == 0}
			}
			_, e := EncodeSlots(rs)
			if e != nil {
				t.Fatal(e)
			}
			b, e := appendSlotsOrder(nil, rs, nil, PredictedOrders)
			if e != nil {
				t.Fatal(e)
			}
			if predicted := b[4]&denseModeOrderLinear != 0; predicted != (n > 1) {
				t.Fatalf("n=%d: predicted=%v", n, predicted)
			}
			got, e := DecodeSlots(2, rs[0].Slot, b)
			if e != nil {
				t.Fatal(e)
			}
			for i := range rs {
				if !bytes.Equal(sealrollup.EncodeRecord(rs[i]), sealrollup.EncodeRecord(got[i])) || rs[i].Slot != got[i].Slot {
					t.Fatalf("changed slot %d", i)
				}
			}
			for cut := 0; cut < len(b); cut += max(1, len(b)/97) {
				if _, e := DecodeSlots(2, rs[0].Slot, b[:cut]); e == nil {
					t.Fatalf("truncation accepted %d", cut)
				}
			}
			if _, e := DecodeSlots(2, rs[0].Slot, append(b, 0)); e == nil {
				t.Fatal("trailing byte accepted")
			}
			if _, e := DecodeSlots(2, rs[0].Slot+1, b); e == nil {
				t.Fatal("first slot mismatch")
			}
		}
	}
	bad := append(append([]byte(nil), denseMagic...), 0)
	bad = binary.AppendUvarint(bad, MaxDenseSlots+1)
	if _, e := DecodeSlots(0, 0, bad); e == nil {
		t.Fatal("unbounded count")
	}
}

// Compare full SST bytes with the existing native exact 128-slot block encoder,
// using standard Zstd outer SST compression. This is a local synthetic density
// qualification, not a native source-visibility adapter or captured-data proof.
func TestDenseLadderBytes(t *testing.T) {
	ladders := [][][2]int64{{{60, 43200}, {3600, 8760}, {86400, 3650}}, {{60, 20160}, {1800, 35040}}}
	for li, ladder := range ladders {
		for _, integer := range []bool{true, false} {
			t.Run(fmt.Sprintf("ladder%d/integer%v", li, integer), func(t *testing.T) {
				spec := fixtureSpec()
				spec.Scope.Partition = 0
				spec.Policies = nil
				spec.Encoding = DenseEncoding
				for a := range ladder {
					spec.Policies = append(spec.Policies, ArchivePolicy{Archive: uint32(a), Definition: []byte{byte(a + 1)}})
					p := &spec.Policies[len(spec.Policies)-1]
					p.Digest = sha256.Sum256(p.Definition)
				}
				root := t.TempDir()
				dense := filepath.Join(root, "dense.sst")
				native := filepath.Join(root, "native.sst")
				w, e := NewDenseWriter(dense, spec)
				if e != nil {
					t.Fatal(e)
				}
				defer w.Abort()
				f, e := vfs.Default.Create(native)
				if e != nil {
					t.Fatal(e)
				}
				nw := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.ZstdCompression, BlockSize: 256 << 10, BlockRestartInterval: 64, TableFormat: sstable.TableFormatRocksDBv2})
				var expected []sealrollup.Record
				total := 0
				for a, archive := range ladder {
					var block chunkstore.PointBlock
					blockStart := int64(0)
					flush := func() {
						if block.Present == [2]uint64{} {
							return
						}
						k := fixtureKey(spec, 0)
						k.Archive = uint32(a)
						k.BlockStart = blockStart
						key, _ := Encode(k)
						if e := nw.Set(key, chunkstore.EncodePointBlock(&block)); e != nil {
							t.Fatal(e)
						}
						block = chunkstore.PointBlock{}
					}
					for i := int64(0); i < archive[1]; i++ {
						if i%128 == 0 {
							flush()
							blockStart = i
						}
						// Coarse archives include sparse gaps; no empty slots are materialized.
						if a > 0 && i%13 == 0 {
							continue
						}
						value := float64(100000 + i%997)
						if !integer {
							value = 123.5 + math.Sin(float64(i)*.013)*53.7 + float64(i%17)*.0001
						}
						bits := math.Float64bits(value)
						stamp := int64(1700000000) + i*archive[0]
						r := sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: i, Order: 1, Sample: sealrollup.Sample{Time: stamp, Bits: bits}}}
						k := fixtureKey(spec, 0)
						k.Archive = uint32(a)
						k.BlockStart = i
						key, _ := Encode(k)
						if e = w.SetEncoded(key, sealrollup.EncodeRecord(r)); e != nil {
							t.Fatal(e)
						}
						slot := int(i % 128)
						block.Present[slot/64] |= 1 << uint(slot%64)
						block.Points[slot] = chunkstore.Point{Timestamp: stamp, Value: value}
						expected = append(expected, r)
						total++
					}
					flush()
				}
				d, e := w.Close()
				if e != nil {
					t.Fatal(e)
				}
				if e = nw.Close(); e != nil {
					t.Fatal(e)
				}
				st, _ := os.Stat(native)
				ratio := float64(d.Bytes) / float64(st.Size())
				descriptor, e := d.MarshalBinary()
				if e != nil {
					t.Fatal(e)
				}
				totalRatio := float64(d.Bytes+uint64(len(descriptor))) / float64(st.Size())
				t.Logf("descriptor=%d with_descriptor_ratio=%.4f", len(descriptor), totalRatio)
				if totalRatio > 1.2 {
					t.Errorf("SST plus descriptor density %.4fx exceeds 1.2x", totalRatio)
				}
				t.Logf("slots=%d dense=%d native=%d dense_B/slot=%.4f native_B/slot=%.4f ratio=%.4f records=%d", total, d.Bytes, st.Size(), float64(d.Bytes)/float64(total), float64(st.Size())/float64(total), ratio, d.Coverage.Records)
				if ratio > 1.2 {
					t.Errorf("density %.4fx exceeds 1.2x", ratio)
				}
				at := 0
				e = ReadExpanded(context.Background(), dense, &d, func(k Key, v []byte) error {
					if at >= len(expected) {
						t.Fatal("extra record")
					}
					r := expected[at]
					if k.Archive != uint32(r.Archive) || k.BlockStart != r.Slot || !reflect.DeepEqual(v, sealrollup.EncodeRecord(r)) {
						t.Fatalf("changed slot %d", at)
					}
					at++
					return nil
				})
				if e != nil || at != len(expected) {
					t.Fatalf("readback %d: %v", at, e)
				}
			})
		}
	}
}

func TestDenseManyNamesDaily(t *testing.T) {
	const names = 1720
	const slots = 229155
	spec := fixtureSpec()
	spec.Scope.Partition = 0
	spec.Scope.VShard = GroupVShard
	spec.Encoding = DenseEncoding
	spec.Source.MinOrder = 1
	spec.Source.MaxOrder = 1
	spec.MinTime = 1700000000
	spec.MaxTime = 1700000000 + 134*86400
	type kv struct {
		k Key
		b []byte
		r sealrollup.Record
	}
	rows := make([]kv, 0, slots)
	rng := rand.New(rand.NewPCG(71, 31))
	for n := 0; n < names; n++ {
		name := ID(fmt.Sprintf("synthetic.daily.metric.%04d", n))
		count := slots / names
		if n < slots%names {
			count++
		}
		for i := 0; i < count; i++ {
			r := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: int64(i), Order: 1, Sample: sealrollup.Sample{Time: 1700000000 + int64(i)*86400, Bits: math.Float64bits(float64(40000+rng.Uint64N(1000000)) / 32)}}}
			k := Key{Group: spec.Scope.Group, VShard: Route(name), Part: spec.Scope.Part, Metric: name, BlockStart: int64(i)}
			key, e := Encode(k)
			if e != nil {
				t.Fatal(e)
			}
			rows = append(rows, kv{k, key, r})
		}
	}
	slices.SortFunc(rows, func(a, b kv) int { return bytes.Compare(a.b, b.b) })
	dir := t.TempDir()
	path := filepath.Join(dir, "daily.sst")
	w, e := NewDenseWriter(path, spec)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	f, e := vfs.Default.Create(filepath.Join(dir, "native.sst"))
	if e != nil {
		t.Fatal(e)
	}
	nw := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.ZstdCompression, BlockSize: 256 << 10, BlockRestartInterval: 64, TableFormat: sstable.TableFormatRocksDBv2})
	var block chunkstore.PointBlock
	var key Key
	flush := func() {
		if block.Present == [2]uint64{} {
			return
		}
		b, _ := Encode(key)
		if e := nw.Set(b, chunkstore.EncodePointBlock(&block)); e != nil {
			t.Fatal(e)
		}
		block = chunkstore.PointBlock{}
	}
	for _, row := range rows {
		if row.k.Metric != key.Metric || row.r.Slot%128 == 0 {
			flush()
			key = row.k
		}
		i := int(row.r.Slot % 128)
		block.Present[i/64] |= 1 << uint(i%64)
		block.Points[i] = chunkstore.Point{Timestamp: row.r.Sample.Time, Value: math.Float64frombits(row.r.Sample.Bits)}
		if e = w.SetEncoded(row.b, sealrollup.EncodeRecord(row.r)); e != nil {
			t.Fatal(e)
		}
	}
	flush()
	d, e := w.Close()
	if e != nil {
		t.Fatal(e)
	}
	if e = nw.Close(); e != nil {
		t.Fatal(e)
	}
	st, e := os.Stat(filepath.Join(dir, "native.sst"))
	if e != nil {
		t.Fatal(e)
	}
	ratio := float64(d.Bytes) / float64(st.Size())
	descriptor, e := d.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	totalRatio := float64(d.Bytes+uint64(len(descriptor))) / float64(st.Size())
	t.Logf("descriptor=%d with_descriptor_ratio=%.4f", len(descriptor), totalRatio)
	if totalRatio > 1.2 {
		t.Errorf("SST plus descriptor density %.4fx exceeds 1.2x", totalRatio)
	}
	t.Logf("shape=convert14-synthetic names=%d slots=%d dense=%d native=%d dense_B/slot=%.4f native_B/slot=%.4f ratio=%.4f files=1 physical_records=%d", names, len(rows), d.Bytes, st.Size(), float64(d.Bytes)/slots, float64(st.Size())/slots, ratio, d.Coverage.Records)
	if ratio > 1.2 {
		t.Errorf("density %.4fx exceeds 1.2x", ratio)
	}
	if d.Version != GroupFileVersion || d.Coverage.Names != names {
		t.Fatal("bad group descriptor")
	}
	i := 0
	e = ReadExpanded(context.Background(), path, &d, func(k Key, b []byte) error {
		if i >= len(rows) || k != rows[i].k || !bytes.Equal(b, sealrollup.EncodeRecord(rows[i].r)) {
			t.Fatalf("changed row %d", i)
		}
		i++
		return nil
	})
	if e != nil || i != slots {
		t.Fatalf("readback %d: %v", i, e)
	}
	old := d
	old.Version = SchemaVersion
	if old.Validate() == nil {
		t.Fatal("unversioned group file accepted")
	}
	other := d
	other.Scope.Group = "other"
	if Read(context.Background(), path, &other, nil) == nil {
		t.Fatal("foreign group descriptor accepted")
	}
}

func TestDenseWriterRejectsInvalidAndClosedInput(t *testing.T) {
	spec := fixtureSpec()
	record := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 1, Order: 7, Sample: sealrollup.Sample{Time: 1700000000, Bits: 1 << 63}}}
	key, _ := Encode(fixtureKey(spec, 0))
	value := sealrollup.EncodeRecord(record)
	for _, kind := range []string{"duplicate", "foreign", "ordinal", "bad-value"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.sst")
			w, e := NewDenseWriter(path, spec)
			if e != nil {
				t.Fatal(e)
			}
			if e = w.SetEncoded(key, value); e != nil {
				t.Fatal(e)
			}
			badKey := fixtureKey(spec, 0)
			badValue := bytes.Clone(value)
			switch kind {
			case "foreign":
				badKey.Part = PartID{8}
			case "ordinal":
				badKey.BlockStart = 2
				badKey.Ordinal = 1
			case "bad-value":
				badKey.BlockStart = 2
				badValue[0] = 9
			}
			encoded, _ := Encode(badKey)
			if e = w.SetEncoded(encoded, badValue); e == nil {
				t.Fatal("bad input accepted")
			}
			if _, e = w.Close(); e == nil {
				t.Fatal("invalid writer published")
			}
			if _, e = os.Stat(path); !os.IsNotExist(e) {
				t.Fatal("invalid output leaked")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "good.sst")
	w, e := NewDenseWriter(path, spec)
	if e != nil {
		t.Fatal(e)
	}
	if e = w.SetEncoded(key, value); e != nil {
		t.Fatal(e)
	}
	if _, e = w.Close(); e != nil {
		t.Fatal(e)
	}
	if e = w.SetEncoded(key, value); !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal("write after close", e)
	}
	if _, e = w.Close(); !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal("second close", e)
	}
	if _, e = NewDenseWriter(path, spec); e == nil {
		t.Fatal("existing file overwritten")
	}
}

func TestDenseWriterByteLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limited.sst")
	spec := fixtureSpec()
	spec.Scope.Partition = 0
	spec.Scope.VShard = GroupVShard
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	w.LimitBytes(1)
	key, err := Encode(Key{Group: spec.Scope.Group, VShard: Route("limit.metric"), Part: spec.Scope.Part, Metric: "limit.metric", Archive: MetadataArchive})
	if err != nil {
		t.Fatal(err)
	}
	err = w.SetEncoded(key, []byte("metadata"))
	if err == nil {
		_, err = w.Close()
	}
	if !errors.Is(err, ErrPartSizeLimit) {
		t.Fatalf("limit error=%v", err)
	}
	_ = w.Abort()
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging remains: %v", err)
	}
}

func TestDenseOrderSummary(t *testing.T) {
	for _, n := range []int{1, 2, 127, MaxDenseSlots} {
		for _, aligned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/aligned=%v", n, aligned), func(t *testing.T) {
				rs := make([]sealrollup.Record, n)
				var maximum uint64
				for i := range rs {
					stamp := int64(1000 + i)
					if !aligned {
						stamp = 1000 + int64(i/2)*2
					}
					order := uint64(i*997 + 1)
					maximum = max(maximum, order)
					rs[i] = sealrollup.Record{Delta: sealrollup.Delta{Slot: int64(i), Order: order, Sample: sealrollup.Sample{Time: stamp, Bits: uint64(i) * 0x7000000000000001}}, Correction: i%3 == 0}
				}
				b, err := appendSlotsOrder(nil, rs, nil, SummaryOrders)
				if err != nil {
					t.Fatal(err)
				}
				if !HasOrderSummary(b) || (b[4]&denseAligned != 0) != (aligned && n > 1) {
					t.Fatalf("mode %x", b[:6])
				}
				got, err := DecodeRecordsInto(nil, 0, 0, b)
				if err != nil {
					t.Fatal(err)
				}
				for i := range rs {
					rs[i].Order = maximum
					rs[i].Sample.Sequence = maximum
				}
				if !reflect.DeepEqual(got, rs) {
					t.Fatal("summary changed sample/flags")
				}
				for cut := 0; cut < len(b); cut++ {
					if _, err = DecodeRecordsInto(nil, 0, 0, b[:cut]); err == nil {
						t.Fatalf("accepted truncation %d", cut)
					}
				}
				for _, ext := range []byte{0, extKnown, 4, 0xff} {
					bad := bytes.Clone(b)
					bad[5] = ext
					if _, err = DecodeRecordsInto(nil, 0, 0, bad); err == nil {
						t.Fatalf("accepted extension %x", ext)
					}
				}
				for _, flag := range []byte{denseModeOrderDoD, denseModeOrderLinear} {
					bad := bytes.Clone(b)
					bad[4] |= flag
					if _, err = DecodeRecordsInto(nil, 0, 0, bad); err == nil {
						t.Fatal("accepted exact/summary flags")
					}
				}
				if n == 1 {
					bad := bytes.Clone(b)
					bad[10] = 0
					if _, err = DecodeRecordsInto(nil, 0, 0, bad); err == nil {
						t.Fatal("accepted zero maximum")
					}
				}
			})
		}
	}
	for _, rs := range [][]sealrollup.Record{
		{{Delta: sealrollup.Delta{Slot: 1, Order: 1, Sample: sealrollup.Sample{Time: 1000}}, Kind: sealrollup.KindMat}},
		{{Delta: sealrollup.Delta{Slot: 1, Order: 1, Sample: sealrollup.Sample{Time: 1000}}}, {Delta: sealrollup.Delta{Slot: 1, Order: 2, Sample: sealrollup.Sample{Time: 1001}}}},
	} {
		if _, err := appendSlotsOrder(nil, rs, nil, SummaryOrders); err == nil {
			t.Fatal("summarized versions")
		}
	}
}

func TestDenseOrderSummaryCopyAndSwitch(t *testing.T) {
	for _, appendRun := range []bool{false, true} {
		t.Run(fmt.Sprint(appendRun), func(t *testing.T) {
			spec := fixtureSpec()
			spec.Encoding = DenseEncoding
			key := fixtureKey(spec, 0)
			key.Ordinal = 0
			key.BlockStart = 0
			rs := make([]sealrollup.Record, MaxDenseSlots)
			for i := range rs {
				rs[i] = sealrollup.Record{Delta: sealrollup.Delta{Archive: int(key.Archive), Slot: int64(i), Order: uint64(i + 1), Sample: sealrollup.Sample{Time: int64(1000 + i), Bits: uint64(i)}}}
			}
			exact, err := EncodeSlots(rs)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "summary.sst")
			w, err := NewDenseWriter(path, spec)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if err = w.SetOrderEncoding(SummaryOrders); err != nil {
				t.Fatal(err)
			}
			k, _ := Encode(key)
			if appendRun {
				err = w.AppendSlots(k, exact, rs)
			} else {
				err = w.SetSlotsEncoded(k, exact)
			}
			if err != nil {
				t.Fatal(err)
			}
			// Switching must flush the pending summary, including a folded/raw copy.
			if err = w.SetOrderEncoding(PredictedOrders); err != nil {
				t.Fatal(err)
			}
			r := rs[0]
			r.Slot = MaxDenseSlots
			r.Order = 7
			key.BlockStart = r.Slot
			k, _ = Encode(key)
			if err = w.SetRecordEncoded(k, r); err != nil {
				t.Fatal(err)
			}
			d, err := w.Close()
			if err != nil {
				t.Fatal(err)
			}
			seen := 0
			err = Read(context.Background(), path, &d, func(k Key, b []byte) error {
				got, e := DecodeRecordsInto(nil, k.Archive, k.BlockStart, b)
				if e != nil {
					return e
				}
				if HasOrderSummary(b) != (seen == 0) {
					t.Fatal("copy or selector bypassed summary")
				}
				if seen == 0 && (len(got) != MaxDenseSlots || got[0].Order != MaxDenseSlots) {
					t.Fatal("wrong maximum")
				}
				if seen == 1 && got[0].Order != 7 {
					t.Fatal("summary leaked to next run")
				}
				seen++
				return nil
			})
			if err != nil || seen != 2 {
				t.Fatalf("runs=%d: %v", seen, err)
			}
		})
	}
}
