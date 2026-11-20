//go:build partstudy && cgo

package v2part

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Merge-path cost of the order codec on one real merged part (PART_STUDY):
// re-encode every run, add it to dense blocks and compress them at the rest
// level, or decode every run. The same file runs against both trees of an A/B.
type benchRun struct {
	key        []byte
	archive    uint32
	blockStart int64
	records    []sealrollup.Record
	encoded    []byte
}

func loadBenchRuns(b *testing.B) ([]benchRun, int) {
	path := os.Getenv("PART_STUDY")
	if path == "" {
		b.Skip("PART_STUDY")
	}
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	rd, err := sstable.NewSimpleReadable(f)
	if err != nil {
		b.Fatal(err)
	}
	reader, err := sstable.NewReader(rd, sstable.ReaderOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()
	it, err := reader.NewIter(nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer it.Close()
	var runs []benchRun
	records := 0
	for ik, v := it.First(); ik != nil; ik, v = it.Next() {
		k, err := Decode(ik.UserKey)
		if err != nil {
			b.Fatal(err)
		}
		raw, _, err := v.Value(nil)
		if err != nil {
			b.Fatal(err)
		}
		_, run, err := SplitHeader(raw)
		if err != nil || len(run) <= 6 || string(run[:4]) != string(denseMagic) {
			continue
		}
		rs, err := DecodeSlots(k.Archive, k.BlockStart, run)
		if err != nil {
			b.Fatal(err)
		}
		key, err := Encode(k)
		if err != nil {
			b.Fatal(err)
		}
		enc, err := appendSlotsOrder(nil, rs, nil, true)
		if err != nil {
			b.Fatal(err)
		}
		runs = append(runs, benchRun{key: binary.LittleEndian.AppendUint64(key, 1), archive: k.Archive, blockStart: k.BlockStart, records: rs, encoded: enc})
		records += len(rs)
	}
	return runs, records
}

func BenchmarkOrderMergeEncode(b *testing.B) {
	runs, records := loadBenchRuns(b)
	var buf []byte
	var table []uint64
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, r := range runs {
			buf, _ = appendSlotsOrder(buf[:0], r.records, &table, true)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*records), "ns/record")
}

func BenchmarkOrderMergeWrite(b *testing.B) {
	runs, records := loadBenchRuns(b)
	enc, err := newDenseEncoder()
	if err != nil {
		b.Fatal(err)
	}
	var buf, compressed []byte
	var table []uint64
	var blk denseBlock
	out := 0
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		out = 0
		flush := func() {
			if blk.entries == 0 {
				return
			}
			data := blk.finish()
			compressed, err = enc.append(data, compressed[:0], denseRestLevel)
			if err != nil {
				b.Fatal(err)
			}
			out += len(compressed)
			blk.reset()
		}
		for _, r := range runs {
			buf, _ = appendSlotsOrder(buf[:0], r.records, &table, true)
			if blk.entries > 0 && len(blk.data)+len(r.key)+len(buf)+4*len(blk.restarts)+32 > denseBlockSize {
				flush()
			}
			blk.add(r.key, buf, 64)
		}
		flush()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*records), "ns/record")
	b.ReportMetric(float64(out)/float64(records), "B/record")
}

func BenchmarkOrderMergeDecode(b *testing.B) {
	runs, records := loadBenchRuns(b)
	var dst []sealrollup.Record
	var err error
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, r := range runs {
			if dst, err = DecodeSlotsInto(dst, r.archive, r.blockStart, r.encoded); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*records), "ns/record")
}
