package whisperio

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func rangeBlockFixture(tb testing.TB, count int, kind string, packed bool) (*continuationFile, whisper.ContinuationBlock, mapContinuationIO, []whisper.TimeSeriesPoint) {
	tb.Helper()
	const step = 60
	kv := mapContinuationIO{}
	fs := newContinuationFiles(kv, false)
	fs.rangeReads = true
	a := whisper.StorageArchiveLayout{Offset: 4096, BlockSize: 65536, BlockCount: 3, Step: step}
	f := &continuationFile{fs: fs, data: &continuationFileData{id: 42, layout: whisper.StorageFileLayout{Compressed: true, Archives: []whisper.StorageArchiveLayout{a}}}}
	desc := blockDescriptor{count: count, bucketed: true}
	if packed {
		desc.packed = store.ColdPackPoints
	}
	input := make([]store.Point, count)
	want := make([]whisper.TimeSeriesPoint, count)
	stamp := int64(1700000001) // Deliberately off grid; physical pruning is not slot filtering.
	for i := range input {
		if i > 0 {
			stamp += int64(step)
			if kind == "gaps" && i%97 == 0 {
				stamp += int64(step * 7)
			}
			if kind == "off-grid" && i%97 == 0 {
				stamp += int64(i%3 - 1)
			}
		}
		value := math.Float64frombits([]uint64{0x8000000000000000, 0x7ff0000000000000, 0x7ff8000000000123, 1}[i%4])
		if kind == "decimal" {
			value = float64((i*7919)%100003) / 100
		}
		input[i] = store.Point{Timestamp: stamp, Value: value}
		want[i] = whisper.TimeSeriesPoint{Time: int(stamp), Value: value}
	}
	for first := 0; first < count; first += desc.groupSize() {
		points := input[first:min(count, first+desc.groupSize())]
		var value []byte
		var err error
		if packed {
			value, err = store.EncodeSealedPoints(points, step)
		} else {
			var block store.PointBlock
			for i, p := range points {
				block.Present[i/64] |= 1 << uint(i%64)
				block.Points[i] = p
			}
			value = store.EncodePointBlock(&block)
		}
		if err != nil {
			tb.Fatal(err)
		}
		kv.Set(desc.groupKey(42, 0, 1, first/desc.groupSize()), value)
	}
	kv.Set(continuationBlockPrefix(42, 'b', 0, 1), desc.encode())
	// Circular rewrites may leave groups beyond the authoritative count.
	kv.Set(desc.groupKey(42, 0, 1, (count+desc.groupSize()-1)/desc.groupSize()), []byte("stale"))
	block := whisper.ContinuationBlock{Archive: 0, Index: 1, Offset: a.Offset + a.BlockSize, Size: a.BlockSize, Step: step, Count: count, First: want[0].Time, Last: want[count-1].Time}
	return f, block, kv, want
}

func filterRange(points []whisper.TimeSeriesPoint, from, until int) []whisper.TimeSeriesPoint {
	var out []whisper.TimeSeriesPoint
	for _, p := range points {
		if from <= p.Time && p.Time <= until {
			out = append(out, p)
		}
	}
	return out
}

func TestContinuationRangeReadOracle(t *testing.T) {
	for _, packed := range []bool{false, true} {
		for _, kind := range []string{"regular", "gaps", "off-grid"} {
			for _, count := range []int{1, 127, 4096, 4097, 2*4096 + 137} {
				t.Run(fmt.Sprintf("packed=%v/%s/%d", packed, kind, count), func(t *testing.T) {
					f, block, _, want := rangeBlockFixture(t, count, kind, packed)
					full, err := f.ReadStorageBlock(block)
					if err != nil {
						t.Fatal(err)
					}
					if err := sameContinuationPoints(want, full); err != nil {
						t.Fatal(err)
					}
					rng := rand.New(rand.NewSource(713))
					ranges := [][2]int{{block.First, block.Last}, {block.First - 10, block.First - 1}, {block.Last + 1, block.Last + 10}, {block.First, block.First}, {block.Last, block.Last}, {block.First + 1, block.First + 2}}
					for i := 0; i < 30; i++ {
						lo := rng.Intn(count)
						hi := lo + rng.Intn(count-lo)
						ranges = append(ranges, [2]int{want[lo].Time, want[hi].Time})
					}
					for _, window := range ranges {
						got, err := f.ReadStorageBlockRange(block, window[0], window[1])
						if err != nil {
							t.Fatalf("range %v: %v", window, err)
						}
						if err := sameContinuationPoints(filterRange(want, window[0], window[1]), filterRange(got, window[0], window[1])); err != nil {
							t.Fatalf("range %v: %v", window, err)
						}
					}
				})
			}
		}
	}
}

func TestContinuationRangeReadValidation(t *testing.T) {
	for _, change := range []string{"missing", "count", "first", "last", "selected-crc", "skipped-crc", "header", "io"} {
		t.Run(change, func(t *testing.T) {
			f, block, kv, want := rangeBlockFixture(t, 2*4096+137, "regular", true)
			desc, _ := f.descriptor(0, 1)
			first := desc.groupKey(42, 0, 1, 0)
			switch change {
			case "missing":
				kv.Delete(first)
			case "count":
				block.Count++
			case "first":
				block.First++
			case "last":
				block.Last++
			case "selected-crc":
				kv[string(desc.groupKey(42, 0, 1, 1))][12] ^= 0x80
			case "skipped-crc":
				kv[string(first)][12] ^= 0x80
			case "header":
				binary.LittleEndian.PutUint16(kv[string(first)][2:4], 0)
			case "io":
				kv.Delete(continuationBlockPrefix(42, 'b', 0, 1))
			}
			from, until := want[4096].Time, want[4097].Time
			_, err := f.ReadStorageBlockRange(block, from, until)
			if change == "skipped-crc" {
				if err != nil {
					t.Fatal("skipped payload was decoded", err)
				}
				if _, err = f.ReadStorageBlock(block); err == nil {
					t.Fatal("full read lost checksum validation")
				}
				f.fs.rangeReads = false
				if _, err = f.ReadStorageBlockRange(block, from, until); err == nil {
					t.Fatal("default path skipped checksum validation")
				}
			} else if err == nil {
				t.Fatal("invalid block accepted")
			}
		})
	}
}

func TestContinuationRangeFetchOracle(t *testing.T) {
	db, codec, now := rangeFetchFixture(t)
	for _, phase := range []string{"sealed", "late-sidecar", "late-direct"} {
		if phase != "sealed" {
			codec.DirectLateWrites = phase == "late-direct"
			bump := math.Pi
			if codec.DirectLateWrites {
				bump = math.E
			}
			if err := db.UpdateMany(context.Background(), "range.metric", []store.Point{
				{Timestamp: int64(now - 3600), Value: bump},
				{Timestamp: int64(now - 7*86400), Value: bump * 2},
			}); err != nil {
				t.Fatal(err)
			}
		}
		for _, hours := range []int{1, 6, 24, 168} {
			from, until := now-120-hours*3600, now-120
			codec.RangeReadsEnabled = func() bool { return false }
			want, err := db.Fetch(context.Background(), "range.metric", from, until)
			if err != nil {
				t.Fatal(err)
			}
			codec.RangeReadsEnabled = func() bool { return true }
			got, err := db.Fetch(context.Background(), "range.metric", from, until)
			if err != nil {
				t.Fatal(err)
			}
			if want.FromTime != got.FromTime || want.UntilTime != got.UntilTime || want.Step != got.Step || len(want.Values) != len(got.Values) {
				t.Fatal("range layout changed")
			}
			for i, v := range want.Values {
				if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
					t.Fatalf("%s/%dh point %d differs", phase, hours, i)
				}
			}
		}
	}
}

func TestContinuationRangeV6Fallback(t *testing.T) {
	f, block, kv, want := rangeBlockFixture(t, 240, "regular", false)
	desc, err := f.descriptor(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	old := desc
	desc.bucketed = false
	for i := 0; i < 2; i++ {
		value, err := kv.Get(old.groupKey(42, 0, 1, i))
		if err != nil {
			t.Fatal(err)
		}
		kv.Set(desc.groupKey(42, 0, 1, i), value)
		kv.Delete(old.groupKey(42, 0, 1, i))
	}
	kv.Set(continuationBlockPrefix(42, 'b', 0, 1), desc.encode())
	got, err := f.ReadStorageBlockRange(block, want[31].Time, want[32].Time)
	if err != nil {
		t.Fatal(err)
	}
	if err := sameContinuationPoints(want, got); err != nil {
		t.Fatal("v6 fallback changed", err)
	}
}
