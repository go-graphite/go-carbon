package whisperio

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type appendTailKV struct {
	data      map[string][]byte
	gets      int
	readError error
}

func (k *appendTailKV) Get(key []byte) ([]byte, error) {
	k.gets++
	if k.readError != nil {
		return nil, k.readError
	}
	v, ok := k.data[string(key)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(v), nil
}
func (k *appendTailKV) Set(key, value []byte) error {
	k.data[string(key)] = bytes.Clone(value)
	return nil
}
func (k *appendTailKV) Delete(key []byte) error { delete(k.data, string(key)); return nil }
func (k *appendTailKV) DeletePrefix(prefix []byte) error {
	for key := range k.data {
		if strings.HasPrefix(key, string(prefix)) {
			delete(k.data, key)
		}
	}
	return nil
}
func (k *appendTailKV) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	var keys []string
	for key := range k.data {
		if strings.HasPrefix(key, string(prefix)) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := visit([]byte(key), k.data[key]); err != nil {
			return err
		}
	}
	return nil
}
func (k *appendTailKV) clone() *appendTailKV {
	result := &appendTailKV{data: make(map[string][]byte)}
	for key, value := range k.data {
		result.data[key] = bytes.Clone(value)
	}
	return result
}

// appendTailBuckets runs the parity tests against v7 time-bucketed groups.
var appendTailBuckets bool

func appendTailFile(k *appendTailKV) *continuationFile {
	clock := func() time.Time { return time.Unix(1_800_000_000, 0) }
	return &continuationFile{fs: &continuationFiles{kv: k, buckets: appendTailBuckets, clock: clock}, data: &continuationFileData{id: 1, layout: whisper.StorageFileLayout{Compressed: true, Size: 4096 + 8*65536, Archives: []whisper.StorageArchiveLayout{{Offset: 4096, Count: 16384, Step: 1, BlockSize: 65536, BlockCount: 8}}}}}
}
func appendTailPoints(first, count int) []whisper.TimeSeriesPoint {
	values := []float64{.1, math.Nextafter(1, 2), math.Copysign(0, -1), 1e-300, 1e100, 9007199254740991, -99}
	result := make([]whisper.TimeSeriesPoint, count)
	for i := range result {
		result[i] = whisper.TimeSeriesPoint{Time: 1000 + first + i, Value: values[(first+i)%len(values)]}
	}
	return result
}
func appendTailBlock(count int) whisper.ContinuationBlock {
	return whisper.ContinuationBlock{Archive: 0, Index: 0, Offset: 4096, Size: 65536, Step: 1, First: 1000, Last: 999 + count, Count: count}
}

// appendTailGroupKey is the seeded block's first group, wherever its
// descriptor places it (v6 body key or a v7 bucket).
func appendTailGroupKey(t testing.TB, k *appendTailKV) []byte {
	t.Helper()
	desc, err := appendTailFile(k).descriptor(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return desc.groupKey(1, 0, 0, 0)
}

func appendTailSeed(t testing.TB, previous int) *appendTailKV {
	t.Helper()
	k := &appendTailKV{data: make(map[string][]byte)}
	if previous > 0 {
		if err := appendTailFile(k).appendStorageBlockLegacyOracle(appendTailBlock(previous), 0, appendTailPoints(0, previous)); err != nil {
			t.Fatal(err)
		}
	}
	return k
}

func TestContinuationAppendTailMatchesLegacy(t *testing.T) {
	for _, previous := range []int{0, 1, 31, 32, 63, 64, 95, 96, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024} {
		for _, count := range []int{1, 2, 31, 32, 63, 64, 127, 128, 129, 257} {
			t.Run(fmt.Sprintf("%d+%d", previous, count), func(t *testing.T) {
				seed := appendTailSeed(t, previous)
				oldKV, newKV := seed.clone(), seed.clone()
				oldFile, newFile := appendTailFile(oldKV), appendTailFile(newKV)
				block := appendTailBlock(previous + count)
				added := appendTailPoints(previous, count)
				oldErr := oldFile.appendStorageBlockLegacyOracle(block, previous, added)
				newErr := newFile.AppendStorageBlock(block, previous, added)
				if oldErr != nil || newErr != nil {
					t.Fatalf("old=%v new=%v", oldErr, newErr)
				}
				if !reflect.DeepEqual(oldKV.data, newKV.data) {
					t.Fatal("encoded persistent state differs from original function")
				}
				saved := oldKV.gets - newKV.gets
				wantSaved := 0
				if previous%128 != 0 {
					wantSaved = 1
				}
				if saved != wantSaved {
					t.Fatalf("reads saved=%d want=%d", saved, wantSaved)
				}
				actual, err := newFile.ReadStorageBlock(block)
				if err != nil {
					t.Fatal(err)
				}
				want := append(appendTailPoints(0, previous), added...)
				if len(actual) != len(want) {
					t.Fatalf("length %d != %d", len(actual), len(want))
				}
				for i, p := range want {
					if actual[i].Time != p.Time || math.Float64bits(actual[i].Value) != math.Float64bits(p.Value) {
						t.Fatalf("point bits differ at%d", i)
					}
				}
			})
		}
	}
}

// The fleet codec permits strictly increasing unaligned physical timestamps.
// Keep this reuse optimization byte-identical on that accepted input too.
func TestContinuationAppendTailUnalignedParity(t *testing.T) {
	for _, previous := range []int{1, 127, 128, 129, 255} {
		for _, count := range []int{1, 128, 257} {
			a, b := appendTailSeed(t, previous), appendTailSeed(t, previous)
			oldFile, newFile := appendTailFile(a), appendTailFile(b)
			oldFile.data.layout.Archives[0].Step = 600
			newFile.data.layout.Archives[0].Step = 600
			block := appendTailBlock(previous + count)
			block.Step = 600
			points := appendTailPoints(previous, count)
			oldErr := oldFile.appendStorageBlockLegacyOracle(block, previous, points)
			newErr := newFile.AppendStorageBlock(block, previous, points)
			if oldErr != nil || newErr != nil || !reflect.DeepEqual(a.data, b.data) {
				t.Fatalf("%d+%d: old=%v new=%v persistent parity=%v", previous, count, oldErr, newErr, reflect.DeepEqual(a.data, b.data))
			}
		}
	}
}

func TestContinuationAppendTailErrorParity(t *testing.T) {
	for _, kind := range []string{"duplicate", "descending", "wrong-count", "missing-tail", "corrupt-tail", "read-error", "readonly", "zero-added", "wrong-layout"} {
		t.Run(kind, func(t *testing.T) {
			previous := 95
			seed := appendTailSeed(t, previous)
			added := appendTailPoints(previous, 257)
			block := appendTailBlock(previous + len(added))
			switch kind {
			case "duplicate":
				added[0].Time--
			case "descending":
				added[130].Time = added[0].Time
			case "wrong-count":
				previous--
			case "missing-tail":
				delete(seed.data, string(appendTailGroupKey(t, seed)))
			case "corrupt-tail":
				seed.data[string(appendTailGroupKey(t, seed))] = []byte{1, 255}
			case "zero-added":
				added = nil
			case "wrong-layout":
				block.Step = 2
			}
			a, b := seed.clone(), seed.clone()
			fa, fb := appendTailFile(a), appendTailFile(b)
			if kind == "read-error" {
				a.readError = errors.New("injected read error")
				b.readError = a.readError
			}
			if kind == "readonly" {
				fa.readOnly = true
				fb.readOnly = true
			}
			ea, eb := fa.appendStorageBlockLegacyOracle(block, previous, added), fb.AppendStorageBlock(block, previous, added)
			if ea == nil || eb == nil || ea.Error() != eb.Error() {
				t.Fatalf("old=%v new=%v", ea, eb)
			}
			if !reflect.DeepEqual(a.data, b.data) {
				t.Fatal("error leaves different staged state")
			}
		})
	}
}
func BenchmarkContinuationAppendTail(b *testing.B) {
	for _, legacy := range []bool{true, false} {
		name := "reused"
		if legacy {
			name = "original"
		}
		b.Run(name, func(b *testing.B) {
			previous := 96
			seed := appendTailSeed(b, previous)
			k := seed.clone()
			f := appendTailFile(k)
			added := appendTailPoints(previous, 32)
			block := appendTailBlock(previous + len(added))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for key, value := range seed.data {
					k.data[key] = value
				}
				var err error
				if legacy {
					err = f.appendStorageBlockLegacyOracle(block, previous, added)
				} else {
					err = f.AppendStorageBlock(block, previous, added)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Verbatim baseline (5310c860) frozen before the production change: the v7
// line's descriptor-aware append, which re-reads the predecessor group.
func (f *continuationFile) appendStorageBlockLegacyOracle(block whisper.ContinuationBlock, previous int, added []whisper.TimeSeriesPoint) error {
	fresh := f.fs.newBlock()
	if err := f.writable(); err != nil {
		return err
	}
	if err := f.validateBlock(block); err != nil {
		return err
	}
	if previous < 0 || len(added) == 0 || previous+len(added) != block.Count {
		return errors.New("invalid continuation append count")
	}
	desc, err := f.descriptor(block.Archive, block.Index)
	if err != nil {
		return err
	}
	if previous == 0 {
		if err := f.fs.kv.DeletePrefix(desc.groupPrefix(f.data.id, block.Archive, block.Index)); err != nil {
			return err
		}
		desc = fresh
	} else if desc.count != previous {
		return fmt.Errorf("continuation append predecessor differs: %d != %d", desc.count, previous)
	}
	previousTime := int64(0)
	if previous > 0 {
		group, err := f.pointBlock(desc.groupKey(f.data.id, block.Archive, block.Index, (previous-1)/128))
		if err != nil {
			return err
		}
		previousTime = group.Points[(previous-1)%128].Timestamp
		if previousTime <= 0 {
			return errors.New("missing continuation append predecessor")
		}
	}
	for first := 0; first < len(added); {
		position := previous + first
		key := desc.groupKey(f.data.id, block.Archive, block.Index, position/128)
		group := &store.PointBlock{}
		if position%128 != 0 {
			group, err = f.pointBlock(key)
			if err != nil {
				return err
			}
		}
		if position != 0 && position%128 != 0 {
			previousTime = group.Points[position%128-1].Timestamp
		}
		end := min(len(added), first+128-position%128)
		for first < end {
			point := added[first]
			if point.Time <= 0 || int64(point.Time) > math.MaxUint32 || int64(point.Time) <= previousTime {
				return errors.New("invalid continuation appended point")
			}
			slot := (previous + first) % 128
			group.Present[slot/64] |= uint64(1) << uint(slot%64)
			group.Points[slot] = store.Point{Timestamp: int64(point.Time), Value: point.Value}
			previousTime = int64(point.Time)
			first++
		}
		if err := f.setPointBlock(key, group); err != nil {
			return err
		}
	}
	if added[len(added)-1].Time != block.Last {
		return errors.New("continuation append end differs")
	}
	desc.count = block.Count
	return f.fs.kv.Set(continuationBlockPrefix(f.data.id, 'b', block.Archive, block.Index), desc.encode())
}

func TestContinuationAppendTailParityWithTimeBuckets(t *testing.T) {
	appendTailBuckets = true
	defer func() { appendTailBuckets = false }()
	if bytes.Equal(appendTailGroupKey(t, appendTailSeed(t, 95)), continuationBodyKey(1, 'c', 0, 0, 0)) {
		t.Fatal("time-bucket mode wrote v6 group keys")
	}
	t.Run("matches", TestContinuationAppendTailMatchesLegacy)
	t.Run("unaligned", TestContinuationAppendTailUnalignedParity)
	t.Run("errors", TestContinuationAppendTailErrorParity)
}
