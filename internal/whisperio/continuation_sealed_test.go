package whisperio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func sealedMutationFixture(t *testing.T, count int) (bucketedMapIO, *continuationFile, whisper.ContinuationBlock, []whisper.TimeSeriesPoint, uint64) {
	t.Helper()
	kv := bucketedMapIO{mapContinuationIO{}}
	fs := newContinuationFiles(kv, false)
	d := &continuationFileData{id: 1, layout: whisper.StorageFileLayout{Compressed: true, Archives: []whisper.StorageArchiveLayout{{Offset: 4096, BlockSize: 32768, BlockCount: 2, Step: 60}}}}
	f := &continuationFile{fs: fs, data: d}
	points := make([]store.Point, count)
	input := make([]whisper.TimeSeriesPoint, count)
	for i := range points {
		points[i] = store.Point{Timestamp: 1700000040 + int64(i*60), Value: float64(i)}
		input[i] = whisper.TimeSeriesPoint{Time: int(points[i].Timestamp), Value: points[i].Value}
	}
	desc := blockDescriptor{count: count, bucket: 472222, bucketed: true, packed: store.ColdPackPoints}
	if err := kv.Set(continuationBlockPrefix(1, 'b', 0, 0), desc.encode()); err != nil {
		t.Fatal(err)
	}
	var thawBytes uint64
	for first := 0; first < count; first += store.ColdPackPoints {
		value, err := store.EncodeSealedPoints(points[first:min(count, first+store.ColdPackPoints)], 60)
		if err != nil {
			t.Fatal(err)
		}
		key := desc.groupKey(1, 0, 0, first/store.ColdPackPoints)
		if err := kv.Set(key, value); err != nil {
			t.Fatal(err)
		}
		thawBytes += uint64(len(key) + len(value))
	}
	ordinary := desc
	ordinary.packed = 0
	ordinary.bucket |= continuationThawedBucket
	for first := 0; first < count; first += 128 {
		var group store.PointBlock
		for i, p := range points[first:min(count, first+128)] {
			group.Present[i/64] |= uint64(1) << uint(i%64)
			group.Points[i] = p
		}
		thawBytes += uint64(len(ordinary.groupKey(1, 0, 0, first/128)) + len(store.EncodePointBlock(&group)))
	}
	block := whisper.ContinuationBlock{Archive: 0, Index: 0, Offset: 4096, Size: 32768, Step: 60, Count: count, First: input[0].Time, Last: input[count-1].Time}
	return kv, f, block, input, thawBytes
}

func TestSealedBlockMutations(t *testing.T) {
	// Single- and multi-sealed-group reads, through the real physical writer
	// entrypoints. Reuse replaces a range outright and must never count a thaw.
	for _, count := range []int{150, 5000} {
		for _, operation := range []string{"append", "patch", "rewrite", "reuse"} {
			t.Run(fmt.Sprintf("%d/%s", count, operation), func(t *testing.T) {
				kv, f, block, input, thawBytes := sealedMutationFixture(t, count)
				before := ColdPackThaws()
				want := before
				var err error
				switch operation {
				case "append":
					input = append(input, whisper.TimeSeriesPoint{Time: input[count-1].Time + 60, Value: 3})
					block.Count++
					block.Last = input[count].Time
					err = f.AppendStorageBlock(block, count, input[count:])
					want.Appends++
				case "patch":
					input[129].Value = math.Pi
					err = f.ReplaceStoragePoints(block, []int{129}, input)
					want.Patches++
				case "rewrite":
					input = input[:131]
					input[129].Value = math.Pi
					block.Count = len(input)
					block.Last = input[130].Time
					err = f.RewriteStorageBlock(block, 129, input)
					want.Rewrites++
				case "reuse":
					input = input[:2]
					block.Count = 2
					block.Last = input[1].Time
					err = f.AppendStorageBlock(block, 0, input)
				}
				if err != nil {
					t.Fatal(err)
				}
				if operation != "reuse" {
					want.Points += uint64(count)
					want.Bytes += thawBytes
				}
				if got := ColdPackThaws(); got != want {
					t.Fatalf("thaw counters: got %+v want %+v", got, want)
				}
				// A second mutation of the ordinary range cannot count again.
				if err := f.RewriteStorageBlock(block, len(input), input); err != nil {
					t.Fatal(err)
				}
				if got := ColdPackThaws(); got != want {
					t.Fatalf("ordinary rewrite counted a thaw: %+v", got)
				}
				got, err := f.ReadStorageBlock(block)
				if err != nil {
					t.Fatal(err)
				}
				if err := sameContinuationPoints(input, got); err != nil {
					t.Fatal(err)
				}
				data, err := kv.Get(continuationBlockPrefix(1, 'b', 0, 0))
				if err != nil {
					t.Fatal(err)
				}
				desc, err := decodeBlockDescriptor(data)
				if err != nil || desc.packed != 0 || (desc.bucket&continuationThawedBucket != 0) != (operation != "reuse") {
					t.Fatalf("mutation descriptor: %+v %v", desc, err)
				}
			})
		}
	}
}

// Frozen decoder/encoder logic from before the lifecycle counters, as in the first
// v9 codec. Do not delegate to the current codec: this is the rollback oracle. The
// v7 decoder is the same 4/8-byte logic without case 12.
func preLifecycleDescriptor(data []byte) (blockDescriptor, error) {
	switch len(data) {
	case 4:
		return blockDescriptor{count: int(binary.BigEndian.Uint32(data))}, nil
	case 8, 12:
		if len(data) == 12 {
			n := int(binary.BigEndian.Uint32(data[8:]))
			if n < 128 || n > store.MaxSealedPoints || n%128 != 0 {
				return blockDescriptor{}, errors.New("invalid sealed group size")
			}
			return blockDescriptor{count: int(binary.BigEndian.Uint32(data)), bucket: binary.BigEndian.Uint32(data[4:]), bucketed: true, packed: n}, nil
		}
		return blockDescriptor{count: int(binary.BigEndian.Uint32(data)), bucket: binary.BigEndian.Uint32(data[4:]), bucketed: true}, nil
	}
	return blockDescriptor{}, errors.New("invalid continuation block count")
}

func preLifecycleEncode(d blockDescriptor) []byte {
	value := binary.BigEndian.AppendUint32(nil, uint32(d.count))
	if d.bucketed {
		value = binary.BigEndian.AppendUint32(value, d.bucket)
	}
	if d.packed > 0 {
		value = binary.BigEndian.AppendUint32(value, uint32(d.packed))
	}
	return value
}

func TestThawedBucketRollback(t *testing.T) {
	kv, f, block, input, _ := sealedMutationFixture(t, 5000)
	input[129].Value = math.Pi
	if err := f.ReplaceStoragePoints(block, []int{129}, input); err != nil {
		t.Fatal(err)
	}
	descKey := continuationBlockPrefix(1, 'b', 0, 0)
	raw, err := kv.Get(descKey)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := preLifecycleDescriptor(raw)
	if err != nil || len(raw) != 8 || desc.bucket != 472222|continuationThawedBucket || desc.packed != 0 {
		t.Fatalf("old decoder rejected thaw: %+v %v", desc, err)
	}
	// Route and mutate using the old unmasked bucket address, without the
	// current groupKey helper. An old ordinary writer preserves the marker.
	for first := 0; first < len(input); first += 128 {
		key := binary.BigEndian.AppendUint32(store.ContinuationBucketKey(desc.bucket, continuationBlockPrefix(1, 'c', 0, 0)), uint32(first/128))
		raw, err := kv.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		var group store.PointBlock
		if err := store.DecodePointBlock(raw, &group); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < min(128, len(input)-first); i++ {
			if group.Points[i].Timestamp != int64(input[first+i].Time) || group.Points[i].Value != input[first+i].Value {
				t.Fatalf("old reader differs at %d", first+i)
			}
		}
		group.Points[0].Value = 42
		input[first].Value = 42
		if err := kv.Set(key, store.EncodePointBlock(&group)); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(rawDescriptor(t, kv, descKey), preLifecycleEncode(desc)) {
		t.Fatal("old writer changed descriptor bytes")
	}
	if err := kv.Set(descKey, preLifecycleEncode(desc)); err != nil {
		t.Fatal(err)
	}
	got, err := f.ReadStorageBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if err := sameContinuationPoints(input, got); err != nil {
		t.Fatal(err)
	}
	// An old sealer retains the full bucket address in 12-byte descriptors.
	desc.packed = store.ColdPackPoints
	sealed := preLifecycleEncode(desc)
	if got, err := preLifecycleDescriptor(sealed); err != nil || got != desc {
		t.Fatalf("old v9 sealed decoder: %+v %v", got, err)
	}
	if err := f.fs.deleteFile(1); err != nil {
		t.Fatal(err)
	}
	if len(kv.mapContinuationIO) != 0 {
		t.Fatal("file deletion left thawed bucket groups behind")
	}
}

func rawDescriptor(t *testing.T, kv store.ContinuationIO, key []byte) []byte {
	t.Helper()
	value, err := kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
