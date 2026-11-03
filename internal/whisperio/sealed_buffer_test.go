package whisperio

import (
	"fmt"
	"math"
	"sync"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestSealedReadBufferOwnership(t *testing.T) {
	kv := bucketedMapIO{mapContinuationIO{}}
	fs := newContinuationFiles(kv, false)
	const size = 4096
	data := &continuationFileData{id: 1, layout: whisper.StorageFileLayout{Compressed: true, Archives: []whisper.StorageArchiveLayout{{Offset: 4096, BlockSize: size, BlockCount: 2, Step: 60}}}}
	f := &continuationFile{fs: fs, data: data}
	count := size + 17
	want := make([]whisper.TimeSeriesPoint, count)
	for i := range want {
		want[i] = whisper.TimeSeriesPoint{Time: 1700000040 + i*60, Value: float64(i)}
	}
	desc := blockDescriptor{count: count, bucketed: true, packed: size}
	if err := kv.Set(continuationBlockPrefix(1, 'b', 0, 0), desc.encode()); err != nil {
		t.Fatal(err)
	}
	for first := 0; first < count; first += size {
		points := make([]store.Point, min(size, count-first))
		for j := range points {
			p := want[first+j]
			points[j] = store.Point{Timestamp: int64(p.Time), Value: p.Value}
		}
		value, err := store.EncodeSealedPoints(points, 60)
		if err != nil {
			t.Fatal(err)
		}
		if err := kv.Set(desc.groupKey(1, 0, 0, first/size), value); err != nil {
			t.Fatal(err)
		}
	}
	block := whisper.ContinuationBlock{Archive: 0, Index: 0, Offset: 4096, Size: size, Step: 60, Count: count, First: want[0].Time, Last: want[count-1].Time}
	owned, err := f.ReadStorageBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	// Borrowed decode scratch must not leak into returned, independently owned
	// block arrays, including when a later read reuses the same pooled buffer.
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			for range 4 {
				got, err := f.ReadStorageBlock(block)
				if err != nil {
					t.Error(err)
					return
				}
				if err := sameContinuationPoints(want, got); err != nil {
					t.Error(err)
				}
				got[0].Value = -100
				input := []whisper.TimeSeriesPoint{want[size-1], want[size], want[count-1]}
				matched, err := f.MatchesStoragePoints(block, input)
				if err != nil || !matched {
					t.Errorf("boundary match: %v %v", matched, err)
				}
			}
		})
	}
	wg.Wait()
	if err := sameContinuationPoints(want, owned); err != nil {
		t.Fatal(err)
	}
	// Error paths must release the scratch and preserve the independent output.
	key := desc.groupKey(1, 0, 0, 1)
	saved, err := kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), saved...)
	bad[len(bad)-1] ^= 0x80
	if err := kv.Set(key, bad); err != nil {
		t.Fatal(err)
	}
	if got, err := f.ReadStorageBlock(block); err == nil || got != nil {
		t.Fatal("corrupt group accepted")
	}
	if err := kv.Set(key, saved); err != nil {
		t.Fatal(err)
	}
	if got, err := f.ReadStorageBlock(block); err != nil {
		t.Fatal(err)
	} else if err := sameContinuationPoints(want, got); err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(owned[0].Value) != math.Float64bits(want[0].Value) {
		t.Fatal("owned result changed")
	}
}

func TestSealedPointBufferPoolBound(t *testing.T) {
	// No parallel tests mutate this package-global pool. Hold more buffers than
	// its idle bound: acquisition must not block or hand out one owner twice.
	held := make([]*store.SealedPointBuffer, maxIdleSealedPointBuffers+9)
	seen := make(map[*store.SealedPointBuffer]bool)
	for i := range held {
		held[i] = getSealedPointBuffer()
		if seen[held[i]] {
			t.Fatal("buffer has two owners")
		}
		seen[held[i]] = true
	}
	for _, s := range held {
		putSealedPointBuffer(s)
	}
	if len(sealedPointBuffers) != maxIdleSealedPointBuffers {
		t.Fatal(fmt.Sprintf("idle buffers: %d", len(sealedPointBuffers)))
	}
}
