package whisperio

import (
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
	"math"
	"testing"
)

func TestSealedBlockMutations(t *testing.T) {
	// Exercise each physical mutation with an already sealed predecessor,
	// including circular reuse at the same keys and truncated group tails.
	for _, operation := range []string{"append", "patch", "rewrite", "reuse"} {
		t.Run(operation, func(t *testing.T) {
			kv := bucketedMapIO{mapContinuationIO{}}
			fs := newContinuationFiles(kv, false)
			d := &continuationFileData{id: 1, layout: whisper.StorageFileLayout{Compressed: true, Archives: []whisper.StorageArchiveLayout{{Offset: 4096, BlockSize: 4096, BlockCount: 2, Step: 60}}}}
			f := &continuationFile{fs: fs, data: d}
			points := make([]store.Point, 150)
			input := make([]whisper.TimeSeriesPoint, 150)
			for i := range points {
				points[i] = store.Point{Timestamp: 1700000040 + int64(i*60), Value: float64(i)}
				input[i] = whisper.TimeSeriesPoint{Time: int(points[i].Timestamp), Value: points[i].Value}
			}
			value, err := store.EncodeSealedPoints(points, 60)
			if err != nil {
				t.Fatal(err)
			}
			desc := blockDescriptor{count: len(points), bucketed: true, packed: 1024}
			key := continuationBlockPrefix(1, 'b', 0, 0)
			if err := kv.Set(key, desc.encode()); err != nil {
				t.Fatal(err)
			}
			if err := kv.Set(desc.groupKey(1, 0, 0, 0), value); err != nil {
				t.Fatal(err)
			}
			block := whisper.ContinuationBlock{Archive: 0, Index: 0, Offset: 4096, Size: 4096, Step: 60, Count: 150, First: input[0].Time, Last: input[149].Time}
			switch operation {
			case "append":
				input = append(input, whisper.TimeSeriesPoint{Time: input[149].Time + 60, Value: 3})
				block.Count++
				block.Last = input[150].Time
				err = f.AppendStorageBlock(block, 150, input[150:])
			case "patch":
				input[129].Value = math.Pi
				err = f.ReplaceStoragePoints(block, []int{129}, input)
			case "rewrite":
				input = input[:131]
				input[129].Value = math.Pi
				block.Count = len(input)
				block.Last = input[130].Time
				err = f.RewriteStorageBlock(block, 129, input)
			case "reuse":
				input = input[:2]
				block.Count = 2
				block.Last = input[1].Time
				err = f.AppendStorageBlock(block, 0, input)
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := f.ReadStorageBlock(block)
			if err != nil {
				t.Fatal(err)
			}
			if err := sameContinuationPoints(input, got); err != nil {
				t.Fatal(err)
			}
			data, err := kv.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			desc, err = decodeBlockDescriptor(data)
			if err != nil || desc.packed != 0 {
				t.Fatalf("mutation did not thaw: %+v %v", desc, err)
			}
		})
	}
}
