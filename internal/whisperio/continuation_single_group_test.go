package whisperio

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestContinuationSingleGroupRead(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, count := range []int{1, 35, 127, 128, 129, 240} {
			t.Run(fmt.Sprintf("buckets=%v/points=%d", buckets, count), func(t *testing.T) {
				kv := appendTailSeed(t, 0)
				f := appendTailFile(kv)
				f.fs.buckets = buckets
				want := appendTailPoints(0, count)
				block := appendTailBlock(count)
				if err := f.AppendStorageBlock(block, 0, want); err != nil {
					t.Fatal(err)
				}
				// Old groups beyond the descriptor's count remain after key reuse.
				desc, err := f.descriptor(0, 0)
				if err != nil {
					t.Fatal(err)
				}
				kv.data[string(desc.groupKey(1, 0, 0, (count+127)/128))] = []byte("stale")
				got, err := f.ReadStorageBlockInto(block, make([]whisper.TimeSeriesPoint, 0, count))
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("read: %v, %v", got, err)
				}
				got[0].Value = 99
				again, err := f.ReadStorageBlock(block)
				if err != nil || !reflect.DeepEqual(again, want) {
					t.Fatal("read result aliases storage", err)
				}
				delete(kv.data, string(desc.groupKey(1, 0, 0, 0)))
				if _, err := f.ReadStorageBlock(block); err == nil {
					t.Fatal("missing group accepted")
				}
			})
		}
	}
}

func TestContinuationSingleGroupReadError(t *testing.T) {
	kv := appendTailSeed(t, 35)
	f := appendTailFile(kv)
	errRead := errors.New("read failed")
	// Descriptor reads happen first; an error must never become an empty block.
	kv.readError = errRead
	if _, err := f.ReadStorageBlock(appendTailBlock(35)); !errors.Is(err, errRead) {
		t.Fatalf("read error = %v", err)
	}
	kv.readError = nil
	key := appendTailGroupKey(t, kv)
	kv.data[string(key)] = store.EncodePointBlock(&store.PointBlock{})
	if _, err := f.ReadStorageBlock(appendTailBlock(35)); err == nil {
		t.Fatal("absent physical points accepted")
	}
}
