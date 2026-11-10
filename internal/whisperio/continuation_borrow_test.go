package whisperio

import (
	"fmt"
	"math"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestClassicGroupBorrowPreservesWriterIsolation(t *testing.T) {
	for _, references := range []bool{false, true} {
		t.Run(fmt.Sprintf("references=%v", references), func(t *testing.T) {
			kv := &appendTailKV{data: map[string][]byte{}}
			buffer := &continuationPointBuffer{ContinuationIO: kv, cacheReads: true}
			f := appendTailFile(kv)
			f.fs.kv = buffer
			f.data.layout.Compressed = false
			f.data.references = references
			f.data.pages = map[uint32]bool{1: true}
			a := f.data.layout.Archives[0]
			key := continuationBodyKey(f.data.id, 'p', 0, 0, 0)
			original := &store.PointBlock{Present: [2]uint64{1}}
			original.Points[0] = store.Point{Timestamp: 1794000000, Value: math.Copysign(0, -1)}
			if err := kv.Set(key, store.EncodePointBlock(original)); err != nil {
				t.Fatal(err)
			}
			read := func(mutable bool) *store.PointBlock {
				t.Helper()
				block, err := f.classicGroup(0, a, 0, mutable)
				if err != nil {
					t.Fatal(err)
				}
				return block
			}
			read(false) // Populate the update-local read cache.
			borrowed := read(false)
			if borrowed != buffer.reads[string(key)] || read(false) != borrowed || kv.gets != 1 {
				t.Fatal("read-only accesses did not reuse the cached block")
			}
			owned := read(true)
			owned.Points[0].Value = 42
			if owned == borrowed || math.Float64bits(read(false).Points[0].Value) != 1<<63 {
				t.Fatal("an uncommitted mutation changed cached point bits")
			}
			if err := buffer.set(key, owned); err != nil {
				t.Fatal(err)
			}
			if got := read(false); got != buffer.blocks[string(key)] || got.Points[0].Value != 42 {
				t.Fatal("a pending write did not shadow the read cache")
			}
			uncommitted := read(true)
			uncommitted.Points[0].Value = 99
			if read(false).Points[0].Value != 42 {
				t.Fatal("an uncommitted mutation changed a pending write")
			}
			if err := buffer.flush(nil); err != nil {
				t.Fatal(err)
			}
			if got := read(false); got.Points[0].Value != 42 {
				t.Fatal("flush lost the stored value")
			}
			if err := buffer.Delete(key); err != nil {
				t.Fatal(err)
			}
			if read(false).Present != [2]uint64{} {
				t.Fatal("a deleted group remained in the read cache")
			}
		})
	}
}

var borrowedBlockResult *store.PointBlock

func BenchmarkContinuationPointBlockAccess(b *testing.B) {
	for _, mutable := range []bool{true, false} {
		b.Run(fmt.Sprintf("mutable=%v", mutable), func(b *testing.B) {
			key := []byte("group")
			buffer := &continuationPointBuffer{cacheReads: true}
			buffer.remember(key, &store.PointBlock{Present: [2]uint64{1}})
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				borrowedBlockResult = buffer.point(key, mutable)
			}
		})
	}
}
