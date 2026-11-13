package whisperio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Materialize the previous reader's independently owned groups as a byte
// oracle. In particular, a missing group after a full group must not reuse
// the previous group's presence mask, timestamps, or float bits.
func classicReadScratchFixture(t testing.TB, buffered, references, sidecar bool) (*continuationFile, []byte) {
	t.Helper()
	kv := &appendTailKV{data: map[string][]byte{}}
	f := appendTailFile(kv)
	f.sidecar = sidecar
	f.data.references = references
	f.data.layout = whisper.StorageFileLayout{HeaderSize: 52, Size: 52, Archives: nil}
	f.data.header = bytes.Repeat([]byte{0x37}, 52)
	f.data.pages = map[uint32]bool{}
	for _, count := range []int{647, 259} {
		a := whisper.StorageArchiveLayout{Offset: f.data.layout.Size, Count: count, Step: 60}
		f.data.layout.Archives = append(f.data.layout.Archives, a)
		f.data.layout.Size += count * 12
	}
	for page := 0; page*4096 < f.data.layout.Size; page++ {
		f.data.pages[uint32(page)] = true
	}
	rng := rand.New(rand.NewSource(142))
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001}
	for ai, a := range f.data.layout.Archives {
		for gi := 0; gi*128 < a.Count; gi++ {
			if gi%3 == 1 {
				continue // allocated pages, absent physical group
			}
			var block store.PointBlock
			for slot := range 128 {
				if rng.Intn(4) == 0 {
					continue
				}
				block.Present[slot/64] |= uint64(1) << uint(slot%64)
				block.Points[slot] = store.Point{Timestamp: int64(rng.Uint32()), Value: math.Float64frombits(bits[slot%len(bits)])}
			}
			if err := kv.Set(continuationBodyKey(f.data.id, 'p', ai, 0, gi), store.EncodePointBlock(&block)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if buffered {
		f.fs.kv = &continuationPointBuffer{ContinuationIO: kv, cacheReads: true}
	}
	want := make([]byte, f.data.layout.Size)
	copy(want, f.data.header)
	for ai, a := range f.data.layout.Archives {
		for gi := 0; gi*128 < a.Count; gi++ {
			block, err := f.classicGroup(ai, a, gi, false)
			if err != nil {
				t.Fatal(err)
			}
			for slot := 0; slot < 128 && gi*128+slot < a.Count; slot++ {
				if block.Present[slot/64]&(uint64(1)<<uint(slot%64)) == 0 {
					continue
				}
				p := block.Points[slot]
				at := a.Offset + (gi*128+slot)*12
				binary.BigEndian.PutUint32(want[at:], uint32(p.Timestamp))
				binary.BigEndian.PutUint64(want[at+4:], math.Float64bits(p.Value))
			}
		}
	}
	return f, want
}

func TestClassicReadScratchMatchesOwnedGroups(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, references := range []bool{false, true} {
			for _, sidecar := range []bool{false, true} {
				t.Run(fmt.Sprintf("buffered=%t/references=%t/sidecar=%t", buffered, references, sidecar), func(t *testing.T) {
					f, want := classicReadScratchFixture(t, buffered, references, sidecar)
					rng := rand.New(rand.NewSource(103))
					spans := [][2]int{{0, len(want)}, {1, len(want) + 17}, {len(want), len(want) + 5}}
					for range 200 {
						start := rng.Intn(len(want))
						spans = append(spans, [2]int{start, start + rng.Intn(len(want)) + 1})
					}
					for _, span := range spans {
						got := bytes.Repeat([]byte{0xa5}, span[1]-span[0])
						n, err := f.ReadAt(got, int64(span[0]))
						end := min(span[1], len(want))
						if n != end-span[0] || !bytes.Equal(got[:n], want[span[0]:end]) {
							t.Fatalf("read %v: %d bytes differ from owned-group oracle", span, n)
						}
						if span[1] > len(want) {
							if !errors.Is(err, io.EOF) || !bytes.Equal(got[n:], bytes.Repeat([]byte{0xa5}, len(got)-n)) {
								t.Fatal("EOF handling changed", err)
							}
						} else if err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func TestClassicReadScratchPropagatesErrors(t *testing.T) {
	f, _ := classicReadScratchFixture(t, false, false, false)
	kv := f.fs.kv.(*appendTailKV)
	readErr := errors.New("storage read failed")
	kv.readError = readErr
	var scratch store.PointBlock
	if _, err := f.readClassicGroup(0, f.data.layout.Archives[0], 0, &scratch); !errors.Is(err, readErr) {
		t.Fatal("storage error lost:", err)
	}
	kv.readError = nil
	kv.data[string(continuationBodyKey(f.data.id, 'p', 0, 0, 0))] = []byte{0xff}
	if _, err := f.readClassicGroup(0, f.data.layout.Archives[0], 0, &scratch); err == nil {
		t.Fatal("malformed group accepted")
	}
}

var classicReadScratchResult *store.PointBlock

func BenchmarkClassicReadGroupScratch(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		b.Run(fmt.Sprintf("reuse=%t", reuse), func(b *testing.B) {
			f, _ := classicReadScratchFixture(b, false, false, false)
			a := f.data.layout.Archives[0]
			var scratch store.PointBlock
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if reuse {
					classicReadScratchResult, err = f.readClassicGroup(0, a, 0, &scratch)
				} else {
					classicReadScratchResult, err = f.classicGroup(0, a, 0, false)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
