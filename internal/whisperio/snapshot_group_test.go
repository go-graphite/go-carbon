package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Materialize the previous reader's independently owned groups as a byte
// oracle. In particular, a missing group after a full group must not reuse
// the previous group's presence mask, timestamps, or float bits.
func snapshotGroupFixture(t testing.TB, buffered, references, sidecar bool) (*continuationFile, []byte) {
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

func TestSnapshotGroupMatchesCurrentReader(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, references := range []bool{false, true} {
			for _, sidecar := range []bool{false, true} {
				t.Run(fmt.Sprintf("buffered=%t/references=%t/sidecar=%t", buffered, references, sidecar), func(t *testing.T) {
					f, want := snapshotGroupFixture(t, buffered, references, sidecar)
					f.fs.snapshotReads = true
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

func TestSnapshotGroupCodecBitExact(t *testing.T) {
	ctx := context.Background()
	for _, buckets := range []bool{false, true} {
		for _, compressed := range []bool{false, true} {
			t.Run(fmt.Sprintf("bucketed=%t/compressed=%t", buckets, compressed), func(t *testing.T) {
				m, kv, base := tailClassicSeed(t, store.Last, .5)
				var io store.ContinuationIO = kv
				if buckets {
					io = bucketedMapIO{kv}
				}
				clock := func() time.Time { return time.Unix(base, 0) }
				if compressed {
					io = mapContinuationIO{}
					if buckets {
						io = bucketedMapIO{mapContinuationIO{}}
					}
					codec := &ContinuationCodec{}
					if err := codec.Create(ctx, io, m, clock); err != nil {
						t.Fatal(err)
					}
					var pts []store.Point
					for j := 0; j < 600; j++ {
						pts = append(pts, store.Point{Timestamp: base - int64(600-j)*60, Value: float64(j)})
					}
					if err := codec.Update(ctx, io, m, pts, -1, clock, nil); err != nil {
						t.Fatal(err)
					}
				}
				old := &ContinuationCodec{keepSnapshotReadAllocations: true}
				want, err := old.Snapshot(ctx, io, m, clock)
				if err != nil {
					t.Fatal(err)
				}
				got, err := (&ContinuationCodec{}).Snapshot(ctx, io, m, clock)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(want, got); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

var snapshotGroupResult *store.PointBlock

func BenchmarkSnapshotGroupReuse(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		b.Run(fmt.Sprintf("reuse=%t", reuse), func(b *testing.B) {
			f, _ := snapshotGroupFixture(b, false, false, false)
			a := f.data.layout.Archives[0]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if reuse {
					snapshotGroupResult, err = f.classicSnapshotGroup(0, a, (i/2)%4)
				} else {
					snapshotGroupResult, err = f.classicGroup(0, a, (i/2)%4, false)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSnapshotGroupImportMergeMatchesCurrentReader(t *testing.T) {
	ctx := context.Background()
	for _, buckets := range []bool{false, true} {
		var expected []store.Snapshot
		for _, oracle := range []bool{true, false} {
			codec := &ContinuationCodec{keepSnapshotReadAllocations: oracle}
			opts := coldOptions(codec)
			opts.TimeBuckets = buckets
			opts.Now = classicTestClock
			db, err := openMemoryTestStore(t, t.TempDir(), opts)
			if err != nil {
				t.Fatal(err)
			}
			db.StopBackground()
			kv, snap := mixedClassicFixture(t)
			record := classicTransfer(kv, snap)
			for i := 0; i < 3; i++ {
				if _, err := db.ImportTransfer(ctx, []store.TransferMetric{record}); err != nil {
					t.Fatal(err)
				}
				got, err := db.Snapshot(ctx, record.Config.Name)
				if err != nil {
					t.Fatal(err)
				}
				if oracle {
					expected = append(expected, got)
				} else if err := store.EqualSnapshots(expected[i], got); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
