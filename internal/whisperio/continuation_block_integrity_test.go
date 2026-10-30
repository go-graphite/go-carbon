package whisperio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// readWholeContinuationFile reads the main file's every byte, which encodes
// every stored block again, as each rewrite does.
func readWholeContinuationFile(kv store.ContinuationIO, now int) error {
	_, err := readWholeContinuationFileInto(kv, now, nil)
	return err
}

func readWholeContinuationFileInto(kv store.ContinuationIO, now int, buf []byte) ([]byte, error) {
	fs := newContinuationFiles(kv, true).withClock(func() time.Time { return time.Unix(int64(now), 0) })
	info, err := fs.Stat(fs.main())
	if err != nil {
		return nil, err
	}
	f, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if int64(cap(buf)) < info.Size() {
		buf = make([]byte, info.Size())
	} else {
		buf = buf[:info.Size()]
	}
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf, nil
}

// A block's encoder inputs are its layout, descriptor and ordered groups.
// Recheck changed inputs after every update; immutable unchanged inputs have
// already passed the same encoding. Keep full-file byte parity checked below.
type blockEncodingKey struct {
	archive whisper.StorageArchiveLayout
	index   int
}

type blockEncodingValidation map[blockEncodingKey][sha256.Size]byte

func (v blockEncodingValidation) check(kv *indexedIntegrityIO, now int) error {
	fs := newContinuationFiles(kv, true).withClock(func() time.Time { return time.Unix(int64(now), 0) })
	data, err := fs.load(filepath.Base(fs.main()))
	if err != nil {
		return err
	}
	f := &continuationFile{fs: fs, data: data}
	live := make(map[blockEncodingKey]bool)
	var scratch []whisper.TimeSeriesPoint
	for archive, a := range data.layout.Archives {
		for index := 0; index < a.BlockCount; index++ {
			key := blockEncodingKey{archive: a, index: index}
			live[key] = true
			desc, err := f.descriptor(archive, index)
			if err != nil {
				return err
			}
			h := sha256.New()
			h.Write(binary.BigEndian.AppendUint32(nil, uint32(desc.count)))
			prefix := desc.groupPrefix(data.id, archive, index)
			// File identities and time buckets locate the points, but do not
			// affect their encoding. Hash ordered relative keys and values so
			// unchanged blocks survive a file rewrite in the validation cache.
			if err := kv.Visit(prefix, func(key, value []byte) error {
				relative := key[len(prefix):]
				h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(relative))))
				h.Write(relative)
				h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(value))))
				h.Write(value)
				return nil
			}); err != nil {
				return err
			}
			digest := [sha256.Size]byte(h.Sum(nil))
			if previous, ok := v[key]; ok && previous == digest {
				continue
			}
			if _, err := f.readRawBlock(archive, index, a, &scratch); err != nil {
				return err
			}
			v[key] = digest
		}
	}
	for key := range v {
		if !live[key] {
			delete(v, key)
		}
	}
	return nil
}

func TestBlockEncodingValidationMatchesWholeFile(t *testing.T) {
	f := continuationReadFixture(t, 4096)
	kv := &indexedIntegrityIO{countingIO: &countingIO{mapContinuationIO: f.fs.kv.(mapContinuationIO)}}
	now := int(f.fs.clock().Unix())
	validation := blockEncodingValidation{}
	check := func(wantError bool) {
		t.Helper()
		cached := validation.check(kv, now)
		whole := readWholeContinuationFile(kv, now)
		if (cached != nil) != wantError || (whole != nil) != wantError {
			t.Fatalf("cached %v, whole file %v, want error %v", cached, whole, wantError)
		}
	}
	check(false)
	check(false)
	previous := continuationFilePrefix(f.data.id)
	nextID := f.data.id + 100
	moved := mapContinuationIO{}
	for key, value := range kv.mapContinuationIO {
		if strings.HasPrefix(key, string(previous)) {
			moved[string(append(continuationFilePrefix(nextID), key[len(previous):]...))] = value
		}
	}
	for key, value := range moved {
		if err := kv.Set([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Set(continuationNameKey(filepath.Base(f.fs.main())), binary.BigEndian.AppendUint64(nil, nextID)); err != nil {
		t.Fatal(err)
	}
	fs := newContinuationFiles(kv, true)
	file, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	f = file.(*continuationFile)
	check(false)
	for archive, a := range f.data.layout.Archives {
		for index := 0; index < a.BlockCount; index++ {
			desc, err := f.descriptor(archive, index)
			if err != nil {
				t.Fatal(err)
			}
			if desc.count < 2 {
				continue
			}
			key := desc.groupKey(f.data.id, archive, index, 0)
			original, err := kv.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			var group store.PointBlock
			if err := store.DecodePointBlock(original, &group); err != nil {
				t.Fatal(err)
			}
			group.Points[1].Timestamp = group.Points[0].Timestamp
			if err := kv.Set(key, store.EncodePointBlock(&group)); err != nil {
				t.Fatal(err)
			}
			check(true)
			if err := kv.Set(key, original); err != nil {
				t.Fatal(err)
			}
			check(false)
			return
		}
	}
	t.Fatal("fixture has no populated block")
}

// miscStream emits the sparse cron shape: one point a minute at an arbitrary
// second, mostly 0, flushed every 1-16 minutes, with late points of up to a
// quarter of the base retention, a few beyond it, and repeats.
func miscStream(rng *rand.Rand, start, duration, retention int, lateRate float64) (flushAt []int, batches [][]store.Point) {
	type arrival struct {
		at int
		p  store.Point
	}
	var arrivals []arrival
	for ts := start; ts < start+duration; ts += 60 {
		stamp := ts + rng.Intn(60)
		v := 0.0
		if rng.Intn(20) == 0 {
			v = 58
		}
		delay := 1 + rng.Intn(30)
		switch r := rng.Float64(); {
		case r < lateRate:
			delay = 120 + rng.Intn(retention/4)
		case r < lateRate*1.2:
			delay = retention/2 + rng.Intn(retention*2)
		}
		p := store.Point{Timestamp: int64(stamp), Value: v}
		arrivals = append(arrivals, arrival{stamp + delay, p})
		if rng.Float64() < .01 {
			arrivals = append(arrivals, arrival{stamp + delay + 1 + rng.Intn(600), p})
		}
	}
	sort.SliceStable(arrivals, func(i, j int) bool { return arrivals[i].at < arrivals[j].at })
	var cur []store.Point
	first, period := 0, 60*(1+rng.Intn(16))
	for _, a := range arrivals {
		if len(cur) > 0 && a.at-first >= period {
			flushAt, batches, cur = append(flushAt, first+period), append(batches, cur), nil
			period = 60 * (1 + rng.Intn(16))
		}
		if len(cur) == 0 {
			first = a.at
		}
		cur = append(cur, a.p)
	}
	return flushAt, batches
}

var integritySchemas = []struct {
	name string
	rets []store.Retention
	days int
}{
	{"1m:14d,30m:2y", []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, 20},
	{"1m:4h,30m:10d", []store.Retention{{Step: 60, Count: 240}, {Step: 1800, Count: 480}}, 2},
}

// TestRandomInterleavingsKeepBlocksEncodable drives the sparse shape's random
// interleavings of in-order writes, late points, sidecar merges and folds,
// rollup corrections and ring wraps through the codec, in both formats, from
// an empty metric and from a converted CWhisper file whose base block is off
// the grid. After every update every stored block must encode again, which
// every rewrite needs, and no update may fail.
func TestRandomInterleavingsKeepBlocksEncodable(t *testing.T) {
	testRandomInterleavingsKeepBlocksEncodable(t, 0)
}

func TestRandomInterleavingsKeepBlocksEncodableSeed1(t *testing.T) {
	testRandomInterleavingsKeepBlocksEncodable(t, 1)
}

func testRandomInterleavingsKeepBlocksEncodable(t *testing.T, selectedSeed int) {
	seeds := 2
	if testing.Short() {
		seeds = 1
	}
	slots := make(chan struct{}, 4)
	var cases sync.WaitGroup
	defer cases.Wait()
	for seed := selectedSeed; seed < min(selectedSeed+1, seeds); seed++ {
		for _, sc := range integritySchemas {
			for _, buckets := range []bool{false, true} {
				for _, converted := range []bool{false, true} {
					if converted && sc.rets[0].Count != 20160 {
						continue
					}
					cases.Add(1)
					go func() {
						defer cases.Done()
						slots <- struct{}{}
						defer func() { <-slots }()
						t.Run(fmt.Sprintf("seed=%d/%s/v7=%v/converted=%v", seed, sc.name, buckets, converted), func(t *testing.T) {
							rng := rand.New(rand.NewSource(int64(seed)))
							ctx := context.Background()
							m := store.Metadata{MetricConfig: store.MetricConfig{Name: "m", Retentions: sc.rets, AggregationMethod: store.Sum, XFilesFactor: []float32{0, .5}[rng.Intn(2)]}}
							codec := &ContinuationCodec{ReuseBlockKeys: true}
							kv := &indexedIntegrityIO{countingIO: &countingIO{mapContinuationIO: mapContinuationIO{}, buckets: buckets}}
							start := 1_792_400_400 + 7*60
							now := start
							clock := func() time.Time { return time.Unix(int64(now), 0) }
							if converted {
								m.XFilesFactor = 0 // the source's
								path := filepath.Join(t.TempDir(), "m.wsp")
								now = start + 60*3*1440
								// Size converted blocks for 512 points while retaining
								// every off-grid source point and the full retention.
								unalignedCWhisperWithBlockSize(t, path, clock, 512)
								if err := codec.Import(ctx, kv, path, m, clock); err != nil {
									t.Fatal(err)
								}
								start = now
							} else if err := codec.Create(ctx, kv, m, clock); err != nil {
								t.Fatal(err)
							}
							minBytes, allow := []int64{1 << 20, 65536, 4096}[rng.Intn(3)], []int{1, 3, 40}[rng.Intn(3)]
							retention := sc.rets[0].Step * sc.rets[0].Count
							at, batches := miscStream(rng, start, sc.days*86400, retention, []float64{.01, .05}[rng.Intn(2)])

							validation := blockEncodingValidation{}
							const phaseUpdates = 32
							for first := 0; first < len(batches); first += phaseUpdates {
								end := min(first+phaseUpdates, len(batches))
								t.Run(fmt.Sprintf("updates=%d-%d", first, end-1), func(t *testing.T) {
									for u := first; u < end; u++ {
										now = at[u]
										maintenance := store.ContinuationMaintenance{MinBytes: minBytes, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute,
											Allow: func() bool { return rng.Intn(allow) == 0 }}
										if err := codec.Update(ctx, kv, m, batches[u], -1, clock, &maintenance); err != nil {
											t.Fatalf("update %d at %d: %v", u, now, err)
										}
										if err := validation.check(kv, now); err != nil {
											t.Fatalf("update %d at %d: %v", u, now, err)
										}
									}
								})
							}

						})
					}()
				}
			}
		}
	}
}

// TestRandomInterleavingsThroughStore runs the same streams through the
// store, with and without the metadata cache, from a converted off-grid file,
// with fetches between updates, and fails on any update, merge or snapshot
// error.
func TestRandomInterleavingsThroughStore(t *testing.T) {
	seeds := 2
	if testing.Short() {
		seeds = 1
	}
	for seed := 0; seed < seeds; seed++ {
		for _, buckets := range []bool{false, true} {
			for _, cache := range []int64{0, 64 << 20} {
				t.Run(fmt.Sprintf("seed=%d/v7=%v/cache=%v", seed, buckets, cache > 0), func(t *testing.T) {
					t.Parallel()
					rng := rand.New(rand.NewSource(int64(seed)))
					ctx := context.Background()
					start := 1_792_400_400 + 7*60
					now := start + 60*3*1440
					clock := func() time.Time { return time.Unix(int64(now), 0) }
					root := t.TempDir()
					sourceDir := filepath.Join(root, "source")
					if err := os.Mkdir(sourceDir, 0700); err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(sourceDir, "m.wsp")
					unalignedCWhisper(t, path, clock)
					db, err := openMemoryTestStore(t, filepath.Join(root, "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true},
						IncrementalSource: sourceDir, Now: clock, TimeBuckets: buckets, MetadataCacheSize: cache,
						CacheSize: 8 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					if cache == 0 {
						defer db.DisableMetadataCacheForTest()()
					}
					capture, err := ReadSnapshotIn(ctx, "m", path, root)
					if err != nil {
						t.Fatal(err)
					}
					staged, err := db.StageFileSnapshot(ctx, capture)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := db.PublishFileOwnership(ctx, capture, staged); err != nil {
						t.Fatal(err)
					}
					aligned := whisper.RewriteBlockFallbackCount(whisper.RewriteBlockAligned)
					at, batches := miscStream(rng, now, 18*86400, 20160*60, .02)
					const phaseUpdates = 32
					for first := 0; first < len(batches); first += phaseUpdates {
						end := min(first+phaseUpdates, len(batches))
						t.Run(fmt.Sprintf("updates=%d-%d", first, end-1), func(t *testing.T) {
							for u := first; u < end; u++ {
								now = at[u]
								maintenance := store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute,
									Allow: func() bool { return rng.Intn(3) == 0 }}
								if err := db.UpdateManyMaintained(ctx, "m", batches[u], maintenance); err != nil {
									t.Fatalf("update %d at %d: %v", u, at[u], err)
								}
								if rng.Intn(4) == 0 {
									if _, err := db.Fetch(ctx, "m", at[u]-20160*60, at[u]); err != nil {
										t.Fatalf("fetch %d: %v", u, err)
									}
								}
							}
						})
					}
					if _, err := db.Snapshot(ctx, "m"); err != nil {
						t.Fatalf("snapshot: %v", err)
					}
					if whisper.RewriteBlockFallbackCount(whisper.RewriteBlockAligned) == aligned {
						t.Fatal("no rewrite aligned the converted block")
					}
				})
			}
		}
	}
}

func BenchmarkContinuationReadAt(b *testing.B) {
	for _, count := range []int{0, 4096} {
		b.Run(fmt.Sprintf("points=%d", count), func(b *testing.B) {
			f := continuationReadFixture(b, count)
			buf := make([]byte, f.data.layout.Size)
			b.ReportAllocs()
			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for range b.N {
				if _, err := f.ReadAt(buf, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func continuationReadFixture(tb testing.TB, count int) *continuationFile {
	tb.Helper()
	kv := mapContinuationIO{}
	codec := &ContinuationCodec{}
	now := 1_792_400_400
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "m", Retentions: integritySchemas[0].rets, AggregationMethod: store.Sum}}
	if err := codec.Create(context.Background(), kv, m, clock); err != nil {
		tb.Fatal(err)
	}
	if count > 0 {
		points := make([]store.Point, count)
		for i := range points {
			points[i] = store.Point{Timestamp: int64(now - (count-i)*60), Value: float64(i) + .1}
		}
		if err := codec.Update(context.Background(), kv, m, points, -1, clock, nil); err != nil {
			tb.Fatal(err)
		}
	}
	fs := newContinuationFiles(kv, true).withClock(clock)
	f, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { f.Close() })
	return f.(*continuationFile)
}

func TestContinuationReadAtMatchesIndividualBlocks(t *testing.T) {
	for _, count := range []int{0, 4096} {
		t.Run(fmt.Sprintf("points=%d", count), func(t *testing.T) {
			f := continuationReadFixture(t, count)
			want := make([]byte, f.data.layout.Size)
			copy(want, f.data.header)
			for archive, a := range f.data.layout.Archives {
				for index := 0; index < a.BlockCount; index++ {
					block := whisper.ContinuationBlock{Archive: archive, Index: index, Offset: a.Offset + index*a.BlockSize, Size: a.BlockSize, Step: a.Step}
					points, err := f.ReadStorageBlock(block)
					if err != nil {
						t.Fatal(err)
					}
					if len(points) == 0 {
						continue
					}
					block.First, block.Last, block.Count = points[0].Time, points[len(points)-1].Time, len(points)
					data, err := whisper.EncodeContinuationBlock(block, points)
					if err != nil {
						t.Fatal(err)
					}
					copy(want[block.Offset:], data)
				}
			}
			for _, span := range [][2]int{{0, len(want)}, {1, len(want) - 3}, {len(f.data.header) + 7, len(want) + 17}} {
				got := bytes.Repeat([]byte{0xff}, span[1]-span[0])
				n, err := f.ReadAt(got, int64(span[0]))
				end := min(span[1], len(want))
				if n != end-span[0] || !bytes.Equal(got[:n], want[span[0]:end]) {
					t.Fatalf("read %v: %d bytes differ from individual blocks", span, n)
				}
				if span[1] > len(want) {
					if !errors.Is(err, io.EOF) || !bytes.Equal(got[n:], bytes.Repeat([]byte{0xff}, len(got)-n)) {
						t.Fatal("EOF read changed trailing bytes or returned", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
