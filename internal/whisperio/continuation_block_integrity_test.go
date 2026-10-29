package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
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
	validationSlots := make(chan struct{}, 4)
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
								unalignedCWhisper(t, path, clock)
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

							// Checkpoints own immutable encoded values: Set always copies its input.
							// Replay every update, including every full-file encoding, in independent
							// bounded phases. The reference pass records the same RNG decisions and
							// the final key/value image, so phase boundaries cannot change the trace.
							const phaseUpdates = 64
							var phases sync.WaitGroup
							defer phases.Wait()
							for first := 0; first < len(batches); first += phaseUpdates {
								end := min(first+phaseUpdates, len(batches))
								initial := maps.Clone(kv.mapContinuationIO)
								decisions := make([][]bool, end-first)
								for u := first; u < end; u++ {
									now = at[u]
									maintenance := store.ContinuationMaintenance{MinBytes: minBytes, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute,
										Allow: func() bool {
											allowed := rng.Intn(allow) == 0
											decisions[u-first] = append(decisions[u-first], allowed)
											return allowed
										}}
									if err := codec.Update(ctx, kv, m, batches[u], -1, clock, &maintenance); err != nil {
										t.Fatalf("checkpoint update %d at %d: %v", u, now, err)
									}
								}
								want := maps.Clone(kv.mapContinuationIO)
								validationSlots <- struct{}{}
								phases.Add(1)
								go func() {
									defer phases.Done()
									defer func() { <-validationSlots }()
									t.Run(fmt.Sprintf("updates=%d-%d", first, end-1), func(t *testing.T) {
										phaseKV := &indexedIntegrityIO{countingIO: &countingIO{mapContinuationIO: initial, buckets: buckets}}
										phaseNow := at[first]
										phaseClock := func() time.Time { return time.Unix(int64(phaseNow), 0) }
										var raw []byte
										for u := first; u < end; u++ {
											phaseNow = at[u]
											calls := 0
											maintenance := store.ContinuationMaintenance{MinBytes: minBytes, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute,
												Allow: func() bool {
													if calls == len(decisions[u-first]) {
														t.Fatalf("update %d requested an extra maintenance decision", u)
													}
													allowed := decisions[u-first][calls]
													calls++
													return allowed
												}}
											if err := codec.Update(ctx, phaseKV, m, batches[u], -1, phaseClock, &maintenance); err != nil {
												t.Fatalf("update %d at %d: %v", u, phaseNow, err)
											}
											if calls != len(decisions[u-first]) {
												t.Fatalf("update %d used %d maintenance decisions, want %d", u, calls, len(decisions[u-first]))
											}
											var err error
											raw, err = readWholeContinuationFileInto(phaseKV, phaseNow, raw)
											if err != nil {
												t.Fatalf("update %d at %d: %v", u, phaseNow, err)
											}
										}
										if err := sameKV(want, phaseKV.mapContinuationIO); err != nil {
											t.Fatalf("checkpoint after update %d: %v", end-1, err)
										}
									})
								}()
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
					for u, b := range batches {
						now = at[u]
						maintenance := store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute,
							Allow: func() bool { return rng.Intn(3) == 0 }}
						if err := db.UpdateManyMaintained(ctx, "m", b, maintenance); err != nil {
							t.Fatalf("update %d at %d: %v", u, at[u], err)
						}
						if rng.Intn(4) == 0 {
							if _, err := db.Fetch(ctx, "m", at[u]-20160*60, at[u]); err != nil {
								t.Fatalf("fetch %d: %v", u, err)
							}
						}
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
