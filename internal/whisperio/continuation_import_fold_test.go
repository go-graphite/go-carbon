package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// importFoldClock is a store clock a test moves explicitly.
type importFoldClock struct{ now int }

func (c *importFoldClock) time() time.Time { return time.Unix(int64(c.now), 0) }

func openImportFoldStore(t testing.TB, source string, clock *importFoldClock, buckets bool, codec store.ContinuationCodec) *store.Store {
	t.Helper()
	db, err := openMemoryTestStore(t, filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: codec,
		IncrementalSource: source, Now: clock.time, SyncInterval: time.Hour, CacheSize: 16 << 20, MemTableSize: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); db = nil })
	return db
}

// gridCheckingCodec checks, after every successful Import and Update, that
// every populated block of the stored main file holds points that strictly
// increase on its archive's grid, read both point by point (ReadStorageBlock,
// as Fetch and the in-place paths read) and as raw bytes (ReadAt, which
// re-encodes the block, as go-whisper's rewrites and replays read it, e.g.
// buildRewrite's "archives[i].blocks[j].file.read"). An import fold, patch or
// direct write that stores a non-increasing or off-grid point fails the
// operation that wrote it instead of a later merge.
type gridCheckingCodec struct {
	*ContinuationCodec
	blocks atomic.Int64
	folds  atomic.Uint64
}

// Only import-result accounting is serialized; independent updates and fetches
// remain concurrent. A rewrite also removes its sidecar, so absence alone does
// not prove that this codec exercised the in-place path.
var gridImportMu sync.Mutex

func (c *gridCheckingCodec) Import(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata, clock func() time.Time) error {
	gridImportMu.Lock()
	before := ImportSidecarFolds(whisper.InPlaced)
	err := c.ContinuationCodec.Import(ctx, kv, path, m, clock)
	if err == nil {
		c.folds.Add(ImportSidecarFolds(whisper.InPlaced) - before)
	}
	gridImportMu.Unlock()
	if err != nil {
		return err
	}
	return c.check(kv, "import")
}

func (c *gridCheckingCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if err := c.ContinuationCodec.Update(ctx, kv, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	return c.check(kv, "update")
}

func (c *gridCheckingCodec) check(kv store.ContinuationIO, op string) error {
	n, err := checkContinuationGrid(kv)
	c.blocks.Add(int64(n))
	if err != nil {
		return fmt.Errorf("after %s: %w", op, err)
	}
	return nil
}

// checkContinuationGrid walks every populated block of kv's main continuation
// file and returns how many it checked, or the first block whose points do not
// strictly increase on its archive's grid or do not read as raw bytes.
func checkContinuationGrid(kv store.ContinuationIO) (int, error) {
	fs := newContinuationFiles(kv, true)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, nil))
	if err != nil {
		return 0, err
	}
	defer w.Close()
	if !w.IsCompressed() {
		return 0, nil
	}
	physical, err := w.ContinuationLayout()
	if err != nil {
		return 0, err
	}
	in, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0600)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	f := in.(*continuationFile)
	checked := 0
	var raw []byte
	for _, b := range physical.Blocks {
		if b.Count == 0 {
			continue
		}
		points, err := f.ReadStorageBlock(b)
		if err != nil {
			return checked, fmt.Errorf("archive %d block at %d: point read: %w", b.Archive, b.Offset, err)
		}
		previous := 0
		for i, p := range points {
			if p.Time <= previous || p.Time%b.Step != 0 {
				return checked, fmt.Errorf("archive %d block at %d point %d: time %d after %d on step %d: points must increase on the archive grid", b.Archive, b.Offset, i, p.Time, previous, b.Step)
			}
			previous = p.Time
		}
		if len(points) != b.Count || points[0].Time != b.First || previous != b.Last {
			return checked, fmt.Errorf("archive %d block at %d: %d points %d..%d, layout says %d points %d..%d", b.Archive, b.Offset, len(points), points[0].Time, previous, b.Count, b.First, b.Last)
		}
		if cap(raw) < b.Size {
			raw = make([]byte, b.Size)
		}
		if _, err := f.ReadAt(raw[:b.Size], int64(b.Offset)); err != nil {
			return checked, fmt.Errorf("archive %d block at %d: raw read: %w", b.Archive, b.Offset, err)
		}
		checked++
	}
	return checked, nil
}

// sidecarSource is a compressed CWhisper file with an out-of-order sidecar,
// written by go-whisper as the persister writes original files.
type sidecarSource struct {
	t     testing.TB
	path  string
	clock importFoldClock
}

func newSidecarSource(t testing.TB, dir string, sc oracleScenario, pointsPerBlock int) *sidecarSource {
	t.Helper()
	s := &sidecarSource{t: t, path: filepath.Join(dir, "source.wsp"), clock: importFoldClock{now: 1_700_000_000}}
	wr := make([]whisper.Retention, len(sc.rets))
	for i, r := range sc.rets {
		wr[i] = whisper.NewRetention(r.Step, r.Count)
	}
	w, err := whisper.CreateWithOptions(s.path, whisper.NewRetentionsNoPointer(wr), whisper.AggregationMethod(sc.method), sc.xff,
		&whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: pointsPerBlock, Now: s.clock.time})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *sidecarSource) open() *whisper.Whisper {
	s.t.Helper()
	w, err := whisper.OpenWithOptions(s.path, &whisper.Options{OutOfOrder: true, Now: s.clock.time})
	if err != nil {
		s.t.Fatal(err)
	}
	return w
}

func (s *sidecarSource) update(now int, pts []store.Point, merge bool) {
	s.t.Helper()
	s.clock.now = now
	w := s.open()
	input := make([]*whisper.TimeSeriesPoint, len(pts))
	for i, p := range pts {
		input[i] = &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value}
	}
	if err := w.UpdateMany(input); err != nil {
		s.t.Fatal(err)
	}
	if merge {
		if err := w.MergeOutOfOrder(); err != nil {
			s.t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		s.t.Fatal(err)
	}
}

func (s *sidecarSource) hasSidecar() bool {
	w := s.open()
	defer w.Close()
	return w.OutOfOrderPath() != ""
}

// mergedFetch serves the source as classic whisper's merge of its sidecar
// would: the conversion's own reference, an offline merge at the Unix epoch.
func (s *sidecarSource) mergedFetch(from, until int) *whisper.TimeSeries {
	s.t.Helper()
	path, cleanup, err := whisper.OfflineMergeOutOfOrderSnapshotIn(s.path, s.t.TempDir())
	if err != nil {
		s.t.Fatal(err)
	}
	defer cleanup()
	w, err := whisper.OpenWithOptions(path, &whisper.Options{Now: s.clock.time})
	if err != nil {
		s.t.Fatal(err)
	}
	defer w.Close()
	ts, err := w.Fetch(from, until)
	if err != nil || ts == nil {
		s.t.Fatalf("merged source fetch: %v", err)
	}
	return ts
}

func convertSource(t testing.TB, db *store.Store, name, path string) {
	t.Helper()
	ctx := context.Background()
	capture, err := ReadSnapshotIn(ctx, name, path, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	staged, err := db.StageFileSnapshot(ctx, capture)
	if err != nil {
		t.Fatalf("stage %s: %v", name, err)
	}
	if _, err := db.PublishFileOwnership(ctx, capture, staged); err != nil {
		t.Fatalf("publish %s: %v", name, err)
	}
}

func importFoldCounts() map[string]uint64 {
	counts := map[string]uint64{"error": ImportSidecarFoldErrors()}
	for _, r := range whisper.InPlaceResults() {
		counts[r.String()] = ImportSidecarFolds(r)
	}
	return counts
}

func importFoldDelta(before map[string]uint64) map[string]uint64 {
	delta := make(map[string]uint64)
	for k, v := range importFoldCounts() {
		if d := v - before[k]; d > 0 {
			delta[k] = d
		}
	}
	return delta
}

// TestImportFoldMatchesClassic converts CWhisper files with out-of-order
// sidecars (late, duplicate and skewed emitters over the in-place shapes, in
// both store layouts, with small and default blocks so folds also carry and
// overflow) three ways: with direct late writes folding the sidecar during the
// import, with direct late writes importing it verbatim for the next update to
// fold (the path before this change), and without direct late writes. Each
// conversion runs the store's own staging and publication, whose verification
// compares the physical state with the import repeated (and folded) from the
// source and the values with the source's merged snapshot.
//
// Right after conversion every slot of every archive of the folded import
// must read as the source merged by classic whisper's merge. Then the rest of
// the stream goes to every store and a classic whisper file: after every
// update the folded import must read exactly as the import-then-fold store,
// and where it differs from classic whisper, as the sidecar store does
// (folding may only remove differences from classic). The final snapshots of
// the two direct-write stores must be equal. After every import and update,
// each store's blocks must hold points strictly increasing on their archive's
// grid (gridCheckingCodec), so no fold, patch or direct write stores a block
// a later merge or rewrite cannot re-encode.
func TestImportFoldMatchesClassic(t *testing.T) {
	const groups, workers = 4, 2
	scenarios := inPlaceScenarios(true)
	if !testing.Short() {
		scenarios = inPlaceScenarios(false)
	}
	// Both layouts import the same immutable source and resume the same classic
	// state. Build that history once; each consumer owns its later updates.
	type fixture struct {
		once    sync.Once
		source  *sidecarSource
		classic *classicFile
		at      []int
		batches [][]store.Point
	}
	fixtures := make([]fixture, len(scenarios))
	root := t.TempDir()
	slots := make(chan struct{}, workers)

	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			t.Parallel()
			for group := 0; group < groups; group++ {
				t.Run(fmt.Sprintf("group=%d", group), func(t *testing.T) {
					t.Parallel()
					before := importFoldCounts()
					var folded, fixed, after, mismatch, imported int
					var gridBlocks int64

					type result struct {
						folded, fixed, after, mismatch, imported int
						gridBlocks                               int64
					}
					results := make([]result, len(scenarios))
					t.Run("scenarios", func(t *testing.T) {
						var cases sync.WaitGroup
						defer cases.Wait()
						for i, sc := range scenarios {
							if i%groups != group {
								continue
							}
							cases.Add(1)
							go func() {
								defer cases.Done()
								slots <- struct{}{}
								defer func() { <-slots }()
								t.Run(fmt.Sprintf("%d/%s/%s", i, sc.method, sc.schema), func(t *testing.T) {
									var folded, fixed, after, mismatch, imported int
									var gridBlocks int64

									name := fmt.Sprintf("fold.%d", i)

									f := &fixtures[i]
									f.once.Do(func() {
										dir := filepath.Join(root, fmt.Sprintf("scenario-%02d", i))
										if err := os.Mkdir(dir, 0700); err != nil {
											t.Fatal(err)
										}
										f.at, f.batches = inPlaceBatches(sc, int64(i))
										f.source = newSidecarSource(t, dir, sc, []int{0, 200}[i%2])
										f.classic = newClassicFile(t, sc)
										cut := len(f.batches) * 2 / 3
										for u := 0; u < cut; u++ {
											// Sources mix an early merge with a later sidecar.
											f.source.update(f.at[u], f.batches[u], u == cut/2)
											f.classic.update(f.at[u], f.batches[u])
										}
									})
									at, batches := f.at, f.batches
									cut := len(batches) * 2 / 3
									src := &sidecarSource{t: t, path: f.source.path, clock: f.source.clock}
									classic := &classicFile{t: t, path: f.classic.path, data: bytes.Clone(f.classic.data), now: f.classic.now}
									dir := filepath.Dir(src.path)

									if !src.hasSidecar() {
										return
									}
									imported++
									clock := &importFoldClock{now: at[cut-1]}
									codecs := []*gridCheckingCodec{
										{ContinuationCodec: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}},
										{ContinuationCodec: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true, keepImportedSidecars: true}},
										{ContinuationCodec: &ContinuationCodec{ReuseBlockKeys: true}},
									}
									fold := openImportFoldStore(t, dir, clock, buckets, codecs[0])
									keep := openImportFoldStore(t, dir, clock, buckets, codecs[1])
									legacy := openImportFoldStore(t, dir, clock, buckets, codecs[2])
									inPlace := codecs[0].folds.Load()
									for _, db := range []*store.Store{fold, keep, legacy} {
										convertSource(t, db, name, src.path)
									}
									if codecs[0].folds.Load() > inPlace {
										folded++
									}
									for _, r := range sc.rets {
										from := clock.now - r.Step*r.Count + r.Step
										got, err := fold.Fetch(context.Background(), name, from, clock.now)
										if err != nil {
											t.Fatal(err)
										}
										merged := src.mergedFetch(from, clock.now)
										ref := merged.Values()
										if got.FromTime != merged.FromTime() || got.Step != merged.Step() || len(got.Values) != len(ref) {
											t.Fatalf("%s (%s) step %d: fetch shape differs from the merged source", name, sc.schema, r.Step)
										}
										for j := range ref {
											if !sameValue(got.Values[j], ref[j]) {
												t.Fatalf("%s (%s) step %d slot %d after conversion: folded import %v, merged source %v", name, sc.schema, r.Step, got.FromTime+j*got.Step, got.Values[j], ref[j])
											}
										}
									}

									type fetched struct {
										index  int
										series []*store.Series
										err    error
									}
									jobs := [3]chan int{make(chan int), make(chan int), make(chan int)}
									answers := make(chan fetched, 3)
									var workers sync.WaitGroup
									for k, db := range []*store.Store{fold, keep, legacy} {
										workers.Add(1)
										go func() {
											defer workers.Done()
											rng := rand.New(rand.NewSource(int64(i)))
											for u := range jobs[k] {
												result := fetched{index: k, series: make([]*store.Series, len(sc.rets))}
												maintenance := store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute, Allow: func() bool { return rng.Intn(4) == 0 }}
												result.err = db.UpdateManyMaintained(context.Background(), name, batches[u], maintenance)
												for ri, r := range sc.rets {
													if result.err != nil {
														break
													}
													from := clock.now - r.Step*r.Count + r.Step
													result.series[ri], result.err = db.Fetch(context.Background(), name, from, clock.now)
												}
												answers <- result
											}
										}()
									}
									defer func() {
										for _, job := range jobs {
											close(job)
										}
										workers.Wait()
									}()
									for u := cut; u < len(batches); u++ {
										clock.now = at[u]
										for _, job := range jobs {
											job <- u
										}
										classic.update(at[u], batches[u])
										references := make([]*whisper.TimeSeries, len(sc.rets))
										for ri, r := range sc.rets {
											references[ri] = classic.series(clock.now - r.Step*r.Count + r.Step)
										}
										var fetchedSeries [3][]*store.Series
										for range jobs {
											result := <-answers
											if result.err != nil {
												t.Fatalf("%s update %d (store %d): %v", name, u, result.index, result.err)
											}
											fetchedSeries[result.index] = result.series
										}
										for ri, r := range sc.rets {
											series := [3]*store.Series{fetchedSeries[0][ri], fetchedSeries[1][ri], fetchedSeries[2][ri]}

											ref := references[ri].Values()
											for j := range ref {
												x, y, z, c := series[0].Values[j], series[1].Values[j], series[2].Values[j], ref[j]
												after++
												if !sameValue(x, y) {
													t.Fatalf("%s (%s) update %d step %d slot %d: folded import %v, import then fold %v, classic %v", name, sc.schema, u, r.Step, series[0].FromTime+j*series[0].Step, x, y, c)
												}
												if sameValue(x, c) {
													if !sameValue(z, c) {
														fixed++
													}
													continue
												}
												mismatch++
												if !sameValue(x, z) {
													t.Fatalf("%s (%s) update %d step %d slot %d: folded import %v, sidecar store %v, classic %v", name, sc.schema, u, r.Step, series[0].FromTime+j*series[0].Step, x, z, c)
												}
											}
										}
									}
									a, err := fold.Snapshot(context.Background(), name)
									if err != nil {
										t.Fatal(err)
									}
									b, err := keep.Snapshot(context.Background(), name)
									if err != nil {
										t.Fatal(err)
									}
									if err := store.EqualSnapshots(a, b); err != nil {
										t.Fatalf("%s: final snapshots of folded import and import then fold differ: %v", name, err)
									}
									for _, c := range codecs {
										gridBlocks += c.blocks.Load()
									}

									results[i] = result{folded, fixed, after, mismatch, imported, gridBlocks}
									t.Logf("%d sources with sidecars, %d folded, %d grid block reads", imported, folded, gridBlocks)
								})
							}()
						}
					})
					for _, r := range results {
						folded += r.folded
						fixed += r.fixed
						after += r.after
						mismatch += r.mismatch
						imported += r.imported
						gridBlocks += r.gridBlocks
					}

					if gridBlocks == 0 {
						t.Fatal("no stored block was checked for points increasing on the archive grid")
					}
					delta := importFoldDelta(before)
					if delta[whisper.InPlaced.String()] == 0 || folded == 0 {
						t.Fatal("no import folded its sidecar")
					}
					t.Logf("%d sources with sidecars, %d folded on import; import fold results %v; after conversion %d slots: %d fixed relative to the sidecar store, %d still differing from classic (identical to the sidecar store); %d stored block reads after imports and updates checked increasing on the archive grid",
						imported, folded, delta, after, fixed, mismatch, gridBlocks)

				})
			}
		})
	}
}

// importOnce imports path with codec into a fresh in-memory store and returns
// it with the fold result Import counted ("" when it attempted none).
func importOnce(t testing.TB, codec *ContinuationCodec, path string, m store.Metadata, now int) (*countingIO, string) {
	t.Helper()
	kv := &countingIO{mapContinuationIO: mapContinuationIO{}}
	before := importFoldCounts()
	if err := codec.Import(context.Background(), kv, path, m, func() time.Time { return time.Unix(int64(now), 0) }); err != nil {
		t.Fatal(err)
	}
	result := ""
	for k := range importFoldDelta(before) {
		result = k
	}
	return kv, result
}

func sameKV(a, b mapContinuationIO) error {
	if len(a) != len(b) {
		return fmt.Errorf("%d keys, want %d", len(a), len(b))
	}
	for k, v := range b {
		if w, ok := a[k]; !ok || string(w) != string(v) {
			return fmt.Errorf("key %q differs", k)
		}
	}
	return nil
}

func sourceMetadata(t testing.TB, path string) store.Metadata {
	t.Helper()
	capture, err := ReadSnapshotIn(context.Background(), "m", path, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return capture.Metadata
}

// TestImportFoldFallsBackToRewrite covers sidecars the import cannot
// fold in place: storage that cannot patch blocks, a fold that would overflow
// the current block, and one that would append past an archive's encoded
// range. Conversion and a live takeover must remove every sidecar while
// preserving the ordinary merge's values, with the rewrite reason counted.
func TestImportFoldFallsBackToRewrite(t *testing.T) {
	rets := []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 288}}
	sc := oracleScenario{rets: rets, method: store.Average}
	start := 1_700_000_000
	history := func(src *sidecarSource, from, until int) {
		var pts []store.Point
		for ts := from; ts < until; ts += 60 {
			pts = append(pts, store.Point{Timestamp: int64(ts), Value: float64(ts % 97)})
		}
		src.update(until, pts, false)
	}
	for _, tc := range []struct {
		name  string
		build func(t *testing.T) string
		setup func() func()
		want  string
	}{
		{
			name: "coarse",
			build: func(t *testing.T) string {
				src := newSidecarSource(t, t.TempDir(), sc, 0)
				history(src, start-6*3600, start)
				side, err := whisper.CreateWithOptions(whisper.OutOfOrderSidecarPath(src.path), whisper.MustParseRetentionDefs("60s:1d,600s:2d"), whisper.Average, 0, &whisper.Options{Now: src.clock.time})
				if err != nil {
					t.Fatal(err)
				}
				if err := side.UpdateMany([]*whisper.TimeSeriesPoint{{Time: start - 25*3600, Value: 17}}); err != nil {
					t.Fatal(err)
				}
				if err := side.Close(); err != nil {
					t.Fatal(err)
				}
				return src.path
			},
			want: whisper.InPlaced.String(),
		},
		{
			name: "older_format",
			build: func(t *testing.T) string {
				single := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}}, method: store.Average}
				src := newSidecarSource(t, t.TempDir(), single, 0)
				history(src, start-6*3600, start)
				src.update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: int64(start - 3*3600), Value: 17}}, false)
				// Version zero uses the same single-archive layout, without
				// the version-one propagation buffers.
				f, err := os.OpenFile(src.path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt([]byte{0}, int64(len("whisper_compressed"))); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				return src.path
			},
			want: whisper.InPlaceUnsupported.String(),
		},

		{
			// A late base point inside the stored range folds in place.
			name: "in_place",
			build: func(t *testing.T) string {
				src := newSidecarSource(t, t.TempDir(), sc, 0)
				history(src, start-6*3600, start)
				src.update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: int64(start - 3*3600), Value: 1000}}, false)
				return src.path
			},
			want: whisper.InPlaced.String(),
		},
		{
			// Storage that cannot patch blocks leaves the sidecar.
			name: "unsupported",
			build: func(t *testing.T) string {
				src := newSidecarSource(t, t.TempDir(), sc, 0)
				history(src, start-6*3600, start)
				src.update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: int64(start - 3*3600), Value: 1000}}, false)
				return src.path
			},
			setup: func() func() {
				inPlaceRollupPatches = false
				return func() { inPlaceRollupPatches = true }
			},
			want: whisper.InPlaceUnsupported.String(),
		},
		{
			// Constant history packs many points into small blocks; late
			// noisy replacements of nine hours of it no longer fit the blocks
			// they join, and the excess carries into the current block, which
			// would overflow and rotate the ring.
			name: "overflow",
			build: func(t *testing.T) string {
				src := newSidecarSource(t, t.TempDir(), sc, 0)
				var pts []store.Point
				for ts := start - 20*3600; ts < start; ts += 60 {
					pts = append(pts, store.Point{Timestamp: int64(ts), Value: 5})
				}
				src.update(start, pts, false)
				rng := rand.New(rand.NewSource(1))
				var late []store.Point
				for ts := start - 10*3600; ts < start-3600; ts += 60 {
					late = append(late, store.Point{Timestamp: int64(ts), Value: rng.Float64()})
				}
				src.update(start+60, append([]store.Point{{Timestamp: int64(start), Value: 1}}, late...), false)
				return src.path
			},
			want: whisper.InPlaceOverflow.String(),
		},
		{
			// With xFilesFactor .5, the last hour's sparse data never
			// completed a coarse window. Late points fill one, whose
			// recomputed aggregate lies past the coarse archive's encoded
			// range: folding would append it there, which only the merge's
			// rewrite orders as classic whisper does.
			name: "tail",
			build: func(t *testing.T) string {
				xsc := sc
				xsc.xff = .5
				src := newSidecarSource(t, t.TempDir(), xsc, 0)
				var pts []store.Point
				for ts := start - 6*3600; ts < start-3600; ts += 60 {
					pts = append(pts, store.Point{Timestamp: int64(ts), Value: float64(ts % 97)})
				}
				for ts := start - 3600; ts < start; ts += 600 {
					pts = append(pts, store.Point{Timestamp: int64(ts), Value: 3})
				}
				src.update(start, pts, false)
				var late []store.Point
				for ts := start - 1800 + 60; ts < start-1200; ts += 60 {
					late = append(late, store.Point{Timestamp: int64(ts), Value: 7})
				}
				src.update(start+60, append([]store.Point{{Timestamp: int64(start), Value: 1}}, late...), false)
				return src.path
			},
			want: whisper.InPlaceTail.String(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				defer tc.setup()()
			}
			path := tc.build(t)
			m := sourceMetadata(t, path)
			now := start + 120
			got, result := importOnce(t, &ContinuationCodec{DirectLateWrites: true}, path, m, now)
			verbatim, none := importOnce(t, &ContinuationCodec{DirectLateWrites: true, keepImportedSidecars: true}, path, m, now)
			if none != "" {
				t.Fatalf("verbatim import attempted a fold (%s)", none)
			}
			legacy, _ := importOnce(t, &ContinuationCodec{keepImportedSidecars: true}, path, m, now)
			if err := sameKV(verbatim.mapContinuationIO, legacy.mapContinuationIO); err != nil {
				t.Fatalf("verbatim imports differ: %v", err)
			}
			if result != tc.want {
				t.Fatalf("fold result %q, want %q", result, tc.want)
			}
			codec := &ContinuationCodec{DirectLateWrites: true}
			if err := codec.Verify(context.Background(), got, path, m); err != nil {
				t.Fatalf("verify: %v", err)
			}
			fs := newContinuationFiles(got, true)
			_, statErr := fs.Stat(whisper.OutOfOrderSidecarPath(fs.main()))

			clock := &importFoldClock{now: now}
			db := openImportFoldStore(t, filepath.Dir(path), clock, true, &ContinuationCodec{DirectLateWrites: true})
			convertSource(t, db, "converted", path)
			if err := db.CheckNoSidecars(); err != nil {
				t.Fatal(err)
			}
			scan, err := db.ScanSidecars(context.Background(), "", 100, true)
			if err != nil || len(scan.Sidecars) != 0 {
				t.Fatalf("conversion left physical sidecars: %+v %v", scan, err)
			}
			if !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("folded import kept a sidecar: %v", statErr)
			}
			gotSnapshot, err := codec.Snapshot(context.Background(), got, m, func() time.Time { return time.Unix(int64(now), 0) })
			if err != nil {
				t.Fatal(err)
			}
			wantSnapshot, err := codec.Snapshot(context.Background(), verbatim, m, func() time.Time { return time.Unix(int64(now), 0) })
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(wantSnapshot, gotSnapshot); err != nil {
				t.Fatal("rewrite changed merge values:", err)
			}
			// The same refused folds must also complete before a live direct
			// update accepts a newer correction beside a legacy sidecar.
			input := []store.Point{{Timestamp: int64(start - 3*3600), Value: 777.25}}
			liveClock := func() time.Time { return time.Unix(int64(now), 0) }
			unfolded := whisper.DirectLateWrites(whisper.DirectUnfolded)
			for _, kv := range []*countingIO{got, verbatim} {
				if err := codec.Update(context.Background(), kv, m, input, -1, liveClock, nil); err != nil {
					t.Fatal(err)
				}
				inventory, err := codec.InspectSidecar(kv)
				if err != nil || inventory.Present {
					t.Fatalf("live update left a sidecar: %+v %v", inventory, err)
				}
			}
			if whisper.DirectLateWrites(whisper.DirectUnfolded) != unfolded {
				t.Fatal("atomic live update diverted behind an unfolded sidecar")
			}
			gotSnapshot, err = codec.Snapshot(context.Background(), got, m, liveClock)
			if err != nil {
				t.Fatal(err)
			}
			wantSnapshot, err = codec.Snapshot(context.Background(), verbatim, m, liveClock)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(wantSnapshot, gotSnapshot); err != nil {
				t.Fatal("live fallback changed merge values:", err)
			}

		})
	}
}

// failingBlockReadsIO fails every walk of a compressed block's point groups,
// which only reading a stored block makes: the import writes blocks, the fold
// reads the ones it changes.
type failingBlockReadsIO struct{ *countingIO }

var errInjectedBlockRead = errors.New("injected block read failure")

func (f failingBlockReadsIO) Visit(p []byte, fn func([]byte, []byte) error) error {
	if len(p) >= 10 && p[0] == 'f' && p[9] == 'c' {
		return errInjectedBlockRead
	}
	return f.countingIO.Visit(p, fn)
}

// TestImportFoldErrorAborts makes the fold fail part way, reading a block
// it would change: conversion must fail without publishing any keys, and
// count the failed fold.
func TestImportFoldErrorAborts(t *testing.T) {
	sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 288}}, method: store.Average}
	start := 1_700_000_000
	src := newSidecarSource(t, t.TempDir(), sc, 0)
	var pts []store.Point
	for ts := start - 6*3600; ts < start; ts += 60 {
		pts = append(pts, store.Point{Timestamp: int64(ts), Value: float64(ts % 97)})
	}
	src.update(start, pts, false)
	src.update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: int64(start - 3*3600), Value: 1000}}, false)
	m := sourceMetadata(t, src.path)
	clock := func() time.Time { return time.Unix(int64(start+120), 0) }
	kv := failingBlockReadsIO{&countingIO{mapContinuationIO: mapContinuationIO{}}}
	failures, folds := ImportSidecarFoldErrors(), ImportSidecarFolds(whisper.InPlaced)

	if err := (&ContinuationCodec{DirectLateWrites: true}).Import(context.Background(), kv, src.path, m, clock); !errors.Is(err, errInjectedBlockRead) {
		t.Fatalf("fold must fail conversion: %v", err)
	}
	if ImportSidecarFoldErrors() != failures+1 || ImportSidecarFolds(whisper.InPlaced) != folds {
		t.Fatal("failed fold not counted")
	}
	if len(kv.mapContinuationIO) != 0 {
		t.Fatal("failed fold published keys")
	}

}

// cancellingIO cancels its context at the n-th operation on it. Like the
// store's own, its writes fail once the context is done.
type cancellingIO struct {
	*countingIO
	ctx       context.Context
	cancel    context.CancelFunc
	ops, at   int
	firstSet  int
	failedSet bool
}

func (c *cancellingIO) tick() {
	c.ops++
	if c.ops == c.at {
		c.cancel()
	}
}
func (c *cancellingIO) Get(k []byte) ([]byte, error) { c.tick(); return c.countingIO.Get(k) }
func (c *cancellingIO) Set(k, v []byte) error {
	c.tick()
	if c.firstSet == 0 {
		c.firstSet = c.ops
	}
	if err := c.ctx.Err(); err != nil {
		c.failedSet = true
		return err
	}
	return c.countingIO.Set(k, v)
}
func (c *cancellingIO) Visit(p []byte, fn func([]byte, []byte) error) error {
	c.tick()
	return c.countingIO.Visit(p, fn)
}

// TestImportFoldInterruptedCommitsNothing cancels an import with a fold at
// every operation it makes on the store in turn. The import and the fold are
// staged in memory, so until the commit's first write an interrupted import
// must have written nothing; a cancellation during the commit fails the
// import, whose caller drops the batch. Once no cancellation lands, the keys
// must equal an uninterrupted import's.
func TestImportFoldInterruptedCommitsNothing(t *testing.T) {
	sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 288}, {Step: 3600, Count: 168}}, method: store.Average}
	start := 1_700_000_000
	src := newSidecarSource(t, t.TempDir(), sc, 100)
	var pts []store.Point
	for ts := start - 20*3600; ts < start; ts += 60 {
		pts = append(pts, store.Point{Timestamp: int64(ts), Value: float64(ts % 97)})
	}
	src.update(start, pts, false)
	src.update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: int64(start - 3*3600), Value: 1000}, {Timestamp: int64(start - 7*3600), Value: 2000}}, false)
	m := sourceMetadata(t, src.path)
	codec := &ContinuationCodec{DirectLateWrites: true}
	want, result := importOnce(t, codec, src.path, m, start+120)
	if result != whisper.InPlaced.String() {
		t.Fatalf("fixture does not fold in place: %q", result)
	}
	clock := func() time.Time { return time.Unix(int64(start+120), 0) }
	before, during := 0, 0
	for at := 1; ; at++ {
		ctx, cancel := context.WithCancel(context.Background())
		kv := &cancellingIO{countingIO: &countingIO{mapContinuationIO: mapContinuationIO{}}, ctx: ctx, cancel: cancel, at: at}
		err := codec.Import(ctx, kv, src.path, m, clock)
		cancel()
		if err == nil {
			if at <= kv.ops {
				t.Fatalf("import cancelled at operation %d of %d succeeded", at, kv.ops)
			}
			if err := sameKV(kv.mapContinuationIO, want.mapContinuationIO); err != nil {
				t.Fatalf("uninterrupted import differs: %v", err)
			}
			break
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("operation %d: %v", at, err)
		}
		switch {
		case kv.firstSet == 0 || at < kv.firstSet:
			if len(kv.mapContinuationIO) != 0 {
				t.Fatalf("operation %d: import interrupted before its commit wrote %d keys", at, len(kv.mapContinuationIO))
			}
			before++
		case !kv.failedSet:
			t.Fatalf("operation %d: import interrupted during its commit failed without a failed write", at)
		default:
			during++
		}
	}
	if before < 2 || during == 0 {
		t.Fatalf("interruptions: %d before the commit, %d during it", before, during)
	}
	t.Logf("import interrupted at %d store operations before its commit (nothing written) and %d during it", before, during)
}

// importFoldStageCodec cancels the import's context at a chosen operation on
// the store, so a test can interrupt a stage anywhere in the import and fold.
type importFoldStageCodec struct {
	*ContinuationCodec
	at int
}

type cancellingStoreIO struct {
	store.ContinuationIO
	cancel  context.CancelFunc
	ops, at int
}

func (c *cancellingStoreIO) tick() {
	c.ops++
	if c.ops == c.at {
		c.cancel()
	}
}
func (c *cancellingStoreIO) TimeBuckets() bool {
	b, ok := c.ContinuationIO.(store.ContinuationTimeBuckets)
	return ok && b.TimeBuckets()
}
func (c *cancellingStoreIO) Get(k []byte) ([]byte, error) { c.tick(); return c.ContinuationIO.Get(k) }
func (c *cancellingStoreIO) Set(k, v []byte) error        { c.tick(); return c.ContinuationIO.Set(k, v) }
func (c *cancellingStoreIO) Visit(p []byte, fn func([]byte, []byte) error) error {
	c.tick()
	return c.ContinuationIO.Visit(p, fn)
}

func (c *importFoldStageCodec) Import(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata, clock func() time.Time) error {
	if c.at == 0 {
		return c.ContinuationCodec.Import(ctx, kv, path, m, clock)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return c.ContinuationCodec.Import(ctx, &cancellingStoreIO{ContinuationIO: kv, cancel: cancel, at: c.at}, path, m, clock)
}

// TestImportFoldStageInterruptedResumes interrupts the store's staging of a
// conversion whose import folds its sidecar at each operation the import
// makes on the store, then resumes it uninterrupted. An interrupted stage must
// leave no candidate (the resumed one is the metric's first generation) and
// the resumed conversion must publish, which verifies the folded physical
// state and the values, and read as the source merged.
func TestImportFoldStageInterruptedResumes(t *testing.T) {
	for _, ingest := range []bool{false, true} {
		t.Run(fmt.Sprintf("ingest=%v", ingest), func(t *testing.T) {
			sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 288}}, method: store.Average}
			start := 1_700_000_000
			dir := t.TempDir()
			src := newSidecarSource(t, dir, sc, 0)
			var pts []store.Point
			for ts := start - 6*3600; ts < start; ts += 60 {
				pts = append(pts, store.Point{Timestamp: int64(ts), Value: float64(ts % 97)})
			}
			src.update(start, pts, false)
			src.update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: int64(start - 3*3600), Value: 1000}}, false)
			codec := &importFoldStageCodec{ContinuationCodec: &ContinuationCodec{DirectLateWrites: true}}
			clock := &importFoldClock{now: start + 120}
			options := store.Options{ExactBlocks: true, Continuation: codec, IncrementalSource: dir, Now: clock.time, SyncInterval: time.Hour, CacheSize: 16 << 20, MemTableSize: 8 << 20}
			if ingest {
				options.IngestFileStages = true
				// Stages are submitted serially; these assertions do not test batching delay.
				options.IngestLinger = time.Nanosecond
			}
			db, err := openMemoryTestStore(t, filepath.Join(t.TempDir(), "shared"), options)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			capture, err := ReadSnapshotIn(ctx, "m", src.path, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			interrupted := 0
			for at := 1; ; at++ {
				codec.at = at
				before := importFoldCounts()
				staged, err := db.StageFileSnapshot(ctx, capture)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("operation %d: %v", at, err)
					}
					if _, err := db.Metadata(ctx, "m"); !errors.Is(err, store.ErrNotFound) {
						t.Fatalf("operation %d: interrupted stage visible: %v", at, err)
					}
					interrupted++
					continue
				}
				if importFoldDelta(before)[whisper.InPlaced.String()] != 1 {
					t.Fatal("the stage did not fold the sidecar")
				}
				if staged.Generation != 1 {
					t.Fatalf("an interrupted stage left a candidate: generation %d", staged.Generation)
				}
				codec.at = 0
				if _, err := db.PublishFileOwnership(ctx, capture, staged); err != nil {
					t.Fatalf("publish after %d interrupted stages: %v", interrupted, err)
				}
				break
			}
			if interrupted == 0 {
				t.Fatal("no stage was interrupted")
			}
			for _, r := range sc.rets {
				from := clock.now - r.Step*r.Count + r.Step
				got, err := db.Fetch(ctx, "m", from, clock.now)
				if err != nil {
					t.Fatal(err)
				}
				ref := src.mergedFetch(from, clock.now).Values()
				for j := range ref {
					if !sameValue(got.Values[j], ref[j]) {
						t.Fatalf("step %d slot %d: store %v, merged source %v", r.Step, got.FromTime+j*got.Step, got.Values[j], ref[j])
					}
				}
			}
			t.Logf("stage interrupted at each of its first %d store operations, then resumed", interrupted)
		})
	}
}

// TestImportFoldVerifyRejectsWrongFold checks that publication's physical
// verification of a folded import fails when the stored state is not the
// source's fold: a store holding the verbatim main file without its sidecar.
func TestImportFoldVerifyRejectsWrongFold(t *testing.T) {
	sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 288}}, method: store.Average}
	start := 1_700_000_000
	src := newSidecarSource(t, t.TempDir(), sc, 0)
	var pts []store.Point
	for ts := start - 6*3600; ts < start; ts += 60 {
		pts = append(pts, store.Point{Timestamp: int64(ts), Value: float64(ts % 97)})
	}
	src.update(start, pts, false)
	src.update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: int64(start - 3*3600), Value: 1000}}, false)
	m := sourceMetadata(t, src.path)
	verbatim, _ := importOnce(t, &ContinuationCodec{keepImportedSidecars: true}, src.path, m, start+120)
	fs := newContinuationFiles(verbatim, false)
	if err := fs.Remove(whisper.OutOfOrderSidecarPath(fs.main())); err != nil {
		t.Fatal(err)
	}
	if err := fs.flush(); err != nil {
		t.Fatal(err)
	}
	if err := (&ContinuationCodec{DirectLateWrites: true}).Verify(context.Background(), verbatim, src.path, m); err == nil {
		t.Fatal("verification accepted a main file without its sidecar's fold")
	}
}

// TestImportFoldWritesLess measures the bytes a conversion of a metric with
// a sidecar and its first update write to the store: importing the sidecar
// verbatim, which the update folds, against folding it during the import.
// Bytes are keys plus values set, as the codec hands them to the store.
func TestImportFoldWritesLess(t *testing.T) {
	type sample struct{ importBytes, updateBytes, importKeys, updateKeys int }
	type source struct {
		path   string
		meta   store.Metadata
		update []store.Point
		now    int
	}
	scenarios := inPlaceScenarios(true)
	sources := make([]source, len(scenarios))
	root := t.TempDir()
	t.Run("sources", func(t *testing.T) {
		for i, sc := range scenarios {
			t.Run(fmt.Sprintf("scenario=%02d", i), func(t *testing.T) {
				t.Parallel()
				at, batches := inPlaceBatches(sc, int64(i))
				cut := len(batches) * 2 / 3
				dir := filepath.Join(root, fmt.Sprint(i))
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				src := newSidecarSource(t, dir, sc, 0)
				for u := 0; u < cut; u++ {
					src.update(at[u], batches[u], false)
				}
				if src.hasSidecar() {
					sources[i] = source{src.path, sourceMetadata(t, src.path), batches[cut], at[cut]}
				}
			})
		}
	})
	var paths []string
	var metas []store.Metadata
	var updates [][]store.Point
	var nows []int
	for _, src := range sources {
		if src.path != "" {
			paths = append(paths, src.path)
			metas = append(metas, src.meta)
			updates = append(updates, src.update)
			nows = append(nows, src.now)
		}
	}

	totals := map[bool]*sample{false: {}, true: {}}
	folds := 0
	for k, path := range paths {
		var bytes [2]int
		for _, keep := range []bool{true, false} {
			codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true, keepImportedSidecars: keep}
			kv, result := importOnce(t, codec, path, metas[k], nows[k])
			if !keep && result == whisper.InPlaced.String() {
				folds++
			}
			s := totals[keep]
			s.importBytes += kv.bytes
			s.importKeys += kv.writes()
			imported := kv.bytes
			kv.sets, kv.deletes, kv.prefixes, kv.bytes = 0, 0, 0, 0
			clock := func() time.Time { return time.Unix(int64(nows[k]), 0) }
			if err := codec.Update(context.Background(), kv, metas[k], updates[k], -1, clock, nil); err != nil {
				t.Fatal(err)
			}
			s.updateBytes += kv.bytes
			s.updateKeys += kv.writes()
			bytes[map[bool]int{true: 0, false: 1}[keep]] = imported + kv.bytes
		}
		if bytes[1] > bytes[0] {
			t.Errorf("source %d: folding on import wrote %d bytes, importing then folding %d", k, bytes[1], bytes[0])
		}
	}
	if folds == 0 {
		t.Fatal("no import folded its sidecar")
	}
	n := len(paths)
	for _, keep := range []bool{true, false} {
		s := totals[keep]
		label := map[bool]string{true: "import, then fold on update", false: "fold on import"}[keep]
		t.Logf("%-28s per metric: import %7d B (%5d keys), first update %7d B (%5d keys), total %7d B", label,
			s.importBytes/n, s.importKeys/n, s.updateBytes/n, s.updateKeys/n, (s.importBytes+s.updateBytes)/n)
	}
	t.Logf("%d sources with sidecars, %d folded on import", n, folds)
}

// productionSidecarSource writes a metric as a long-lived original: 30 days
// of minutely history with values of a typical gauge, then a day of
// ten-minute writeouts, a third of which carry a late point between minAge
// and maxAge seconds old, most of which reach the sidecar.
func productionSidecarSource(t testing.TB, dir string, seed int64, minAge, maxAge int) (*sidecarSource, int) {
	t.Helper()
	sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}, method: store.Average}
	src := newSidecarSource(t, dir, sc, 0)
	rng := rand.New(rand.NewSource(seed))
	start := 1_700_000_000
	value := func() float64 { return float64(100 + rng.Intn(50)) }
	for day := 30; day > 1; day-- {
		var pts []store.Point
		for ts := start - day*86400; ts < start-(day-1)*86400; ts += 60 {
			pts = append(pts, store.Point{Timestamp: int64(ts), Value: value()})
		}
		src.update(start-(day-1)*86400, pts, false)
	}
	now := start - 86400
	for ; now < start; now += 600 {
		var pts []store.Point
		for ts := now; ts < now+600; ts += 60 {
			pts = append(pts, store.Point{Timestamp: int64(ts), Value: value()})
		}
		if rng.Intn(3) == 0 {
			ts := now - minAge - rng.Intn(maxAge-minAge)
			pts = append(pts, store.Point{Timestamp: int64(ts - ts%60), Value: value()})
		}
		src.update(now+600, pts, false)
	}
	return src, now
}

// TestImportFoldPebbleBytes converts production-shaped metrics with sidecars
// into a real store, then gives each one ordinary update, and reports the
// bytes Pebble wrote (WAL, flushed and ingested tables, before compaction)
// per metric: importing the sidecar verbatim, which the update folds, and
// folding it during the import. It also reports the keys plus values the
// codec handed the store. Set IMPORT_FOLD_METRICS to change the count.
func TestImportFoldPebbleBytes(t *testing.T) {
	var imports sync.Mutex
	n := 4
	if testing.Short() {
		n = 2
	}
	if v := os.Getenv("IMPORT_FOLD_METRICS"); v != "" {
		fmt.Sscan(v, &n)
	}
	for _, shape := range []struct {
		name           string
		minAge, maxAge int
		fold           bool
	}{
		// Corrections older than the coarse archives' buffered windows
		// fold in place.
		{"late by 4-14 days", 4 * 86400, 14 * 86400, true},
		// Recent late points change coarse windows still held in buffers,
		// past the archives' encoded ranges: neither the import nor the
		// update folds such a sidecar in place (rewrite_tail).
		{"late by up to 4 hours", 60, 4 * 3600, false},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			measureImportFoldBytes(t, n, shape.minAge, shape.maxAge, shape.fold, &imports)
		})
	}
}

func measureImportFoldBytes(t *testing.T, n, minAge, maxAge int, fold bool, imports *sync.Mutex) {
	dir := t.TempDir()
	sources := make([]*sidecarSource, n)
	ends := make([]int, n)
	metas := make([]store.Metadata, n)
	t.Run("sources", func(t *testing.T) {
		for i := 0; i < n; i++ {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Parallel()
				sub := filepath.Join(dir, fmt.Sprintf("m%d", i))
				if err := os.Mkdir(sub, 0o755); err != nil {
					t.Fatal(err)
				}
				src, now := productionSidecarSource(t, sub, int64(i), minAge, maxAge)
				if !src.hasSidecar() {
					t.Fatalf("source %d has no sidecar", i)
				}
				sources[i], ends[i], metas[i] = src, now, sourceMetadata(t, src.path)
			})
		}
	})
	end := ends[n-1]

	written := func(db *store.Store) uint64 {
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		var total uint64
		for kind := range store.FSKindNames {
			total += db.Stats().FS.Bytes[store.FSWrite][kind]
		}
		return total
	}
	label := map[bool]string{true: "import, then fold on update", false: "fold on import"}
	for _, ingest := range []bool{false, true} {
		var totals [2]uint64
		for k, keep := range []bool{true, false} {
			clock := &importFoldClock{now: end}
			codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true, keepImportedSidecars: keep}
			db, err := openMemoryTestStore(t, filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec,
				// Each stage is awaited before submitting the next, so no batch can form.
				IncrementalSource: dir, IngestFileStages: ingest, IngestLinger: time.Nanosecond, Now: clock.time, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 64 << 20})
			if err != nil {
				t.Fatal(err)
			}
			base := written(db)
			before := importFoldCounts()
			directBefore := whisper.DirectSidecarFolds(whisper.InPlaced)
			began := time.Now()
			for i, src := range sources {
				func() {
					// Keep exact import-result deltas isolated between the two shapes.
					imports.Lock()
					defer imports.Unlock()
					convertSource(t, db, fmt.Sprintf("m%d", i), src.path)
				}()
			}
			took := time.Since(began) / time.Duration(n)
			converted := written(db)
			clock.now = end + 60
			for i := range sources {
				if err := db.UpdateMany(context.Background(), fmt.Sprintf("m%d", i), []store.Point{{Timestamp: int64(end), Value: 1}}); err != nil {
					t.Fatal(err)
				}
			}
			updated := written(db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			totals[k] = updated - base
			t.Logf("ingest=%v %-28s per metric: conversion %8d B in %v, first update %8d B, total %8d B (import folds %v, update folds in place %d)", ingest, label[keep],
				(converted-base)/uint64(n), took.Round(time.Microsecond), (updated-converted)/uint64(n), (updated-base)/uint64(n), importFoldDelta(before), whisper.DirectSidecarFolds(whisper.InPlaced)-directBefore)
		}
		t.Logf("ingest=%v: folding on import writes %.1f%% of importing then folding", ingest, 100*float64(totals[1])/float64(totals[0]))
	}
	var logical [2][2]int
	folds := 0
	for k, keep := range []bool{true, false} {
		for i, src := range sources {
			m := metas[i]
			codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true, keepImportedSidecars: keep}
			kv, result := func() (*countingIO, string) {
				imports.Lock()
				defer imports.Unlock()
				return importOnce(t, codec, src.path, m, end)
			}()
			if result == whisper.InPlaced.String() {
				folds++
			}
			logical[k][0] += kv.bytes
			kv.bytes = 0
			if err := codec.Update(context.Background(), kv, m, []store.Point{{Timestamp: int64(end), Value: 1}}, -1, func() time.Time { return time.Unix(int64(end+60), 0) }, nil); err != nil {
				t.Fatalf("source %d: %v", i, err)
			}
			logical[k][1] += kv.bytes
		}
		t.Logf("codec %-28s per metric: import %8d B, first update %8d B, total %8d B", label[keep], logical[k][0]/n, logical[k][1]/n, (logical[k][0]+logical[k][1])/n)
	}
	if fold != (folds == n) {
		t.Fatalf("%d of %d imports folded in place", folds, n)
	}
	if fold && logical[1][0]+logical[1][1] >= logical[0][0]+logical[0][1] {
		t.Fatal("folding on import did not write less")
	}
}

// A full rewrite removes the sidecar too, but cannot satisfy the matrix's
// requirement that at least one import actually folded in place.
func TestImportFoldCoverageDistinguishesRewrite(t *testing.T) {
	for _, older := range []bool{false, true} {
		t.Run(fmt.Sprintf("older-format=%v", older), func(t *testing.T) {
			sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}}, method: store.Average}
			if !older {
				sc.rets = append(sc.rets, store.Retention{Step: 600, Count: 288})
			}
			const start = 1_700_000_000
			src := newSidecarSource(t, t.TempDir(), sc, 0)
			var points []store.Point
			for stamp := start - 6*3600; stamp < start; stamp += 60 {
				points = append(points, store.Point{Timestamp: int64(stamp), Value: float64(stamp % 97)})
			}
			src.update(start, points, false)
			src.update(start+60, []store.Point{{Timestamp: start, Value: 1}, {Timestamp: start - 3*3600, Value: 1000}}, false)
			if older {
				f, err := os.OpenFile(src.path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteAt([]byte{0}, int64(len("whisper_compressed")))
				closeErr := f.Close()
				if err := errors.Join(err, closeErr); err != nil {
					t.Fatal(err)
				}
			}
			codec := &gridCheckingCodec{ContinuationCodec: &ContinuationCodec{DirectLateWrites: true}}
			kv := &countingIO{mapContinuationIO: mapContinuationIO{}}
			if err := codec.Import(context.Background(), kv, src.path, sourceMetadata(t, src.path), func() time.Time { return time.Unix(start+120, 0) }); err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if older {
				want = 0
			}
			if got := codec.folds.Load(); got != want {
				t.Fatalf("in-place folds %d, want %d", got, want)
			}
			fs := newContinuationFiles(kv, true)
			if _, err := fs.Stat(whisper.OutOfOrderSidecarPath(fs.main())); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("import kept sidecar: %v", err)
			}
		})
	}
}
