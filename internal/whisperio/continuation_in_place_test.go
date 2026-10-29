package whisperio

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// inPlaceScenarios are the oracle scenarios plus shapes that exercise the
// in-place paths often: the sparse emitter (late points every writeout, so
// buffered rollup corrections) and short base retentions whose small sidecars
// outlive the base archive (sidecar slot collisions).
func inPlaceScenarios(short bool) []oracleScenario {
	out := oracleScenarios(short)
	out = append(out,
		oracleScenario{"1m:1d,1h:7d,1d:30d sparse", []store.Retention{{Step: 60, Count: 1440}, {Step: 3600, Count: 168}, {Step: 86400, Count: 30}}, store.Average, 0, digestEmitter{cadence: 26, lateRate: .08}},
		oracleScenario{"1m:2h,10m:2d", []store.Retention{{Step: 60, Count: 120}, {Step: 600, Count: 288}}, store.Average, 0, digestEmitter{cadence: 60, lateRate: .03, dupRate: .03}},
		oracleScenario{"1m:2h,10m:2d sum", []store.Retention{{Step: 60, Count: 120}, {Step: 600, Count: 288}}, store.Sum, .5, digestEmitter{cadence: 20, jitter: 4, lateRate: .02, dupRate: .02}},
		oracleScenario{"10s:1h,1m:6h,10m:2d", []store.Retention{{Step: 10, Count: 360}, {Step: 60, Count: 360}, {Step: 600, Count: 288}}, store.Max, 0, digestEmitter{cadence: 10, jitter: 3, lateRate: .01, dupRate: .02}},
	)
	return out
}

// inPlaceBatches is runOracleScenario's stream: history up to start, a day of
// the emitter, and a future point in the fourth batch that seeds a replay.
func inPlaceBatches(sc oracleScenario, seed int64) (at []int, batches [][]store.Point) {
	start := 1_700_000_000
	step := sc.rets[0].Step
	history := min(step*sc.rets[0].Count+2*step, 35*86400)
	chunk := 86400
	if step*sc.rets[0].Count < chunk {
		chunk = step * sc.rets[0].Count / 3
	}
	from := start - history
	for ts := from - from%step; ts < start; ts += chunk {
		var pts []store.Point
		for x := ts; x < ts+chunk && x < start; x += step {
			pts = append(pts, store.Point{Timestamp: int64(x), Value: float64(x % 97)})
		}
		at, batches = append(at, min(ts+chunk, start)), append(batches, pts)
	}
	flushAt, stream := sc.e.batches(rand.New(rand.NewSource(seed)), start)
	for i, b := range stream {
		if i == 3 {
			b = append(b, store.Point{Timestamp: int64(flushAt[i] + 30), Value: 5})
		}
		at, batches = append(at, flushAt[i]), append(batches, b)
	}
	return at, batches
}

// TestInPlaceMatchesRewrite drives two stores in lockstep, one taking buffered
// rollup corrections and sidecar slot collisions in place and one rewriting
// and replaying the whole file as before, in both formats, next to a classic
// whisper file. After every update, every slot of every archive's fetch must
// equal the rewrite's, or else classic whisper's: the replay's rewrite is
// known to leave rollups that differ from classic (see
// TestContinuationMatchesClassicWhisper), and avoiding it can only remove
// such a difference. The final snapshots must match within retention.
func TestInPlaceMatchesRewriteV6Group0(t *testing.T) {
	testInPlaceMatchesRewrite(t, false, false, 0)
}

func TestInPlaceMatchesRewriteV6Group1(t *testing.T) {
	testInPlaceMatchesRewrite(t, false, false, 1)
}

func TestInPlaceMatchesRewriteV6EarlyGroup0(t *testing.T) {
	testInPlaceMatchesRewrite(t, false, true, 0)
}

func TestInPlaceMatchesRewriteV6EarlyGroup1(t *testing.T) {
	testInPlaceMatchesRewrite(t, false, true, 1)
}

func TestInPlaceMatchesRewriteV7Group0(t *testing.T) {
	testInPlaceMatchesRewrite(t, true, false, 0)
}

func TestInPlaceMatchesRewriteV7Group1(t *testing.T) {
	testInPlaceMatchesRewrite(t, true, false, 1)
}

func TestInPlaceMatchesRewriteV7EarlyGroup0(t *testing.T) {
	testInPlaceMatchesRewrite(t, true, true, 0)
}

func TestInPlaceMatchesRewriteV7EarlyGroup1(t *testing.T) {
	testInPlaceMatchesRewrite(t, true, true, 1)
}

const inPlaceResultFile = "GO_CARBON_INPLACE_RESULT_FILE"

type inPlaceCaseResult struct{ Updates, Rollups, Merges, Folds, Closer, Replays int }

// Each process owns the package hooks and counters. Its matrix owner still
// checks every case's combined path coverage after collecting the results.
func runInPlaceProcess(t *testing.T) *inPlaceCaseResult {
	t.Helper()
	if os.Getenv(inPlaceResultFile) != "" || testing.CoverMode() != "" {
		return nil
	}
	resultFile := filepath.Join(t.TempDir(), "result.json")
	parts := strings.Split(t.Name(), "/")
	for i := range parts {
		parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	args := []string{"-test.run=" + strings.Join(parts, "/"), "-test.count=1", "-test.timeout=55s", "-test.v",
		fmt.Sprintf("-test.short=%v", testing.Short())}
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	// Cleanup closes both stores before exit; each child needs no extra
	// race-detector exit delay after its test has finished.
	cmd.Env = append(os.Environ(), inPlaceResultFile+"="+resultFile,
		"GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("in-place case: %v\n%s", err, output)
	}
	if testing.Verbose() {
		t.Logf("%s", output)
	}
	data, err := os.ReadFile(resultFile)
	if err != nil {
		t.Fatal(err)
	}
	var result inPlaceCaseResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return &result
}

func testInPlaceMatchesRewrite(t *testing.T, selectedBuckets, selectedEarly bool, selectedGroup int) {
	var updateMu sync.Mutex
	// Bound simultaneous instrumented runtimes; each case owns two stores.
	slots := make(chan struct{}, 4)
	for _, mode := range []struct{ buckets, early bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		if mode.buckets != selectedBuckets || mode.early != selectedEarly {
			continue
		}
		buckets := mode.buckets
		t.Run(fmt.Sprintf("v7=%v/early-folds=%v", buckets, mode.early), func(t *testing.T) {
			t.Parallel()
			for group := 0; group < 2; group++ {
				if group != selectedGroup {
					continue
				}
				t.Run(fmt.Sprintf("group=%d", group), func(t *testing.T) {
					t.Parallel()

					replaysBefore := whisper.OverlapUpdates(whisper.OverlapSidecarSlot)
					var updates, rollups, merges, folds, closer, replays int
					results := make([]inPlaceCaseResult, len(inPlaceScenarios(testing.Short())))
					t.Run("scenarios", func(t *testing.T) {
						var cases sync.WaitGroup
						defer cases.Wait()
						for i, sc := range inPlaceScenarios(testing.Short()) {
							if i%2 != group {
								continue
							}
							cases.Add(1)
							go func() {
								defer cases.Done()
								slots <- struct{}{}
								defer func() { <-slots }()
								t.Run(fmt.Sprintf("scenario=%02d", i), func(t *testing.T) {
									if result := runInPlaceProcess(t); result != nil {
										results[i] = *result
										return
									}

									caseReplaysBefore := whisper.OverlapUpdates(whisper.OverlapSidecarSlot)
									clockNow := 1_700_000_000
									open := func() *store.Store {
										db, err := openMemoryTestStore(t, filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: &ContinuationCodec{ReuseBlockKeys: true},
											Now: func() time.Time { return time.Unix(int64(clockNow), 0) }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20})
										if err != nil {
											t.Fatal(err)
										}
										t.Cleanup(func() { db.Close(); db = nil })
										return db
									}
									inPlace, rewrite := open(), open()
									var updates, rollups, merges, folds, closer int

									name := fmt.Sprintf("inplace.%d", i)
									for _, db := range []*store.Store{inPlace, rewrite} {
										if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: sc.rets, AggregationMethod: sc.method, XFilesFactor: sc.xff}); err != nil {
											t.Fatal(err)
										}
									}
									rngs := []*rand.Rand{rand.New(rand.NewSource(int64(i))), rand.New(rand.NewSource(int64(i)))}
									at, batches := inPlaceBatches(sc, int64(i))
									classic := newClassicFile(t, sc)
									for u, b := range batches {
										clockNow = at[u]
										for k, db := range []*store.Store{inPlace, rewrite} {
											func() {
												updateMu.Lock()
												defer updateMu.Unlock()
												defer func(v bool) { inPlaceRollupPatches = v }(inPlaceRollupPatches)
												defer func(v bool) { earlySidecarFolds = v }(earlySidecarFolds)

												inPlaceRollupPatches = k == 0
												earlySidecarFolds = mode.early
												rng := rngs[k]
												r, m, f := whisper.RollupCorrections(whisper.InPlaced), whisper.SidecarSlotMerges(whisper.InPlaced), whisper.SidecarFolds(whisper.InPlaced)
												maintenance := store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute, Allow: func() bool { return rng.Intn(4) == 0 }}
												if err := db.UpdateManyMaintained(context.Background(), name, b, maintenance); err != nil {
													t.Fatalf("%s update %d (in place %v): %v", name, u, k == 0, err)
												}
												if k == 0 {
													rollups += int(whisper.RollupCorrections(whisper.InPlaced) - r)
													merges += int(whisper.SidecarSlotMerges(whisper.InPlaced) - m)
													folds += int(whisper.SidecarFolds(whisper.InPlaced) - f)
												} else if whisper.RollupCorrections(whisper.InPlaced) != r || whisper.SidecarSlotMerges(whisper.InPlaced) != m || whisper.SidecarFolds(whisper.InPlaced) != f {
													t.Fatalf("%s update %d: the rewriting store patched in place", name, u)
												}
											}()
										}
										updates++
										classic.update(at[u], b)
										for _, r := range sc.rets {
											from := clockNow - r.Step*r.Count + r.Step
											got, err := inPlace.Fetch(context.Background(), name, from, clockNow)
											if err != nil {
												t.Fatal(err)
											}
											want, err := rewrite.Fetch(context.Background(), name, from, clockNow)
											if err != nil {
												t.Fatal(err)
											}
											if sameSeries(got, want) {
												continue
											}
											series := classic.series(from)
											ref := series.Values()
											if got.FromTime != want.FromTime || got.Step != want.Step || len(got.Values) != len(want.Values) || len(ref) != len(got.Values) || series.FromTime() != got.FromTime {
												t.Fatalf("%s (%s) update %d step %d: in-place fetch shape differs", name, sc.schema, u, r.Step)
											}
											for j := range got.Values {
												x, y, c := got.Values[j], want.Values[j], ref[j]
												if sameValue(x, y) {
													continue
												}
												if !sameValue(x, c) {
													t.Fatalf("%s (%s) update %d step %d slot %d: in place %v, rewrite %v, classic %v", name, sc.schema, u, r.Step, got.FromTime+j*got.Step, x, y, c)
												}
												closer++
											}
										}
									}
									if closer > 0 {
										t.Logf("%s (%s): %d slots differ from the rewrite, all matching classic whisper", name, sc.schema, closer)
									}
									a, err := inPlace.Snapshot(context.Background(), name)
									if err != nil {
										t.Fatal(err)
									}
									b, err := rewrite.Snapshot(context.Background(), name)
									if err != nil {
										t.Fatal(err)
									}
									// Block boundaries differ between the two, so the ring can recycle
									// different blocks: physical points past an archive's retention,
									// which no read serves, may differ. Within retention a point may
									// differ only where it matches classic whisper.
									ra, rb := retained(a.Archives, clockNow), retained(b.Archives, clockNow)
									for ai := range ra {
										r := sc.rets[ai]
										from := clockNow - r.Step*r.Count + r.Step
										ref := map[int64]float64{}
										series := classic.series(from)
										for j, v := range series.Values() {
											ref[int64(series.FromTime()+j*series.Step())] = v
										}
										pa, pb := map[int64]float64{}, map[int64]float64{}
										for _, p := range ra[ai].Points {
											pa[p.Timestamp] = p.Value
										}
										for _, p := range rb[ai].Points {
											pb[p.Timestamp] = p.Value
										}
										for ts := range pb {
											if _, ok := pa[ts]; !ok {
												pa[ts] = math.NaN()
											}
										}
										for ts, x := range pa {
											y, ok := pb[ts]
											if !ok {
												y = math.NaN()
											}
											if sameValue(x, y) {
												continue
											}
											c, ok := ref[ts]
											if !ok {
												c = math.NaN()
											}
											if !sameValue(x, c) {
												t.Fatalf("%s (%s): snapshot archive %d at %d: in place %v, rewrite %v, classic %v", name, sc.schema, ai, ts, x, y, c)
											}
										}
									}

									results[i] = inPlaceCaseResult{updates, rollups, merges, folds, closer, int(whisper.OverlapUpdates(whisper.OverlapSidecarSlot) - caseReplaysBefore)}
									if path := os.Getenv(inPlaceResultFile); path != "" {
										data, err := json.Marshal(results[i])
										if err != nil {
											t.Fatal(err)
										}
										if err := os.WriteFile(path, data, 0600); err != nil {
											t.Fatal(err)
										}
									}
								})
							}()
						}
					})
					for _, r := range results {
						updates += r.Updates
						rollups += r.Rollups
						merges += r.Merges
						folds += r.Folds
						closer += r.Closer
						replays += r.Replays
					}
					// The owner checks the combined results; a child only supplies its case.
					if os.Getenv(inPlaceResultFile) != "" {
						for _, k := range whisper.InPlaceResults() {
							t.Logf("results %s: rollups %d, sidecar merges %d, sidecar folds %d", k, whisper.RollupCorrections(k), whisper.SidecarSlotMerges(k), whisper.SidecarFolds(k))
						}
						return
					}
					if testing.CoverMode() != "" {
						replays = int(whisper.OverlapUpdates(whisper.OverlapSidecarSlot) - replaysBefore)
					}
					// Early folds remove small sidecars before they collide, so each
					// mode must exercise its own path.
					if rollups == 0 || !mode.early && merges == 0 || mode.early && folds == 0 {
						t.Fatalf("in-place paths not exercised: %d rollup corrections, %d sidecar merges, %d early sidecar folds", rollups, merges, folds)
					}
					t.Logf("early in-place sidecar folds: %d", folds)
					t.Logf("%d updates; %d slots differ from the rewrite, all matching classic; in place: %d rollup corrections, %d sidecar merges; %d sidecar_slot replays left", updates, closer, rollups, merges, replays)

				})
			}
		})
	}
}

// retained drops snapshot points older than their archive's retention.
func retained(archives []store.Archive, now int) []store.Archive {
	out := make([]store.Archive, len(archives))
	for i, a := range archives {
		out[i].Retention = a.Retention
		for _, p := range a.Points {
			if p.Timestamp > int64(now-a.Retention.Step*a.Retention.Count) {
				out[i].Points = append(out[i].Points, p)
			}
		}
	}
	return out
}

// countingIO counts the writes of a continuation update.
type countingIO struct {
	mapContinuationIO
	buckets         bool
	sets, deletes   int
	prefixes, bytes int
}

func (c *countingIO) Set(k, v []byte) error {
	c.sets++
	c.bytes += len(k) + len(v)
	return c.mapContinuationIO.Set(k, v)
}
func (c *countingIO) Delete(k []byte) error { c.deletes++; return c.mapContinuationIO.Delete(k) }
func (c *countingIO) DeletePrefix(p []byte) error {
	// The store turns a prefix delete into a tombstone per key.
	for k := range c.mapContinuationIO {
		if strings.HasPrefix(k, string(p)) {
			c.prefixes++
		}
	}
	return c.mapContinuationIO.DeletePrefix(p)
}
func (c *countingIO) TimeBuckets() bool { return c.buckets }
func (c *countingIO) writes() int       { return c.sets + c.deletes + c.prefixes }

// inPlaceKeyStream builds a stream that triggers both in-place paths: a sparse
// emitter (late points every writeout) for buffered rollup corrections, and in
// the short-retention schema a few early late points whose small sidecar later
// collides with ordinary in-order batches.
func inPlaceKeyStream(sidecar bool) (store.Metadata, []int, [][]store.Point) {
	if !sidecar {
		sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}, {Step: 3600, Count: 168}, {Step: 86400, Count: 30}}, method: store.Average, e: digestEmitter{cadence: 26, lateRate: .08}}
		at, batches := inPlaceBatches(sc, 1)
		return store.Metadata{MetricConfig: store.MetricConfig{Name: "keys", Retentions: sc.rets, AggregationMethod: sc.method}}, at, batches
	}
	rets := []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 4320}, {Step: 3600, Count: 8760}}
	start := 1_700_000_000
	var at []int
	var batches [][]store.Point
	for ts := start - 10*86400; ts < start; ts += 86400 {
		pts := make([]store.Point, 0, 1440)
		for x := ts; x < ts+86400; x += 60 {
			pts = append(pts, store.Point{Timestamp: int64(x), Value: float64(x % 89)})
		}
		at, batches = append(at, ts+86400), append(batches, pts)
	}
	for ts := start; ts < start+30*3600; ts += 600 {
		var pts []store.Point
		for x := ts; x < ts+600; x += 60 {
			pts = append(pts, store.Point{Timestamp: int64(x), Value: float64(x % 89)})
		}
		if ts < start+1800 {
			// Late corrections of points written an hour ago go to the
			// sidecar; a day later its slots collide with in-order batches.
			pts = append(pts, store.Point{Timestamp: int64(ts - 3600 + 60), Value: 1000})
		}
		at, batches = append(at, ts+600), append(batches, pts)
	}
	return store.Metadata{MetricConfig: store.MetricConfig{Name: "keys", Retentions: rets, AggregationMethod: store.Average}}, at, batches
}

// TestInPlaceKeepsFileAndWritesLess runs the same stream with and without the
// in-place paths and checks, for every update the in-place run patched, that
// the main file kept its identity there and the update wrote far fewer keys
// than the rewrite or replay of the other run.
func TestInPlaceKeepsFileAndWritesLess(t *testing.T) {
	defer func(v bool) { inPlaceRollupPatches = v }(inPlaceRollupPatches)
	for _, buckets := range []bool{false, true} {
		for _, sidecar := range []bool{false, true} {
			t.Run(fmt.Sprintf("v7=%v/sidecar=%v", buckets, sidecar), func(t *testing.T) {
				m, at, batches := inPlaceKeyStream(sidecar)
				codec := &ContinuationCodec{ReuseBlockKeys: true}
				kvs := []*countingIO{{mapContinuationIO: mapContinuationIO{}, buckets: buckets}, {mapContinuationIO: mapContinuationIO{}, buckets: buckets}}
				now := at[0]
				clock := func() time.Time { return time.Unix(int64(now), 0) }
				for _, kv := range kvs {
					if err := codec.Create(context.Background(), kv, m, clock); err != nil {
						t.Fatal(err)
					}
				}
				counter := whisper.RollupCorrections
				if sidecar {
					counter = whisper.SidecarSlotMerges
				}
				var patched, inPlaceWrites, rewriteWrites int
				for u, b := range batches {
					now = at[u]
					var writes [2]int
					var ids [2][2]string
					hit := false
					for k, kv := range kvs {
						inPlaceRollupPatches = k == 0
						before, count := kv.writes(), counter(whisper.InPlaced)
						ids[k][0] = string(kv.mapContinuationIO["n/main.wsp"])
						if err := codec.Update(context.Background(), kv, m, b, -1, clock, nil); err != nil {
							t.Fatal(err)
						}
						ids[k][1] = string(kv.mapContinuationIO["n/main.wsp"])
						writes[k] = kv.writes() - before
						if k == 0 && counter(whisper.InPlaced) != count {
							hit = true
						}
					}
					if !hit {
						continue
					}
					patched++
					if ids[0][0] != ids[0][1] {
						t.Fatalf("update %d: in-place correction changed the file id", u)
					}
					if ids[1][0] == ids[1][1] {
						t.Fatalf("update %d: the rewrite kept the file id", u)
					}
					inPlaceWrites += writes[0]
					rewriteWrites += writes[1]
				}
				if patched == 0 {
					var parts []string
					for _, k := range whisper.InPlaceResults() {
						parts = append(parts, fmt.Sprintf("%s=%d/%d", k, whisper.RollupCorrections(k), whisper.SidecarSlotMerges(k)))
					}
					t.Fatalf("no update was patched in place; sidecar_slot replays %d; results %s", whisper.OverlapUpdates(whisper.OverlapSidecarSlot), strings.Join(parts, " "))
				}
				if inPlaceWrites*2 > rewriteWrites {
					t.Fatalf("in place wrote %d keys over %d corrections, rewrite %d", inPlaceWrites, patched, rewriteWrites)
				}
				t.Logf("%d corrections: %.1f vs %.1f key writes per update (in place vs rewrite)", patched, float64(inPlaceWrites)/float64(patched), float64(rewriteWrites)/float64(patched))
			})
		}
	}
}

// BenchmarkInPlaceCorrection times the first update of inPlaceKeyStream that
// patches in place, from the same state, with and without the in-place paths,
// and reports the keys and bytes it writes.
func BenchmarkInPlaceCorrection(b *testing.B) {
	defer func(v bool) { inPlaceRollupPatches = v }(inPlaceRollupPatches)
	for _, sidecar := range []bool{false, true} {
		m, at, batches := inPlaceKeyStream(sidecar)
		codec := &ContinuationCodec{ReuseBlockKeys: true}
		counter := whisper.RollupCorrections
		if sidecar {
			counter = whisper.SidecarSlotMerges
		}
		now := at[0]
		clock := func() time.Time { return time.Unix(int64(now), 0) }
		// Find the first update the in-place run patches; the state before it
		// is the same in both runs.
		kv := &countingIO{mapContinuationIO: mapContinuationIO{}}
		if err := codec.Create(context.Background(), kv, m, clock); err != nil {
			b.Fatal(err)
		}
		target := -1
		for u, pts := range batches {
			now = at[u]
			state := mapContinuationIO{}
			for k, v := range kv.mapContinuationIO {
				state[k] = v
			}
			inPlaceRollupPatches = true
			before := counter(whisper.InPlaced)
			if err := codec.Update(context.Background(), kv, m, pts, -1, clock, nil); err != nil {
				b.Fatal(err)
			}
			if counter(whisper.InPlaced) != before {
				target, kv.mapContinuationIO = u, state
				break
			}
		}
		if target < 0 {
			b.Fatal("no in-place update")
		}
		for _, inPlace := range []bool{false, true} {
			b.Run(fmt.Sprintf("sidecar=%v/in_place=%v", sidecar, inPlace), func(b *testing.B) {
				inPlaceRollupPatches = inPlace
				now = at[target]
				var keys, bytes int
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					run := &countingIO{mapContinuationIO: make(mapContinuationIO, len(kv.mapContinuationIO))}
					for k, v := range kv.mapContinuationIO {
						run.mapContinuationIO[k] = v
					}
					b.StartTimer()
					if err := codec.Update(context.Background(), run, m, batches[target], -1, clock, nil); err != nil {
						b.Fatal(err)
					}
					keys += run.writes()
					bytes += run.bytes
				}
				b.ReportMetric(float64(keys)/float64(b.N), "keys/op")
				b.ReportMetric(float64(bytes)/float64(b.N), "written-B/op")
			})
		}
	}
}

// classicFile is a classic whisper file receiving the same updates.
type classicFile struct {
	data []byte
	t    testing.TB
	path string
	now  int
}

func newClassicFile(t testing.TB, sc oracleScenario) *classicFile {
	t.Helper()
	c := &classicFile{t: t, path: filepath.Join(t.TempDir(), "classic.wsp")}
	wr := make([]whisper.Retention, len(sc.rets))
	for i, r := range sc.rets {
		wr[i] = whisper.NewRetention(r.Step, r.Count)
	}
	w, err := whisper.CreateWithOptions(c.path, whisper.NewRetentionsNoPointer(wr), whisper.AggregationMethod(sc.method), sc.xff, &whisper.Options{Sparse: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	c.data, err = os.ReadFile(c.path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (c *classicFile) open() *whisper.Whisper {
	w, err := whisper.OpenWithOptions(c.path, &whisper.Options{InMemory: true, InMemoryContent: c.data, Now: func() time.Time { return time.Unix(int64(c.now), 0) }})
	if err != nil {
		c.t.Fatal(err)
	}
	return w
}

func (c *classicFile) update(now int, pts []store.Point) {
	c.now = now
	w := c.open()
	input := make([]*whisper.TimeSeriesPoint, len(pts))
	for i, p := range pts {
		input[i] = &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value}
	}
	if err := w.UpdateMany(input); err != nil {
		c.t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		c.t.Fatal(err)
	}
}

func (c *classicFile) series(from int) *whisper.TimeSeries {
	w := c.open()
	defer w.Close()
	ts, err := w.Fetch(from, c.now)
	if err != nil || ts == nil {
		c.t.Fatalf("classic fetch: %v", err)
	}
	return ts
}

func sameValue(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b) || math.IsNaN(a) && math.IsNaN(b)
}
