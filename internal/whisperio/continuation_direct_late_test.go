package whisperio

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func openDirectTestStore(t testing.TB, buckets, direct bool, clockNow *int) *store.Store {
	t.Helper()
	db, err := openMemoryTestStore(t, filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: buckets,
		Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: direct},
		Now:          func() time.Time { return time.Unix(int64(*clockNow), 0) }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); db = nil })
	return db
}

// directLateTally counts, per slot compared after an update, how the direct
// store and the sidecar store relate to classic whisper.
type directLateTally struct {
	slots    int // compared slots
	direct   int // direct store differs from classic
	sidecar  int // sidecar store differs from classic
	fixed    int // sidecar differs, direct matches classic
	worse    int // sidecar matches classic, direct differs
	firstBad string
}

// TestDirectLateWritesMatchClassic drives a store with direct late writes and
// one diverting late points to sidecars, in lockstep next to a classic whisper
// file, over the oracle streams (sub-resolution, late, duplicate and skewed
// emitters) and the in-place shapes, in both formats. After every update every
// slot of every archive's fetch is compared with classic whisper.
//
// Every slot of the direct store must equal classic whisper. The test reports
// how many slots the sidecar store serves differently from classic that the
// direct path gets right.
func TestDirectLateWritesMatchClassic(t *testing.T) {
	t.Parallel()
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			t.Parallel()
			for group := 0; group < 4; group++ {
				t.Run(fmt.Sprintf("group=%d", group), func(t *testing.T) {
					t.Parallel()
					clockNow := 1_700_000_000
					direct, sidecar := openDirectTestStore(t, buckets, true, &clockNow), openDirectTestStore(t, buckets, false, &clockNow)
					writtenBefore := whisper.DirectLatePoints(whisper.DirectWritten)
					var total directLateTally
					var updates int
					for i, sc := range inPlaceScenarios(testing.Short()) {
						if i%4 != group {
							continue
						}
						t.Run(fmt.Sprintf("scenario=%02d", i), func(t *testing.T) {
							name := fmt.Sprintf("direct.%d", i)
							for _, db := range []*store.Store{direct, sidecar} {
								if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: sc.rets, AggregationMethod: sc.method, XFilesFactor: sc.xff}); err != nil {
									t.Fatal(err)
								}
							}
							rngs := []*rand.Rand{rand.New(rand.NewSource(int64(i))), rand.New(rand.NewSource(int64(i)))}
							at, batches := inPlaceBatches(sc, int64(i))
							classic := newClassicFile(t, sc)
							var tally directLateTally
							for u, b := range batches {
								clockNow = at[u]
								for k, db := range []*store.Store{direct, sidecar} {
									rng := rngs[k]
									maintenance := store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute, Allow: func() bool { return rng.Intn(4) == 0 }}
									if err := db.UpdateManyMaintained(context.Background(), name, b, maintenance); err != nil {
										t.Fatalf("%s update %d (direct %v): %v", name, u, k == 0, err)
									}
								}
								updates++
								classic.update(at[u], b)
								for _, r := range sc.rets {
									from := clockNow - r.Step*r.Count + r.Step
									got, err := direct.Fetch(context.Background(), name, from, clockNow)
									if err != nil {
										t.Fatal(err)
									}
									old, err := sidecar.Fetch(context.Background(), name, from, clockNow)
									if err != nil {
										t.Fatal(err)
									}
									series := classic.series(from)
									ref := series.Values()
									if got.FromTime != series.FromTime() || got.Step != series.Step() || len(got.Values) != len(ref) || len(old.Values) != len(ref) {
										t.Fatalf("%s (%s) update %d step %d: fetch shape differs from classic", name, sc.schema, u, r.Step)
									}
									for j := range ref {
										x, y, c := got.Values[j], old.Values[j], ref[j]
										tally.slots++
										dx, dy := !sameValue(x, c), !sameValue(y, c)
										if dx {
											tally.direct++
										}
										if dy {
											tally.sidecar++
										}
										if dy && !dx {
											tally.fixed++
										}
										if dx && !dy {
											tally.worse++
										}
										if dx {
											if tally.firstBad == "" {
												tally.firstBad = fmt.Sprintf("update %d step %d slot %d (now-%ds): direct %v, sidecar %v, classic %v", u, r.Step, got.FromTime+j*got.Step, clockNow-(got.FromTime+j*got.Step), x, y, c)
											}
										}
									}
								}
							}
							if tally.direct > 0 {
								msg := fmt.Sprintf("%s (%s, %v, method %d, xff %v): %d slots differ from classic (%d where the sidecar store matches it); first %s", name, sc.schema, sc.e, sc.method, sc.xff, tally.direct, tally.worse, tally.firstBad)
								if os.Getenv("DIRECT_LATE_REPORT") != "" {
									t.Log(msg)
								} else {
									t.Error(msg)
								}
							}
							if tally.direct > 0 || tally.sidecar > 0 {
								t.Logf("%s (%s, %v, method %d): differ from classic: direct %d, sidecar %d slots; fixed by direct %d", name, sc.schema, sc.e, sc.method, tally.direct, tally.sidecar, tally.fixed)
							}
							total.slots += tally.slots
							total.direct += tally.direct
							total.sidecar += tally.sidecar
							total.fixed += tally.fixed
							total.worse += tally.worse

						})
					}

					written := whisper.DirectLatePoints(whisper.DirectWritten) - writtenBefore
					if written == 0 {
						t.Fatal("no late point was written directly")
					}
					t.Logf("%d updates, %d slots: differ from classic: direct %d, sidecar %d; fixed by direct %d, only direct %d; %d late points written directly",
						updates, total.slots, total.direct, total.sidecar, total.fixed, total.worse, written)
					for _, k := range whisper.DirectLateResults() {
						t.Logf("late points %s: %d (updates %d)", k, whisper.DirectLatePoints(k), whisper.DirectLateWrites(k))
					}
					for _, k := range whisper.InPlaceResults() {
						if n := whisper.DirectSidecarFolds(k); n > 0 {
							t.Logf("sidecar folds %s: %d", k, n)
						}
					}

				})
			}
		})
	}
}

func openDirectStoreAt(t testing.TB, dir string, buckets, direct bool, clockNow *int) *store.Store {
	t.Helper()
	db, err := openMemoryTestStore(t, dir, store.Options{ExactBlocks: true, TimeBuckets: buckets,
		Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: direct},
		Now:          func() time.Time { return time.Unix(int64(*clockNow), 0) }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestDirectLateWritesTakeOverSidecars switches a store from sidecars to
// direct late writes halfway through each stream, by reopening it, next to a
// store that keeps sidecars and a classic whisper file. Existing sidecars are
// folded in place on the next update. From the switch on, every slot must
// equal classic whisper or, where it does not, what the sidecar store serves:
// folding and direct writes may only remove differences from classic.
func TestDirectLateWritesTakeOverSidecars(t *testing.T) {
	t.Parallel()
	slots := make(chan struct{}, 4)
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			t.Parallel()
			var cases sync.WaitGroup
			defer cases.Wait()
			for i, sc := range inPlaceScenarios(true) {
				cases.Add(1)
				go func() {
					defer cases.Done()
					slots <- struct{}{}
					defer func() { <-slots }()
					t.Run(fmt.Sprintf("scenario=%02d", i), func(t *testing.T) {
						clockNow := 1_700_000_000
						foldsBefore := whisper.DirectSidecarFolds(whisper.InPlaced)
						var fixed, after, mismatch int
						name := fmt.Sprintf("takeover.%d", i)
						dir := filepath.Join(t.TempDir(), "takeover")
						switched := openDirectStoreAt(t, dir, buckets, false, &clockNow)
						sidecar := openDirectTestStore(t, buckets, false, &clockNow)
						for _, db := range []*store.Store{switched, sidecar} {
							if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: sc.rets, AggregationMethod: sc.method, XFilesFactor: sc.xff}); err != nil {
								t.Fatal(err)
							}
						}
						rngs := []*rand.Rand{rand.New(rand.NewSource(int64(i))), rand.New(rand.NewSource(int64(i)))}
						at, batches := inPlaceBatches(sc, int64(i))
						classic := newClassicFile(t, sc)
						half := len(batches) / 2
						for u, b := range batches {
							clockNow = at[u]
							if u == half {
								if err := switched.Close(); err != nil {
									t.Fatal(err)
								}
								switched = openDirectStoreAt(t, dir, buckets, true, &clockNow)
							}
							for k, db := range []*store.Store{switched, sidecar} {
								rng := rngs[k]
								maintenance := store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute, Allow: func() bool { return rng.Intn(4) == 0 }}
								if err := db.UpdateManyMaintained(context.Background(), name, b, maintenance); err != nil {
									t.Fatalf("%s update %d: %v", name, u, err)
								}
							}
							classic.update(at[u], b)
							if u < half {
								continue
							}
							for _, r := range sc.rets {
								from := clockNow - r.Step*r.Count + r.Step
								got, err := switched.Fetch(context.Background(), name, from, clockNow)
								if err != nil {
									t.Fatal(err)
								}
								old, err := sidecar.Fetch(context.Background(), name, from, clockNow)
								if err != nil {
									t.Fatal(err)
								}
								ref := classic.series(from).Values()
								for j := range ref {
									x, y, c := got.Values[j], old.Values[j], ref[j]
									after++
									if sameValue(x, c) {
										if !sameValue(y, c) {
											fixed++
										}
										continue
									}
									mismatch++
									if !sameValue(x, y) {
										t.Fatalf("%s (%s) update %d step %d slot %d: switched %v, sidecar %v, classic %v", name, sc.schema, u, r.Step, got.FromTime+j*got.Step, x, y, c)
									}
								}
							}
						}
						if err := switched.Close(); err != nil {
							t.Fatal(err)
						}
						t.Logf("after the switch: %d slots, %d fixed relative to the sidecar store, %d still differing from classic (identical to the sidecar store); %d sidecars folded in place",
							after, fixed, mismatch, whisper.DirectSidecarFolds(whisper.InPlaced)-foldsBefore)
					})
				}()
			}
		})
	}
}

// TestDirectLateWriteSupersedesSidecarSlot writes a late value that lands in
// a sidecar, then, with direct late writes, a newer late value for the same
// slot: as in classic whisper the newer write wins at every resolution, and
// the sidecar is gone.
func TestDirectLateWriteSupersedesSidecarSlot(t *testing.T) {
	t.Parallel()
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			t.Parallel()
			clockNow := 1_700_000_000
			sc := oracleScenario{rets: []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 288}}, method: store.Average}
			dir := filepath.Join(t.TempDir(), "slot")
			db := openDirectStoreAt(t, dir, buckets, false, &clockNow)
			name := "slot"
			if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: sc.rets, AggregationMethod: sc.method}); err != nil {
				t.Fatal(err)
			}
			classic := newClassicFile(t, sc)
			update := func(now int, pts []store.Point) {
				t.Helper()
				clockNow = now
				if err := db.UpdateManyMaintained(context.Background(), name, pts, store.ContinuationMaintenance{}); err != nil {
					t.Fatal(err)
				}
				classic.update(now, pts)
			}
			start := 1_700_000_000
			var pts []store.Point
			for ts := start - 6*3600; ts < start; ts += 60 {
				pts = append(pts, store.Point{Timestamp: int64(ts), Value: float64(ts % 101)})
			}
			update(start, pts)
			slot := int64(start - 3*3600)
			update(start+60, []store.Point{{Timestamp: int64(start), Value: 1}, {Timestamp: slot, Value: 1000}})
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openDirectStoreAt(t, dir, buckets, true, &clockNow)
			defer db.Close()
			written := whisper.DirectLatePoints(whisper.DirectWritten)
			update(start+120, []store.Point{{Timestamp: int64(start + 60), Value: 2}, {Timestamp: slot, Value: 2000}})
			if whisper.DirectLatePoints(whisper.DirectWritten) == written {
				t.Fatal("the late point was not written directly")
			}
			for _, r := range sc.rets {
				from := clockNow - r.Step*r.Count + r.Step
				got, err := db.Fetch(context.Background(), name, from, clockNow)
				if err != nil {
					t.Fatal(err)
				}
				ref := classic.series(from).Values()
				for j := range ref {
					if !sameValue(got.Values[j], ref[j]) {
						t.Fatalf("step %d slot %d: store %v, classic %v", r.Step, got.FromTime+j*got.Step, got.Values[j], ref[j])
					}
				}
				if r.Step == 60 {
					if v := got.Values[(int(slot)-got.FromTime)/60]; v != 2000 {
						t.Fatalf("late slot holds %v, want the newer write 2000", v)
					}
				}
			}
		})
	}
}

// directLateStream is ten days of minutely history, then ten-minute batches
// that each carry a late correction of a point written an hour earlier.
func directLateStream() (store.Metadata, []int, [][]store.Point) {
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
	for ts := start; ts < start+4*3600; ts += 600 {
		var pts []store.Point
		for x := ts; x < ts+600; x += 60 {
			pts = append(pts, store.Point{Timestamp: int64(x), Value: float64(x % 89)})
		}
		pts = append(pts, store.Point{Timestamp: int64(ts - 3600 + 120), Value: 1000})
		at, batches = append(at, ts+600), append(batches, pts)
	}
	return store.Metadata{MetricConfig: store.MetricConfig{Name: "late", Retentions: rets, AggregationMethod: store.Average}}, at, batches
}

func copyIO(kv mapContinuationIO) *countingIO {
	run := &countingIO{mapContinuationIO: make(mapContinuationIO, len(kv))}
	for k, v := range kv {
		run.mapContinuationIO[k] = v
	}
	return run
}

// BenchmarkDirectLateWrite times an update carrying a late correction, with
// direct late writes and with the sidecar, from each path's own state: after
// the history only (the sidecar path creates its sidecar) and after a dozen
// such updates (it extends its sidecar). It reports the keys and bytes the
// update writes.
func BenchmarkDirectLateWrite(b *testing.B) {
	m, at, batches := directLateStream()
	history := 10
	for _, prior := range []int{0, 12} {
		target := history + prior
		for _, direct := range []bool{false, true} {
			codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: direct}
			now := at[0]
			clock := func() time.Time { return time.Unix(int64(now), 0) }
			kv := &countingIO{mapContinuationIO: mapContinuationIO{}}
			if err := codec.Create(context.Background(), kv, m, clock); err != nil {
				b.Fatal(err)
			}
			for u := 0; u < target; u++ {
				now = at[u]
				if err := codec.Update(context.Background(), kv, m, batches[u], -1, clock, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.Run(fmt.Sprintf("prior_late=%d/direct=%v", prior, direct), func(b *testing.B) {
				now = at[target]
				var keys, bytes int
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					run := copyIO(kv.mapContinuationIO)
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

// readCountingIO counts point reads and walks of a fetch.
type readCountingIO struct {
	mapContinuationIO
	gets, visits int
}

func (r *readCountingIO) Get(k []byte) ([]byte, error) { r.gets++; return r.mapContinuationIO.Get(k) }
func (r *readCountingIO) Visit(prefix []byte, fn func(k, v []byte) error) error {
	r.visits++
	return r.mapContinuationIO.Visit(prefix, fn)
}

// BenchmarkDirectLateFetch fetches every archive of a metric that received a
// late correction every update for four hours: without a sidecar after direct
// late writes, and through the sidecar overlay otherwise.
func BenchmarkDirectLateFetch(b *testing.B) {
	m, at, batches := directLateStream()
	for _, direct := range []bool{false, true} {
		codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: direct}
		now := at[0]
		clock := func() time.Time { return time.Unix(int64(now), 0) }
		kv := &countingIO{mapContinuationIO: mapContinuationIO{}}
		if err := codec.Create(context.Background(), kv, m, clock); err != nil {
			b.Fatal(err)
		}
		for u := range batches {
			now = at[u]
			if err := codec.Update(context.Background(), kv, m, batches[u], -1, clock, nil); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("direct=%v", direct), func(b *testing.B) {
			reader := &readCountingIO{mapContinuationIO: kv.mapContinuationIO}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, r := range m.Retentions {
					if _, err := codec.Fetch(context.Background(), reader, m, now-r.Step*r.Count+r.Step, now, clock); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(reader.gets)/float64(b.N), "gets/op")
			b.ReportMetric(float64(reader.visits)/float64(b.N), "walks/op")
		})
	}
}
