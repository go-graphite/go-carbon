package whisperio

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type sumTestIO struct {
	mapContinuationIO
	hits, writes, bytes int
}

func (kv *sumTestIO) ClearDigestCache()       { kv.DeletePrefix(sumRevisionKey) }
func (kv *sumTestIO) CacheDigest(k, v []byte) { kv.writes++; kv.Set(k, v) }
func (kv *sumTestIO) Get(k []byte) ([]byte, error) {
	v, e := kv.mapContinuationIO.Get(k)
	kv.bytes += len(v)
	if len(k) == 9 && string(k[:3]) == "ds/" && e == nil {
		kv.hits++
	}
	return v, e
}
func (kv *sumTestIO) Visit(k []byte, f func([]byte, []byte) error) error {
	return kv.mapContinuationIO.Visit(k, func(k, v []byte) error { kv.bytes += len(v); return f(k, v) })
}

// Independently hashes the full canonical Fetch window with the old SHA recipe.
// It also accumulates a reference sum; comparison verdicts are checked across
// equal/different copies, not by equating two different hash algorithms.
func sumOracle(tb testing.TB, kv store.ContinuationIO, m store.Metadata, now, from, until int) ([]store.SumArchive, [][32]byte) {
	tb.Helper()
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	fs := newContinuationFiles(kv, true)
	w, e := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if e != nil {
		tb.Fatal(e)
	}
	defer w.Close()
	out := make([]store.SumArchive, len(m.Retentions))
	hashes := make([][32]byte, len(out))
	for ai, r := range m.Retentions {
		lo, hi := store.DigestBounds(r, now, from, until)
		var d store.PointSum
		h := sha256.New()
		var buf [16]byte
		if hi > lo {
			series, e := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: ai, From: lo, Until: hi})
			if e != nil {
				tb.Fatal(e)
			}
			for i, v := range series.Values() {
				ts := int64(series.FromTime() + i*r.Step)
				if math.IsNaN(v) || math.IsInf(v, 0) {
					continue
				}
				d.Add(ts, v)
				binary.BigEndian.PutUint64(buf[:8], uint64(ts))
				binary.BigEndian.PutUint64(buf[8:], math.Float64bits(v))
				h.Write(buf[:])
			}
		}
		copy(hashes[ai][:], h.Sum(nil))
		out[ai] = store.SumArchive{Archive: ai, Step: r.Step, Points: r.Count, From: lo, Until: hi, Sum: fmt.Sprintf("%016x", d.Sum), Count: d.Count}
	}
	return out, hashes
}

func TestSumDigestDifferential(t *testing.T) {
	ctx := context.Background()
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprint(direct), func(t *testing.T) {
			now := 1700000000
			clock := func() time.Time { return time.Unix(int64(now), 0) }
			codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: direct}
			kv := &sumTestIO{mapContinuationIO: mapContinuationIO{}}
			m := store.Metadata{MetricConfig: store.MetricConfig{Name: "sum", Retentions: []store.Retention{{Step: 1, Count: 200000}, {Step: 60, Count: 12000}}, AggregationMethod: store.Average, XFilesFactor: .5}}
			if e := codec.Create(ctx, kv, m, clock); e != nil {
				t.Fatal(e)
			}
			rng := rand.New(rand.NewSource(8144))
			hits := 0
			for cycle := 0; cycle < 5; cycle++ {
				input := make([]store.Point, 0, 4000)
				for i := 0; i < 100000; i++ {
					ts := now - 120000 + i
					if i < 8192 && rng.Intn(9) == 0 {
						continue
					}
					v := float64(rng.Intn(2000)) / 7
					if i%397 == 0 {
						v = math.Float64frombits([]uint64{0x8000000000000000, 0x7ff8000000000123, 0x7ff0000000000000, 1}[cycle%4])
					}
					input = append(input, store.Point{Timestamp: int64(ts), Value: v})
				}
				// Repeat and overwrite timestamps crossing prior physical blocks.
				input = append(input, store.Point{Timestamp: int64(now - 6000), Value: 17}, store.Point{Timestamp: int64(now - 6000), Value: 18})
				if e := codec.Update(ctx, kv, m, input, -1, clock, nil); e != nil {
					t.Fatal(e)
				}
				m.Revision++
				for _, window := range [][2]int{{0, 0}, {now - 18000, now - 600}, {now - 1701, now - 97}, {now - 80000, now - 79000}, {(now-120000)/sumBucketSlots*sumBucketSlots + sumBucketSlots, (now-120000)/sumBucketSlots*sumBucketSlots + 4*sumBucketSlots}} {
					want, sha := sumOracle(t, kv, m, now, window[0], window[1])
					for warm := 0; warm < 2; warm++ {
						got, e := codec.SumDigest(ctx, kv, m, window[0], window[1], clock)
						if e != nil {
							t.Fatal(e)
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("cycle %d window %v warm %d\ngot %+v\nwant %+v", cycle, window, warm, got, want)
						}
					}
					copyKV := &sumTestIO{mapContinuationIO: mapContinuationIO{}}
					for k, v := range kv.mapContinuationIO {
						copyKV.mapContinuationIO[k] = append([]byte(nil), v...)
					}
					other, e := codec.SumDigest(ctx, copyKV, m, window[0], window[1], clock)
					if e != nil {
						t.Fatal(e)
					}
					_, otherSHA := sumOracle(t, copyKV, m, now, window[0], window[1])
					if reflect.DeepEqual(want, other) != reflect.DeepEqual(sha, otherSHA) {
						t.Fatal("equal-copy verdict differs")
					}
				}
				hits += kv.hits
				kv.hits = 0
				now += 2000
			}
			if hits == 0 || kv.writes == 0 {
				t.Fatal("test never exercised lazy cache hits")
			}
		})
	}
}

func TestSumDigestColdPack(t *testing.T) {
	db, _, now := rangeFetchFixture(t)
	ctx := context.Background()
	before, e := db.SumDigest(ctx, "range.metric", 0, 0)
	if e != nil {
		t.Fatal(e)
	}
	after, e := db.SumDigest(ctx, "range.metric", 0, 0)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("warm differs")
	}
	// The current SHA path selects one archive by the age of From. Compare the
	// same canonical view independently, including retention edges.
	for _, a := range before {
		series, e := db.Fetch(ctx, "range.metric", a.From-1, a.Until)
		if e != nil {
			t.Fatal(e)
		}
		if series == nil || series.Step != a.Step {
			t.Fatalf("archive selection: %+v", a)
		}
		var d store.PointSum
		for i, v := range series.Values {
			ts := series.FromTime + i*series.Step
			if ts >= a.From && ts < a.Until {
				d.Add(int64(ts), v)
			}
		}
		if a.Sum != fmt.Sprintf("%016x", d.Sum) || a.Count != d.Count {
			t.Fatalf("archive %d differs from Fetch at %d: %+v vs %+v", a.Archive, now, a, d)
		}
	}
}

// Independently ingested replicas have different physical headers/block layouts.
// Check both equal and unequal verdicts against the existing ordered SHA recipe.
func TestSumDigestReplicaVerdicts(t *testing.T) {
	ctx := context.Background()
	now := 1700000000
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	codec := &ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true}
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "replica", Retentions: []store.Retention{{Step: 1, Count: 200000}}, AggregationMethod: store.Last}}
	replicas := []*sumTestIO{{mapContinuationIO: mapContinuationIO{}}, {mapContinuationIO: mapContinuationIO{}}}
	revisions := []store.Metadata{m, m}
	start := (now - 120000) / sumBucketSlots * sumBucketSlots
	var points []store.Point
	rng := rand.New(rand.NewSource(2937))
	for i := 0; i < 8*sumBucketSlots; i++ {
		// Preserve several complete dense buckets and one sparse/special bucket.
		if i < sumBucketSlots && rng.Intn(5) == 0 {
			continue
		}
		v := math.Float64frombits(rng.Uint64())
		if i >= sumBucketSlots && (math.IsNaN(v) || math.IsInf(v, 0)) {
			v = 1
		}
		points = append(points, store.Point{Timestamp: int64(start + i), Value: v})
	}
	for ri, kv := range replicas {
		// Force genuinely different physical block boundaries, rather than
		// relying on ingestion batch boundaries to perturb the encoding.
		fs := newContinuationFiles(kv, false).withClock(clock)
		options := continuationOptions(fs, clock)
		options.PointsPerBlock = []int{2048, 3072}[ri]
		w, err := whisper.CreateWithOptions(fs.main(), whisper.MustParseRetentionDefs("1s:200000s"), whisper.Last, 0, options)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := fs.flush(); err != nil {
			t.Fatal(err)
		}
		batch := len(points)
		if ri == 1 {
			batch = 337
		}
		for first := 0; first < len(points); first += batch {
			if err := codec.Update(ctx, kv, revisions[ri], points[first:min(first+batch, len(points))], -1, clock, nil); err != nil {
				t.Fatal(err)
			}
			revisions[ri].Revision++
		}
	}
	left, err := readSumWriteView(replicas[0], clock)
	if err != nil {
		t.Fatal(err)
	}
	right, err := readSumWriteView(replicas[1], clock)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(left.blocks, right.blocks) {
		t.Fatal("replica fixture has identical physical block layouts")
	}
	compare := func() bool {
		t.Helper()
		sums := make([][]store.SumArchive, 2)
		hashes := make([][][32]byte, 2)
		for ri, kv := range replicas {
			want, sha := sumOracle(t, kv, revisions[ri], now, 0, 0)
			for warm := 0; warm < 2; warm++ {
				got, err := codec.SumDigest(ctx, kv, revisions[ri], 0, 0, clock)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("replica %d sum differs from canonical Fetch", ri)
				}
				sums[ri] = got
			}
			hashes[ri] = sha
		}
		equal := reflect.DeepEqual(sums[0], sums[1])
		if equal != reflect.DeepEqual(hashes[0], hashes[1]) {
			t.Fatal("sum/SHA verdict disagreement")
		}
		return equal
	}
	if !compare() {
		t.Fatal("different block layouts changed equality")
	}
	for _, value := range []float64{math.Copysign(0, -1), 0, math.NaN(), math.Inf(1), math.SmallestNonzeroFloat64, 71} {
		ts := int64(start + 3*sumBucketSlots + 101)
		for ri, kv := range replicas {
			old, _ := sumOracle(t, kv, revisions[ri], now, 0, 0)
			input := []store.Point{{Timestamp: ts, Value: value}, {Timestamp: ts, Value: value}}
			if err := codec.Update(ctx, kv, revisions[ri], input, -1, clock, nil); err != nil {
				t.Fatal(err)
			}
			revisions[ri].Revision++
			equal := compare()
			updated, _ := sumOracle(t, kv, revisions[ri], now, 0, 0)
			if ri == 0 && equal && !reflect.DeepEqual(old, updated) {
				t.Fatal("missed overwrite")
			}
			if ri == 1 && !equal {
				t.Fatal("matching overwrite did not restore equality")
			}
		}
	}
	// Cache entries do not include partially expired buckets as time advances.
	now = start + 200000 + 103
	if !compare() {
		t.Fatal("retention edge changed equality")
	}
	for _, kv := range replicas {
		if kv.hits == 0 {
			t.Fatal("no warm bucket hits")
		}
	}
}

func TestSumDigestUninstrumentedMutationFence(t *testing.T) {
	ctx := context.Background()
	now := 1700000000
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	codec := &ContinuationCodec{DirectLateWrites: true}
	kv := &sumTestIO{mapContinuationIO: mapContinuationIO{}}
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "fence", Retentions: []store.Retention{{Step: 1, Count: 200000}}, AggregationMethod: store.Last}}
	if err := codec.Create(ctx, kv, m, clock); err != nil {
		t.Fatal(err)
	}
	start := (now - 120000) / sumBucketSlots * sumBucketSlots
	var points []store.Point
	for i := 0; i < 4*sumBucketSlots; i++ {
		points = append(points, store.Point{Timestamp: int64(start + i), Value: 1})
	}
	if err := codec.Update(ctx, kv, m, points, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	m.Revision++
	if _, err := codec.SumDigest(ctx, kv, m, 0, 0, clock); err != nil {
		t.Fatal(err)
	}
	cached := 0
	for k, v := range kv.mapContinuationIO {
		if len(k) == 9 && k[:3] == "ds/" {
			cached++
			if len(v) != 8 {
				t.Fatal("sum value not 8 bytes")
			}
		}
	}
	if cached == 0 {
		t.Fatal("no cached buckets")
	}
	// An append must retain unrelated history even with a routine maintenance
	// policy present: the production writer supplies one on normal ingestion.
	policy := &store.ContinuationMaintenance{Allow: func() bool { return false }}
	if err := codec.Update(ctx, kv, m, []store.Point{{Timestamp: int64(now - 10), Value: 2}}, -1, clock, policy); err != nil {
		t.Fatal(err)
	}
	m.Revision++
	retained := 0
	for k := range kv.mapContinuationIO {
		if len(k) == 9 && k[:3] == "ds/" {
			retained++
		}
	}
	if retained == 0 {
		t.Fatal("append invalidated all history")
	}
	// Simulate a patch/import path that bypasses Update's selective invalidation.
	if err := codec.updateReanchoring(ctx, kv, m, []store.Point{{Timestamp: int64(start + sumBucketSlots + 17), Value: 99}}, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	m.Revision++
	want, _ := sumOracle(t, kv, m, now, 0, 0)
	got, err := codec.SumDigest(ctx, kv, m, 0, 0, clock)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatalf("stale fence: %v\ngot %+v\nwant %+v", err, got, want)
	}
}

func TestSumDigestStoreReopenAndWrites(t *testing.T) {
	ctx := context.Background()
	const now = 1700000000
	clock := func() time.Time { return time.Unix(now, 0) }
	codec := &ContinuationCodec{DirectLateWrites: true}
	dir := t.TempDir()
	options := store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: clock, CacheSize: 2 << 20, MetadataCacheSize: 2 << 20, MemTableSize: 1 << 20}
	db, err := store.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	db.StopBackground()
	config := store.MetricConfig{Name: "reopen", Retentions: []store.Retention{{Step: 1, Count: 200000}}, AggregationMethod: store.Last}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	start := (now - 120000) / sumBucketSlots * sumBucketSlots
	var points []store.Point
	for i := 0; i < 4*sumBucketSlots; i++ {
		points = append(points, store.Point{Timestamp: int64(start + i), Value: float64(i % 101)})
	}
	if err := db.UpdateMany(ctx, config.Name, points); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		got, err := db.SumDigest(ctx, config.Name, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		series, err := db.Fetch(ctx, config.Name, got[0].From-1, got[0].Until)
		if err != nil {
			t.Fatal(err)
		}
		var want store.PointSum
		for i, v := range series.Values {
			ts := series.FromTime + i*series.Step
			if ts >= got[0].From && ts < got[0].Until {
				want.Add(int64(ts), v)
			}
		}
		if got[0].Count != want.Count || got[0].Sum != fmt.Sprintf("%016x", want.Sum) {
			t.Fatalf("database cache differs: %+v vs %+v", got, want)
		}
	}
	check()
	check()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = store.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	check()
	for i := 0; i < 5; i++ {
		// Warm both the metadata state cache and the digest cache between writes.
		if err := db.UpdateMany(ctx, config.Name, []store.Point{{Timestamp: int64(start + sumBucketSlots + 99), Value: float64(500 + i)}}); err != nil {
			t.Fatal(err)
		}
		check()
		check()
	}
	// Archive-preserving imports select retention seconds, not an archive index.
	// Direct late writes can change a value without changing any block bounds.
	for i := 0; i < 2; i++ {
		if err := db.UpdateManyForArchive(ctx, config.Name, []store.Point{{Timestamp: int64(start + sumBucketSlots + 99), Value: float64(700 + i)}}, config.Retentions[0].MaxRetention()); err != nil {
			t.Fatal(err)
		}
		check()
		check()
	}
	exported, err := db.ExportTransfer(ctx, []string{config.Name})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range exported[0].Keys {
		if len(entry.Key) >= 2 && string(entry.Key[:2]) == "ds" {
			t.Fatal("transfer contains local cache")
		}
	}
}
