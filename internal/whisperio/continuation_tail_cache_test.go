package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	whisper "github.com/go-graphite/go-whisper"
)

func tailClassicSeed(t testing.TB, method store.AggregationMethod, xff float32) (store.Metadata, mapContinuationIO, int64) {
	t.Helper()
	const base = int64(1794000000)
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "tail.hot", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 300, Count: 17280}, {Step: 3600, Count: 17520}}, AggregationMethod: method, XFilesFactor: xff}}
	kv := mapContinuationIO{}
	clock := func() time.Time { return time.Unix(base, 0) }
	fs := newContinuationFiles(kv, false).withClock(clock)
	options := continuationOptions(fs, clock)
	options.Compressed = false // classic main files, including resize survivors
	w, err := whisper.CreateWithOptions(fs.main(), whisper.MustParseRetentionDefs("1m:14d,5m:60d,1h:2y"), whisper.AggregationMethod(method), xff, options)
	if err != nil {
		t.Fatal(err)
	}
	var history []*whisper.TimeSeriesPoint
	for ts := base - 8*86400; ts < base; ts += 60 {
		if ts%19 == 0 {
			continue
		} // sparse historical slots
		history = append(history, &whisper.TimeSeriesPoint{Time: int(ts), Value: float64(ts%997) + .1})
	}
	if err := errors.Join(w.UpdateMany(history), w.Close(), fs.flush()); err != nil {
		t.Fatal(err)
	}
	return m, kv, base
}

// Compare every persisted byte against the previous update path, rather than
// treating NaNs or signed zeros as equivalent. Both coalesced and write-through
// batches cross circular slots, late corrections and maintenance boundaries.
func TestContinuationTailReadReuseParity(t *testing.T) {
	for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.Min, store.Max, store.Last, store.First} {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("%d/%g", method, xff), func(t *testing.T) {
				m, seed, base := tailClassicSeed(t, method, xff)
				plain, cached := copyIO(seed), copyIO(seed)
				codecs := []*ContinuationCodec{{keepTailReads: true}, {}}
				bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000012, 0x7ff0000000000001, 0x3ff0000000000001}
				for round := range 60 {
					count := []int{1, 9, 10, 127, 564}[round%5]
					base += int64(count) * 60
					var input []store.Point
					for j := range count {
						input = append(input, store.Point{Timestamp: base - int64(count-j)*60, Value: math.Float64frombits(bits[(round+j)%len(bits)])})
					}
					if round%3 == 1 {
						input = append(input, store.Point{Timestamp: base - 5401, Value: .125}, input[0])
					}
					clock := func() time.Time { return time.Unix(base+1, 0) }
					maintenance := &store.ContinuationMaintenance{MinPoints: 1, Allow: func() bool { return round%11 == 0 }}
					for side, kv := range []*countingIO{plain, cached} {
						if err := codecs[side].Update(context.Background(), kv, m, input, -1, clock, maintenance); err != nil {
							t.Fatalf("round %d: %v", round, err)
						}
					}
					if err := sameKV(plain.mapContinuationIO, cached.mapContinuationIO); err != nil {
						t.Fatalf("round %d: %v", round, err)
					}
				}
			})
		}
	}
}

func TestContinuationTailReadCacheInvalidation(t *testing.T) {
	kv := &appendTailKV{data: map[string][]byte{}}
	b := &continuationPointBuffer{ContinuationIO: kv, cacheReads: true}
	f := appendTailFile(kv)
	f.fs.kv = b
	key := []byte("group")
	read := func(want float64) {
		t.Helper()
		block, err := f.pointBlock(key)
		if err != nil || math.Float64bits(block.Points[0].Value) != math.Float64bits(want) {
			t.Fatalf("read %v %v, want %v", block, err, want)
		}
		block.Points[0].Value = 999 // returned results cannot mutate the cache
	}
	read(0)
	read(0)
	if kv.gets != 1 {
		t.Fatal("missing group was not cached")
	}
	block := &store.PointBlock{Present: [2]uint64{1}, Points: [128]store.Point{{Timestamp: 1, Value: math.Copysign(0, -1)}}}
	if err := b.Set(key, store.EncodePointBlock(block)); err != nil {
		t.Fatal(err)
	}
	read(block.Points[0].Value)
	read(block.Points[0].Value)
	block.Points[0].Value = 42
	if err := b.set(key, block); err != nil {
		t.Fatal(err)
	}
	read(42)
	if err := b.Visit(nil, func(_, _ []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	read(42)
	if err := b.DeletePrefix([]byte("gro")); err != nil {
		t.Fatal(err)
	}
	read(0)
	if err := b.Set(key, store.EncodePointBlock(block)); err != nil {
		t.Fatal(err)
	}
	read(42)
	if err := b.Delete(key); err != nil {
		t.Fatal(err)
	}
	read(0)
	for i := range 200 {
		b.remember([]byte(fmt.Sprint(i)), block)
	}
	if len(b.reads) > 64 {
		t.Fatal("unbounded cache")
	}
	// Errors are never memoized, and clean reads never produce writes.
	b.reads = nil
	kv.readError = errors.New("read failure")
	if _, err := f.pointBlock(key); !errors.Is(err, kv.readError) {
		t.Fatal(err)
	}
	kv.readError = nil
	read(0)
	if err := b.flush(nil); err != nil || len(kv.data) != 0 {
		t.Fatal("clean reads wrote storage", err)
	}
}

type tailReadFixture struct {
	db      *store.Store
	now     time.Time
	base    int64
	names   []string
	dir     string
	options store.Options
}

// Real Pebble fixture: classic rollup main files, eight days of history,
// At least 48 older time partitions outside the live window, sparse/deleted metrics,
// settled SSTs, ongoing churn and a rotating hot working set.
func newTailReadFixture(t testing.TB, keep bool) *tailReadFixture {
	t.Helper()
	m, seed, base := tailClassicSeed(t, store.Average, .5)
	f := &tailReadFixture{base: base, now: time.Unix(base, 0)}
	codec := &ContinuationCodec{keepTailReads: keep, ReuseBlockKeys: true}
	f.dir = t.TempDir()
	f.options = store.Options{ExactBlocks: true, Continuation: codec, TimeBuckets: true,
		Now: func() time.Time { return f.now }, SyncInterval: time.Hour, CacheSize: 8 << 20, MetadataCacheSize: 8 << 20,
		MemTableSize: 1 << 20, LBaseMaxBytes: 4 << 20, TargetFileSize: 64 << 10, BloomFilterBits: 10}
	db, err := openTestStore(f.dir, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	t.Cleanup(func() { f.db.Close() })
	db.StopBackground()
	ctx := context.Background()
	var coldNames []string
	for i := range 64 {
		f.now = time.Unix(base-8*86400+int64(i)*3*3600, 0)
		cfg := store.MetricConfig{Name: fmt.Sprintf("tail.history.%02d", i), Retentions: []store.Retention{{Step: 60, Count: 20160}}, AggregationMethod: store.Last}
		if _, err := db.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		var points []store.Point
		for j := range 128 {
			points = append(points, store.Point{Timestamp: f.now.Unix() - int64(128-j)*60, Value: float64(j) + .125})
		}
		if err := db.UpdateMany(ctx, cfg.Name, points); err != nil {
			t.Fatal(err)
		}
		if i%7 == 0 {
			if err := db.Delete(ctx, cfg.Name); err != nil {
				t.Fatal(err)
			}
		} else {
			coldNames = append(coldNames, cfg.Name)
		}
	}
	cold, err := db.ExportTransfer(ctx, coldNames)
	if err != nil {
		t.Fatal(err)
	}
	partitions := map[string]bool{}
	for _, metric := range cold {
		for _, item := range metric.Keys {
			if len(item.Key) >= 5 && item.Key[0] == 'T' {
				partitions[string(item.Key[:5])] = true
			}
		}
	}
	if len(partitions) < 48 {
		t.Fatalf("fixture has only %d historical partitions", len(partitions))
	}
	f.now = time.Unix(base, 0)
	var keys []store.RelayoutKV
	for key, value := range seed {
		keys = append(keys, store.RelayoutKV{Key: []byte(key), Value: value})
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].Key, keys[j].Key) < 0 })
	for i := range 16 {
		cfg := m.MetricConfig
		cfg.Name = fmt.Sprintf("tail.hot.%02d", i)
		if _, err := db.ImportTransfer(ctx, []store.TransferMetric{{Config: cfg, Version: store.TransferVersion{Name: cfg.Name}, Continuation: true, Keys: keys}}); err != nil {
			t.Fatal(err)
		}
		f.names = append(f.names, cfg.Name)
	}
	if err := errors.Join(db.Flush(), db.Compact()); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *tailReadFixture) update(t testing.TB, i int) {
	const batch = 564
	t.Helper()
	// A sub-resolution emitter, as on the persister: six-second input into
	// a minutely archive. Count input points, including overwritten slots.
	const cadence = 6
	end := f.base + int64(i/len(f.names)+1)*int64(batch)*cadence
	f.now = time.Unix(end+1, 0)
	input := make([]store.Point, batch)
	for j := range input {
		input[j] = store.Point{Timestamp: end - int64(batch-j)*cadence, Value: float64((i+j)%997) + .125}
	}
	ctx, timer := stagetime.Start(context.Background())
	if err := f.db.UpdateMany(ctx, f.names[i%len(f.names)], input); err != nil {
		t.Fatal(err)
	}
	timer.Finish()
	if i%32 == 31 {
		cfg := store.MetricConfig{Name: "tail.churn", Retentions: []store.Retention{{Step: 60, Count: 20160}}, AggregationMethod: store.Sum}
		if _, err := f.db.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(f.db.UpdateMany(ctx, cfg.Name, input[:1]), f.db.Delete(ctx, cfg.Name)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestContinuationTailReadStoreParity(t *testing.T) {
	plain, cached := newTailReadFixture(t, true), newTailReadFixture(t, false)
	tailReadStoreParity(t, plain, cached)
}

func tailReadStoreParity(t *testing.T, plain, cached *tailReadFixture) [2]uint64 {
	t.Helper()
	var reads [2]uint64
	ctx := context.Background()
	for i := range 64 {
		for side, f := range []*tailReadFixture{plain, cached} {
			count := store.CountStoreReadsForTest()
			f.update(t, i)
			reads[side] += count()
			if i == 31 {
				if err := errors.Join(f.db.Flush(), f.db.Compact(), f.db.Close()); err != nil {
					t.Fatal(err)
				}
				var err error
				f.db, err = openTestStore(f.dir, f.options)
				if err != nil {
					t.Fatal(err)
				}
				f.db.StopBackground()
			}
		}
		if i%16 != 15 {
			continue
		}
		left, err := plain.db.ExportTransfer(ctx, plain.names)
		if err != nil {
			t.Fatal(err)
		}
		right, err := cached.db.ExportTransfer(ctx, cached.names)
		if err != nil {
			t.Fatal(err)
		}
		for n := range left {
			if len(left[n].Keys) != len(right[n].Keys) {
				t.Fatal("different persistent key count")
			}
			for k, item := range left[n].Keys {
				if !bytes.Equal(item.Key, right[n].Keys[k].Key) || !bytes.Equal(item.Value, right[n].Keys[k].Value) {
					t.Fatalf("update %d metric %d key %d differs", i, n, k)
				}
			}
			for _, age := range []int{600, 14 * 86400, 100 * 86400} {
				until := int(plain.now.Unix())
				want, err := plain.db.Fetch(ctx, plain.names[n], until-age, until)
				if err != nil {
					t.Fatal(err)
				}
				got, err := cached.db.Fetch(ctx, cached.names[n], until-age, until)
				if err != nil {
					t.Fatal(err)
				}
				if want.FromTime != got.FromTime || want.Step != got.Step || len(want.Values) != len(got.Values) {
					t.Fatal("fetch shape differs")
				}
				for j, value := range want.Values {
					if math.Float64bits(value) != math.Float64bits(got.Values[j]) {
						t.Fatalf("fetch %s age %d point %d differs", plain.names[n], age, j)
					}
				}
			}
		}
	}
	return reads
}

func BenchmarkContinuationTailReadReuse(b *testing.B) {
	for _, keep := range []bool{true, false} {
		b.Run(fmt.Sprintf("reuse=%v", !keep), func(b *testing.B) {
			f := newTailReadFixture(b, keep)
			const batch = 564
			for i := range 32 {
				f.update(b, i)
			}
			if err := f.db.Flush(); err != nil {
				b.Fatal(err)
			}
			reads := store.CountStoreReadsForTest()
			stages := stagetime.Snapshot()
			var before, after syscall.Rusage
			syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				f.update(b, i+32)
			}
			b.StopTimer()
			syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			cpu := func(r syscall.Rusage) float64 {
				return float64(r.Utime.Sec+r.Stime.Sec)*1e6 + float64(r.Utime.Usec+r.Stime.Usec)
			}
			end := stagetime.Snapshot()
			points := float64(b.N * batch)
			b.ReportMetric((cpu(after)-cpu(before))/points, "cpu-us/point")
			b.ReportMetric(float64(reads())/points, "reads/point")
			b.ReportMetric(float64(end[stagetime.TailRead].Calls-stages[stagetime.TailRead].Calls)/points, "tail-reads/point")
		})
	}
}
