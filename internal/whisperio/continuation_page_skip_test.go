package whisperio

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// checkClassicGroupsAllocated requires every stored classic group of every
// classic file in kv to lie on allocated pages, and classicGroup to return
// exactly the stored group (an empty one where nothing is stored).
func checkClassicGroupsAllocated(t *testing.T, kv store.ContinuationIO) (groups, skipped int) {
	t.Helper()
	fs := newContinuationFiles(kv, true)
	if err := kv.Visit([]byte("n/"), func(key, value []byte) error {
		name := string(key[2:])
		in, err := fs.OpenFile(filepath.Join(fs.dir, name), os.O_RDONLY, 0600)
		if err != nil {
			return err
		}
		defer in.Close()
		f := in.(*continuationFile)
		if f.data.layout.Compressed {
			return nil
		}
		for ai, a := range f.data.layout.Archives {
			stored := map[int]bool{}
			if err := kv.Visit(continuationBlockPrefix(f.data.id, 'p', ai, 0), func(key, _ []byte) error {
				stored[int(binary.BigEndian.Uint32(key[16:]))] = true
				return nil
			}); err != nil {
				return err
			}
			for index := 0; index*128 < a.Count; index++ {
				groups++
				allocated := f.classicGroupAllocated(a, index)
				if stored[index] && !allocated {
					t.Fatalf("%s archive %d group %d is stored on unallocated pages", name, ai, index)
				}
				if !allocated {
					skipped++
				}
				got, err := f.classicGroup(ai, a, index, false)
				if err != nil {
					return err
				}
				want, err := f.pointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, index))
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(*got, *want) {
					t.Fatalf("%s archive %d group %d: classicGroup differs from storage", name, ai, index)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return groups, skipped
}

// TestClassicGroupSkipsOnlyUnallocated writes sparse classic files, as an OOO
// sidecar is, through random writes and zeroing writes, in both manifest
// formats, and requires every stored group to be found.
func TestClassicGroupSkipsOnlyUnallocated(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			kv := mapContinuationIO{}
			var io store.ContinuationIO = kv
			if buckets {
				io = bucketedMapIO{kv}
			}
			rng := rand.New(rand.NewSource(1))
			archives := []whisper.StorageArchiveLayout{{Step: 60, Count: 20160}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 400}}
			offset := 64
			for i := range archives {
				archives[i].Offset = offset
				offset += archives[i].Count * 12
			}
			layout := whisper.StorageFileLayout{HeaderSize: 64, Size: offset, Archives: archives}
			for round := 0; round < 20; round++ {
				fs := newContinuationFiles(io, false)
				name := whisper.OutOfOrderSidecarPath(fs.main())
				flags := os.O_RDWR
				if round == 0 {
					flags |= os.O_CREATE | os.O_EXCL
				}
				out, err := fs.OpenFile(name, flags, 0600)
				if err != nil {
					t.Fatal(err)
				}
				f := out.(*continuationFile)
				if round == 0 {
					if err := f.ConfigureStorage(layout); err != nil {
						t.Fatal(err)
					}
					if err := f.Truncate(int64(layout.Size)); err != nil {
						t.Fatal(err)
					}
				}
				for w := 0; w < 1+rng.Intn(6); w++ {
					a := archives[rng.Intn(len(archives))]
					slot := rng.Intn(a.Count)
					n := 1 + rng.Intn(min(4, a.Count-slot))
					record := make([]byte, 12*n)
					for i := 0; i < n; i++ {
						if rng.Intn(5) > 0 {
							binary.BigEndian.PutUint32(record[i*12:], uint32(1_700_000_000+slot+i))
							binary.BigEndian.PutUint64(record[i*12+4:], math.Float64bits(float64(rng.Intn(100))))
						}
					}
					if _, err := f.WriteAt(record, int64(a.Offset+slot*12)); err != nil {
						t.Fatal(err)
					}
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				if err := fs.flush(); err != nil {
					t.Fatal(err)
				}
			}
			groups, skipped := checkClassicGroupsAllocated(t, io)
			if skipped == 0 || skipped == groups {
				t.Fatalf("skipped %d of %d groups; want a sparse file", skipped, groups)
			}
			t.Logf("%d of %d classic groups proven absent from the allocation", skipped, groups)
		})
	}
}

// TestClassicGroupSkipImportedSidecar imports a disk CWhisper file with late
// points (a real sidecar), verifies it, which reads through the allocation,
// and checks every stored group lies on allocated pages.
func TestClassicGroupSkipImportedSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metric.wsp")
	now := 1_700_000_000
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(60, 20160), whisper.NewRetention(1800, 35040)}), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	for ts := now - 86400; ts <= now; ts += 60 {
		if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: ts, Value: float64(ts % 97)}}); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 40; i++ {
		late := now - 3600 - rng.Intn(80000)
		if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: late - late%60, Value: 1000 + float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if w.OutOfOrderPath() == "" {
		t.Fatal("no sidecar was created")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			kv := mapContinuationIO{}
			var io store.ContinuationIO = kv
			if buckets {
				io = bucketedMapIO{kv}
			}
			m := store.Metadata{MetricConfig: store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average}}
			c := &ContinuationCodec{keepImportedSidecars: true}
			if err := c.Import(context.Background(), io, path, m, clock); err != nil {
				t.Fatal(err)
			}
			// Verify reads only groups holding source slots, through the
			// allocation, so it fails if the import left one unallocated.
			if err := c.Verify(context.Background(), io, path, m); err != nil {
				t.Fatalf("verify through the allocation: %v", err)
			}
			groups, skipped := checkClassicGroupsAllocated(t, io)
			if skipped == 0 {
				t.Fatal("imported sidecar has no unallocated group to skip")
			}
			t.Logf("%d of %d imported sidecar groups proven absent", skipped, groups)
		})
	}
}

// TestClassicGroupSkipMatchesProbing drives two stores through the oracle's
// late, duplicate and skewed streams, one reading every classic group and one
// skipping unallocated groups, in both formats, and requires identical
// fetches and snapshots after every update.
func TestClassicGroupSkipMatchesProbing(t *testing.T) {
	defer func(v bool) { skipUnallocatedGroups = v }(skipUnallocatedGroups)
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			open := func() *store.Store {
				db, err := openMemoryTestStore(t, filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: &ContinuationCodec{ReuseBlockKeys: true},
					Now: func() time.Time { return time.Unix(int64(oracleNow), 0) }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { db.Close() })
				return db
			}
			skipping, probing := open(), open()
			var updates int
			skipsBefore := UnallocatedGroupSkips()
			for i, sc := range oracleScenarios(true) {
				if sc.e.lateRate == 0 && sc.e.dupRate == 0 && sc.e.skew == 0 {
					continue
				}
				name := fmt.Sprintf("skip.%d", i)
				for _, db := range []*store.Store{skipping, probing} {
					if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: sc.rets, AggregationMethod: sc.method, XFilesFactor: sc.xff}); err != nil {
						t.Fatal(err)
					}
				}
				rngs := []*rand.Rand{rand.New(rand.NewSource(int64(i))), rand.New(rand.NewSource(int64(i)))}
				start := 1_700_000_000
				step := sc.rets[0].Step
				var batches [][]store.Point
				var at []int
				for ts := start - min(step*sc.rets[0].Count, 3*86400); ts < start; ts += 6 * 3600 {
					var pts []store.Point
					for x := ts - ts%step; x < ts+6*3600 && x < start; x += step {
						pts = append(pts, store.Point{Timestamp: int64(x), Value: float64(x % 97)})
					}
					batches, at = append(batches, pts), append(at, min(ts+6*3600, start))
				}
				flushAt, stream := sc.e.batches(rand.New(rand.NewSource(int64(i))), start)
				batches, at = append(batches, stream...), append(at, flushAt...)
				for u, b := range batches {
					oracleNow = at[u]
					for k, db := range []*store.Store{skipping, probing} {
						skipUnallocatedGroups = k == 0
						rng := rngs[k]
						maintenance := store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute, Allow: func() bool { return rng.Intn(4) == 0 }}
						if err := db.UpdateManyMaintained(context.Background(), name, b, maintenance); err != nil {
							t.Fatal(err)
						}
					}
					updates++
					for _, r := range sc.rets {
						from := oracleNow - r.Step*r.Count + r.Step
						skipUnallocatedGroups = true
						got, err := skipping.Fetch(context.Background(), name, from, oracleNow)
						if err != nil {
							t.Fatal(err)
						}
						skipUnallocatedGroups = false
						want, err := probing.Fetch(context.Background(), name, from, oracleNow)
						if err != nil {
							t.Fatal(err)
						}
						if !sameSeries(got, want) {
							t.Fatalf("%s update %d step %d: skipping fetch differs from probing", name, u, r.Step)
						}
					}
				}
				skipUnallocatedGroups = true
				a, err := skipping.Snapshot(context.Background(), name)
				if err != nil {
					t.Fatal(err)
				}
				skipUnallocatedGroups = false
				b, err := probing.Snapshot(context.Background(), name)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(a.Archives, b.Archives) {
					t.Fatalf("%s: snapshots differ", name)
				}
			}
			if UnallocatedGroupSkips() == skipsBefore {
				t.Fatal("no classic group read was skipped")
			}
			t.Logf("%d updates identical; %d group reads skipped", updates, UnallocatedGroupSkips()-skipsBefore)
		})
	}
}

func sameSeries(a, b *store.Series) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.FromTime != b.FromTime || a.UntilTime != b.UntilTime || a.Step != b.Step || len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Values {
		x, y := a.Values[i], b.Values[i]
		if math.Float64bits(x) != math.Float64bits(y) && !(math.IsNaN(x) && math.IsNaN(y)) {
			return false
		}
	}
	return true
}

// probeCountingStoreIO counts storage reads of a fetch's continuation io.
type probeCountingIO struct {
	mapContinuationIO
	reads int
}

func (k *probeCountingIO) Get(key []byte) ([]byte, error) {
	k.reads++
	return k.mapContinuationIO.Get(key)
}

// BenchmarkContinuationSparseSidecarFetch fetches a day of a 1m metric whose
// sidecar holds late=N points spread over that day, reading every classic
// group or skipping those the allocation proves absent.
func BenchmarkContinuationSparseSidecarFetch(b *testing.B) {
	defer func(v bool) { skipUnallocatedGroups = v }(skipUnallocatedGroups)
	now := 1_700_000_000
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average}}
	c := &ContinuationCodec{}
	for _, late := range []int{1, 2, 8} {
		kv := &probeCountingIO{mapContinuationIO: mapContinuationIO{}}
		if err := c.Create(context.Background(), kv, m, clock); err != nil {
			b.Fatal(err)
		}
		update := func(pts []store.Point) {
			if err := c.Update(context.Background(), kv, m, pts, -1, clock, nil); err != nil {
				b.Fatal(err)
			}
		}
		for ts := now - 3*86400; ts < now; ts += 3600 {
			pts := make([]store.Point, 0, 60)
			for x := ts; x < ts+3600; x += 60 {
				pts = append(pts, store.Point{Timestamp: int64(x), Value: float64(x % 97)})
			}
			update(pts)
		}
		for i := 0; i < late; i++ {
			update([]store.Point{{Timestamp: int64(now - 7200 - i*(72000/late)), Value: 1000}})
		}
		for _, skip := range []bool{false, true} {
			b.Run(fmt.Sprintf("late=%d/skip=%v", late, skip), func(b *testing.B) {
				skipUnallocatedGroups = skip
				kv.reads = 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := c.Fetch(context.Background(), kv, m, now-86400, now, clock); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(kv.reads)/float64(b.N), "reads/op")
			})
		}
	}
}
