package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// bucketedMapIO is an in-memory store with format v7 enabled.
type bucketedMapIO struct{ mapContinuationIO }

func (bucketedMapIO) TimeBuckets() bool { return true }

type switchableBucketsIO struct {
	mapContinuationIO
	buckets *bool
}

func (io switchableBucketsIO) TimeBuckets() bool { return *io.buckets }

// TestContinuationTimeBucketsMatchSource drives a disk CWhisper file and its
// continuation with identical updates while the clock crosses many days. Blocks
// rotate into new buckets; reads must match the source, and every bucketed key
// must belong to a live block that its descriptor places in that bucket. The
// store starts in v6 and switches to v7 halfway, as an upgraded store does.
func TestContinuationTimeBucketsMatchSource(t *testing.T) {
	for _, upgrade := range []bool{false, true} {
		t.Run(fmt.Sprintf("upgrade=%v", upgrade), func(t *testing.T) {
			const base = int64(1792432800)
			now := time.Unix(base, 0)
			clock := func() time.Time { return now }
			retentions := whisper.MustParseRetentionDefs("1h:20d,6h:60d")
			options := func(fs whisper.StorageFileSystem) *whisper.Options {
				return &whisper.Options{FileSystem: fs, Now: clock, Compressed: true, OutOfOrder: true, PointsPerBlock: 6}
			}
			path := filepath.Join(t.TempDir(), "source.wsp")
			src, err := whisper.CreateWithOptions(path, retentions, whisper.Average, 0, options(nil))
			if err != nil {
				t.Fatal(err)
			}
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			buckets := !upgrade
			kv := switchableBucketsIO{mapContinuationIO{}, &buckets}
			fs := newContinuationFiles(kv, false).withClock(clock)
			dst, err := whisper.CreateWithOptions(fs.main(), retentions, whisper.Average, 0, options(fs))
			if err != nil {
				t.Fatal(err)
			}
			if err := dst.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fs.flush(); err != nil {
				t.Fatal(err)
			}
			const rounds = 24 * 45
			for round := 1; round <= rounds; round++ {
				now = time.Unix(base+int64(round)*3600, 0)
				if upgrade && round == rounds/2 {
					buckets = true
				}
				points := []*whisper.TimeSeriesPoint{{Time: int(now.Unix()) - 60, Value: math.Sin(float64(round)) * 1e3}}
				if round%11 == 0 {
					// A late point rewrites an older block out of order.
					points = append(points, &whisper.TimeSeriesPoint{Time: int(now.Unix()) - 5*3600 - 60, Value: float64(round)})
				}
				for _, target := range []struct {
					fs   whisper.StorageFileSystem
					path string
				}{{nil, path}, {fs, ""}} {
					name := target.path
					if target.fs != nil {
						fs = newContinuationFiles(kv, false).withClock(clock)
						target.fs, name = fs, fs.main()
					}
					w, err := whisper.OpenWithOptions(name, options(target.fs))
					if err != nil {
						t.Fatal(err)
					}
					if err := w.UpdateMany(points); err != nil {
						t.Fatalf("round %d: %v", round, err)
					}
					if round%97 == 0 {
						if err := w.MergeOutOfOrder(); err != nil {
							t.Fatal(err)
						}
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					if target.fs != nil {
						if err := fs.flush(); err != nil {
							t.Fatal(err)
						}
					}
				}
				if round%24 == 0 || round == rounds {
					sameFetches(t, path, kv, clock, options, round)
					checkBucketedKeys(t, kv.mapContinuationIO, round)
				}
			}
			used := bucketsInUse(t, kv.mapContinuationIO)
			if len(used) < 3 {
				t.Fatalf("blocks used %d buckets; want rotation across days", len(used))
			}
			if !upgrade {
				for k := range kv.mapContinuationIO {
					if len(k) >= 10 && k[0] == 'f' && k[9] == 'c' {
						t.Fatalf("v6 group key %q in a v7 store", k)
					}
				}
			}
			if err := (&ContinuationCodec{}).Drop(context.Background(), kv); err != nil {
				t.Fatal(err)
			}
			for k := range kv.mapContinuationIO {
				if k[0] == 'T' {
					t.Fatalf("bucketed key %q survived Drop", k)
				}
			}
		})
	}
}

func sameFetches(t *testing.T, path string, kv store.ContinuationIO, clock func() time.Time, options func(whisper.StorageFileSystem) *whisper.Options, round int) {
	t.Helper()
	readOnly := os.O_RDONLY
	srcOptions := options(nil)
	srcOptions.OpenFileFlag = &readOnly
	src, err := whisper.OpenWithOptions(path, srcOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	fs := newContinuationFiles(kv, true)
	dstOptions := options(fs)
	dstOptions.OpenFileFlag = &readOnly
	dst, err := whisper.OpenWithOptions(fs.main(), dstOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	now := int(clock().Unix())
	for _, age := range []int{6 * 3600, 3 * 86400, 19 * 86400, 59 * 86400} {
		want, err := src.Fetch(now-age, now)
		if err != nil {
			t.Fatal(err)
		}
		got, err := dst.Fetch(now-age, now)
		if err != nil {
			t.Fatalf("round %d age %d: %v", round, age, err)
		}
		if (want == nil) != (got == nil) {
			t.Fatalf("round %d age %d: presence differs", round, age)
		}
		if want == nil {
			continue
		}
		if want.FromTime() != got.FromTime() || want.Step() != got.Step() || len(want.Values()) != len(got.Values()) {
			t.Fatalf("round %d age %d: shape differs", round, age)
		}
		for i, v := range want.Values() {
			if math.Float64bits(v) != math.Float64bits(got.Values()[i]) && !(math.IsNaN(v) && math.IsNaN(got.Values()[i])) {
				t.Fatalf("round %d age %d slot %d: source=%g continuation=%g", round, age, i, v, got.Values()[i])
			}
		}
	}
}

// checkBucketedKeys requires every bucketed group to sit in its block's
// recorded bucket, below the block's point count.
func checkBucketedKeys(t *testing.T, kv mapContinuationIO, round int) {
	t.Helper()
	for k := range kv {
		if k[0] != 'T' {
			continue
		}
		key := []byte(k)
		if len(key) != 5+16+4 || key[5] != 'f' || key[14] != 'c' {
			t.Fatalf("round %d: malformed bucketed key %x", round, key)
		}
		descriptorKey := append([]byte(nil), key[5:21]...)
		descriptorKey[9] = 'b'
		value, ok := kv[string(descriptorKey)]
		if !ok {
			t.Fatalf("round %d: orphan bucketed group %x", round, key)
		}
		d, err := decodeBlockDescriptor(value)
		if err != nil {
			t.Fatal(err)
		}
		group := int(binary.BigEndian.Uint32(key[21:]))
		if !d.bucketed || d.bucket != binary.BigEndian.Uint32(key[1:5]) || group*128 >= d.count {
			t.Fatalf("round %d: stale bucketed group %x for descriptor %+v", round, key, d)
		}
	}
}

func bucketsInUse(t *testing.T, kv mapContinuationIO) map[uint32]bool {
	t.Helper()
	used := make(map[uint32]bool)
	for k := range kv {
		if k[0] == 'T' {
			used[binary.BigEndian.Uint32([]byte(k)[1:5])] = true
		}
	}
	return used
}

// Truncating or unlinking a file must remove its bucketed groups too.
func TestContinuationTimeBucketsDeleteFile(t *testing.T) {
	now := time.Unix(1792432800, 0)
	clock := func() time.Time { return now }
	kv := bucketedMapIO{mapContinuationIO{}}
	fs := newContinuationFiles(kv, false).withClock(clock)
	options := &whisper.Options{FileSystem: fs, Now: clock, Compressed: true, PointsPerBlock: 6}
	w, err := whisper.CreateWithOptions(fs.main(), whisper.MustParseRetentionDefs("1h:40d"), whisper.Average, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 24*30; i++ {
		now = now.Add(time.Hour)
		if w, err = whisper.OpenWithOptions(fs.main(), options); err != nil {
			t.Fatal(err)
		}
		if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now.Unix()) - 1, Value: math.Sin(float64(i)) * 1e6}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.flush(); err != nil {
		t.Fatal(err)
	}
	if len(bucketsInUse(t, kv.mapContinuationIO)) < 2 {
		t.Fatal("fixture must span buckets")
	}
	f, err := fs.OpenFile(fs.main(), os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	for k := range kv.mapContinuationIO {
		if bytes.HasPrefix([]byte(k), []byte{'T'}) {
			t.Fatalf("bucketed key %x survived truncate", k)
		}
	}
}

// A conversion puts each archive's newest block, which keeps receiving
// appends, in today's bucket and the rest of its history in the history bucket.
func TestContinuationImportSeparatesHistoryFromTails(t *testing.T) {
	now := time.Unix(1792432800, 0)
	clock := func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "m.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1h:40d,1d:2y"), whisper.Average, 0,
		&whisper.Options{Compressed: true, Now: clock, PointsPerBlock: 6})
	if err != nil {
		t.Fatal(err)
	}
	var points []*whisper.TimeSeriesPoint
	for age := 39 * 24; age > 0; age-- {
		points = append(points, &whisper.TimeSeriesPoint{Time: int(now.Unix()) - age*3600, Value: math.Sin(float64(age)) * 1e6})
	}
	if err := w.UpdateMany(points); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	kv := bucketedMapIO{mapContinuationIO{}}
	m := store.Metadata{MetricConfig: store.MetricConfig{AggregationMethod: store.Average, Retentions: []store.Retention{{Step: 3600, Count: 960}, {Step: 86400, Count: 730}}}}
	if err := (&ContinuationCodec{}).Import(context.Background(), kv, path, m, clock); err != nil {
		t.Fatal(err)
	}
	today := uint32(now.Unix() / continuationBucketSeconds)
	tails, history := map[uint16]int{}, 0
	for k, v := range kv.mapContinuationIO {
		if len(k) != 16 || k[9] != 'b' {
			continue
		}
		d, err := decodeBlockDescriptor(v)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case !d.bucketed:
			t.Fatalf("imported v6 block %x", k)
		case d.bucket == today:
			tails[binary.BigEndian.Uint16([]byte(k)[10:])]++
		case d.bucket == continuationHistoryBucket:
			history++
		default:
			t.Fatalf("imported block in bucket %d", d.bucket)
		}
	}
	if len(tails) != 2 || tails[0] != 1 || tails[1] != 1 || history == 0 {
		t.Fatalf("tails per archive %v, history blocks %d", tails, history)
	}
	checkBucketedKeys(t, kv.mapContinuationIO, 0)
}

type headerCountingIO struct {
	bucketedMapIO
	pageBytes, manifests int
}

func (c *headerCountingIO) Set(k, v []byte) error {
	if len(k) == 12 && k[0] == 'f' && k[9] == 'H' {
		c.pageBytes += len(v)
	}
	if len(k) == 10 && k[0] == 'f' && k[9] == 'h' {
		c.manifests++
	}
	return c.bucketedMapIO.Set(k, v)
}

// In a v7 store an update rewrites only the header pages it changed, and the
// manifest only when layout or allocation changes; reads are unchanged.
func TestContinuationPagedHeaderWritesDirtyPages(t *testing.T) {
	const base = int64(1792368000)
	now := time.Unix(base, 0)
	clock := func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "m.wsp")
	defs := "1s:1d,1m:30d,1h:1y,1d:10y"
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(defs), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	var seed []*whisper.TimeSeriesPoint
	for ts := base - 86400; ts < base; ts++ {
		seed = append(seed, &whisper.TimeSeriesPoint{Time: int(ts), Value: math.Floor(math.Sin(float64(ts)/97) * 1000)})
	}
	if err := w.UpdateMany(seed); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	kv := &headerCountingIO{bucketedMapIO: bucketedMapIO{mapContinuationIO{}}}
	m := store.Metadata{MetricConfig: store.MetricConfig{AggregationMethod: store.Average,
		Retentions: []store.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}}}
	c := &ContinuationCodec{}
	if err := c.Import(context.Background(), kv, path, m, clock); err != nil {
		t.Fatal(err)
	}
	src, err := whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	headerSize := 0
	if d, err := newContinuationFiles(kv, true).load("main.wsp"); err == nil {
		headerSize = len(d.header)
	} else {
		t.Fatal(err)
	}
	kv.pageBytes, kv.manifests = 0, 0
	const rounds = 50
	for round := 1; round <= rounds; round++ {
		now = time.Unix(base+int64(round)*300, 0)
		input := make([]store.Point, 300)
		points := make([]*whisper.TimeSeriesPoint, 300)
		for j := range input {
			ts := now.Unix() - int64(300-j)
			v := math.Floor(math.Sin(float64(ts)/97) * 1000)
			input[j] = store.Point{Timestamp: ts, Value: v}
			points[j] = &whisper.TimeSeriesPoint{Time: int(ts), Value: v}
		}
		if err := c.Update(context.Background(), kv, m, input, -1, clock, nil); err != nil {
			t.Fatal(err)
		}
		if err := src.UpdateMany(points); err != nil {
			t.Fatal(err)
		}
	}
	if kv.manifests != 0 {
		t.Fatalf("manifest rewritten %d times by ordinary updates", kv.manifests)
	}
	if per := kv.pageBytes / rounds; per*2 > headerSize {
		t.Fatalf("header pages %d B/update of a %d B header", per, headerSize)
	}
	for _, age := range []int{3600, 86400, 30 * 86400, 365 * 86400} {
		from, until := int(now.Unix())-age, int(now.Unix())
		want, err := src.Fetch(from, until)
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.Fetch(context.Background(), kv, m, from, until, clock)
		if err != nil {
			t.Fatal(err)
		}
		if want.FromTime() != got.FromTime || len(want.Values()) != len(got.Values) {
			t.Fatalf("age %d: shape differs", age)
		}
		for i, v := range want.Values() {
			if math.Float64bits(v) != math.Float64bits(got.Values[i]) && !(math.IsNaN(v) && math.IsNaN(got.Values[i])) {
				t.Fatalf("age %d slot %d: source %g continuation %g", age, i, v, got.Values[i])
			}
		}
	}
	t.Logf("header %d B; pages written %d B/update", headerSize, kv.pageBytes/rounds)
}
