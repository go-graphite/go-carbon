package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// recordingIO logs every call the codec makes and what it observed.
type recordingIO struct {
	store.ContinuationIO
	log *[]string
}

func (r recordingIO) add(format string, args ...any) {
	*r.log = append(*r.log, fmt.Sprintf(format, args...))
}

func (r recordingIO) TimeBuckets() bool {
	b, ok := r.ContinuationIO.(store.ContinuationTimeBuckets)
	return ok && b.TimeBuckets()
}

func (r recordingIO) Get(key []byte) ([]byte, error) {
	value, err := r.ContinuationIO.Get(key)
	r.add("get %q = %q %v", key, value, err)
	return value, err
}

func (r recordingIO) Probe(key []byte) ([]byte, error) {
	value, err := r.ContinuationIO.(store.ContinuationProber).Probe(key)
	r.add("probe %q = %q %v", key, value, err)
	return value, err
}

func (r recordingIO) Set(key, value []byte) error {
	err := r.ContinuationIO.Set(key, value)
	r.add("set %q %q %v", key, value, err)
	return err
}

func (r recordingIO) Delete(key []byte) error {
	err := r.ContinuationIO.Delete(key)
	r.add("delete %q %v", key, err)
	return err
}

func (r recordingIO) DeletePrefix(prefix []byte) error {
	err := r.ContinuationIO.DeletePrefix(prefix)
	r.add("delete prefix %q %v", prefix, err)
	return err
}

func (r recordingIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	err := r.ContinuationIO.Visit(prefix, func(key, value []byte) error {
		r.add("visit %q: %q = %q", prefix, key, value)
		return visit(key, value)
	})
	r.add("visit %q end %v", prefix, err)
	return err
}

// recordingCodec records the io traffic of each update.
type recordingCodec struct {
	*ContinuationCodec
	log []string
}

func (c *recordingCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	return c.ContinuationCodec.Update(ctx, recordingIO{kv, &c.log}, m, input, retention, clock, maintenance)
}

// Real codec updates, with OOO sidecars, block rotation, sidecar merges,
// flushes and compactions, must make the same reads, see the same values and
// write the same keys through the shared update iterator as through a lookup
// per read.
func TestContinuationSharedUpdateIteratorMatchesPointLookups(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%v", buckets), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			const base = int64(1792432631)
			now := time.Unix(base+2, 0)
			clock := func() time.Time { return now }
			root := t.TempDir()
			sourceDir := filepath.Join(root, "source")
			if err := os.Mkdir(sourceDir, 0700); err != nil {
				t.Fatal(err)
			}
			const metrics = 4
			for i := 0; i < metrics; i++ {
				w, err := whisper.CreateWithOptions(filepath.Join(sourceDir, fmt.Sprintf("m%d.wsp", i)), whisper.MustParseRetentionDefs("1s:5m,10s:1h,1m:1d"), whisper.Average, .5,
					&whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: 40, Now: clock})
				if err != nil {
					t.Fatal(err)
				}
				var seed []*whisper.TimeSeriesPoint
				for age := 200; age > 0; age-- {
					seed = append(seed, &whisper.TimeSeriesPoint{Time: int(base) - age, Value: float64(age*i) + .5})
				}
				if err := errors.Join(w.UpdateMany(seed), w.Close()); err != nil {
					t.Fatal(err)
				}
			}
			var codecs [2]*recordingCodec
			var dbs [2]*store.Store
			for side := range dbs {
				codecs[side] = &recordingCodec{ContinuationCodec: &ContinuationCodec{ReuseBlockKeys: true}}
				db, err := openTestStore(filepath.Join(root, fmt.Sprintf("shared%d", side)), store.Options{ExactBlocks: true, Continuation: codecs[side], IncrementalSource: sourceDir,
					TimeBuckets: buckets, Now: clock, SyncInterval: time.Hour, MemTableSize: 1 << 20, BloomFilterBits: 10, L0TargetFileSize: 4 << 10,
					L0CompactionThreshold: 8, UpdatePointLookups: side == 1})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				dbs[side] = db
				for i := 0; i < metrics; i++ {
					name := fmt.Sprintf("m%d", i)
					capture, err := ReadSnapshotIn(ctx, name, filepath.Join(sourceDir, name+".wsp"), root)
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
				}
			}
			for seq := 0; seq < 240; seq++ {
				stamp := base + int64(3*(seq+1))
				now = time.Unix(stamp+2, 0)
				name := fmt.Sprintf("m%d", seq%metrics)
				input := []store.Point{{Timestamp: stamp - 3, Value: float64(seq)}, {Timestamp: stamp - 2, Value: .25}, {Timestamp: stamp - 1, Value: -1}}
				if seq%5 == 2 {
					// Late points fill the sidecar; some collide with stored slots.
					input = append(input, store.Point{Timestamp: stamp - 40 - int64(seq%7), Value: 111}, store.Point{Timestamp: stamp - 290, Value: 222})
				}
				merge := seq%17 == 16
				for side, db := range dbs {
					codecs[side].log = codecs[side].log[:0]
					if err := db.UpdateManyMaintained(ctx, name, input, store.ContinuationMaintenance{MinPoints: 1, Allow: func() bool { return merge }}); err != nil {
						t.Fatalf("update %d side %d: %v", seq, side, err)
					}
				}
				// flush writes each file in map order, so compare the calls as
				// multisets: its files' keys are disjoint.
				for _, c := range codecs {
					sort.Strings(c.log)
				}
				if !reflect.DeepEqual(codecs[0].log, codecs[1].log) {
					for i := range min(len(codecs[0].log), len(codecs[1].log)) {
						if codecs[0].log[i] != codecs[1].log[i] {
							t.Fatalf("update %d call %d: shared %s, lookups %s", seq, i, codecs[0].log[i], codecs[1].log[i])
						}
					}
					t.Fatalf("update %d: %d calls through the shared iterator, %d through lookups", seq, len(codecs[0].log), len(codecs[1].log))
				}
				for _, db := range dbs {
					switch {
					case seq%11 == 10:
						if err := db.Flush(); err != nil {
							t.Fatal(err)
						}
					case seq%61 == 60:
						if err := db.Compact(); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			for i := 0; i < metrics; i++ {
				name := fmt.Sprintf("m%d", i)
				var series [2]*store.Series
				for side, db := range dbs {
					s, err := db.Fetch(ctx, name, int(now.Unix())-3600, int(now.Unix()))
					if err != nil {
						t.Fatal(err)
					}
					series[side] = s
				}
				if series[0] == nil || series[1] == nil || series[0].FromTime != series[1].FromTime || series[0].UntilTime != series[1].UntilTime ||
					series[0].Step != series[1].Step || len(series[0].Values) != len(series[1].Values) {
					t.Fatalf("%s fetches differ: %+v, %+v", name, series[0], series[1])
				}
				for j, v := range series[0].Values {
					if math.Float64bits(v) != math.Float64bits(series[1].Values[j]) {
						t.Fatalf("%s value %d differs: %v, %v", name, j, v, series[1].Values[j])
					}
				}
			}
		})
	}
}
