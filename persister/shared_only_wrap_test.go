package persister

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

// Qualify ongoing shared-only writes after reclamation, including the source's
// buffered rollups and late duplicate behavior across circular retention wraps.
func TestSharedOnlyRetentionWrapVisibleRollups(t *testing.T) {
	sharedOnlyRetentionWrap(t, "1s:20s,5s:2m,30s:10m")
}

func TestSharedOnlySingleRetentionWrap(t *testing.T) {
	sharedOnlyRetentionWrap(t, "1s:20s")
}

func sharedOnlyRetentionWrap(t *testing.T, retentionDefs string) {
	if storageClockMatrix(t) {
		return
	}
	now := time.Unix(1792432631, 0)
	previous := whisper.Now
	whisper.Now = func() time.Time { return now }
	defer func() { whisper.Now = previous }()
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("%s/xff%g", method, xff), func(t *testing.T) {
				base := int64(1792432631)
				now = time.Unix(base+2, 0)
				root := t.TempDir()
				cache, referenceCache := &fakeCache{}, &fakeCache{}
				p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
				reference := newOOOTestPersister(t, filepath.Join(root, "reference"), referenceCache)
				p.SetFLock(true)
				for _, writer := range []*Whisper{p, reference} {
					writer.schemas[0].Retentions = whisper.MustParseRetentionDefs(retentionDefs)
					writer.aggregation.Default.aggregationMethod = method
					writer.aggregation.Default.xFilesFactor = float64(xff)
				}
				const metric = "edge"
				add := func(timestamp int64, value float64) {
					cache.add(metric, timestamp, value)
					referenceCache.add(metric, timestamp, value)
				}
				for age := 18; age > 0; age-- {
					add(base-int64(age), float64(age)+.1)
				}
				p.store(metric)
				reference.store(metric)
				destination := filepath.Join(root, "exact")
				db, err := openTestStore(t, destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath, Now: func() time.Time { return now }}))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				p.SetMetricStore(db)
				if err := p.SetIncrementalMigration(db, destination); err != nil {
					t.Fatal(err)
				}
				if _, err := p.ConvertFile(context.Background(), metric); err != nil {
					t.Fatal(err)
				}
				for seq := 0; seq < 40; seq++ {
					stamp := base + int64(3*(seq+1))
					now = time.Unix(stamp+2, 0)
					for _, age := range []int64{3, 2, 1} {
						add(stamp-age, float64(seq)+float64(age)/4+.1)
					}
					if seq >= 3 {
						add(stamp-17, 111.25+float64(seq))
						add(stamp-17, 222.5+float64(seq))
					}
					p.store(metric)
					reference.store(metric)
					now = time.Unix(stamp+3, 0)
					w, err := whisper.OpenWithOptions(filepath.Join(reference.rootPath, "edge.wsp"), &whisper.Options{OutOfOrder: true})
					if err != nil {
						t.Fatal(err)
					}
					for _, age := range []int{8, 20, 100, 240} {
						from, until := int(now.Unix())-age, int(now.Unix())-1
						want, err := w.Fetch(from, until)
						if err != nil {
							t.Fatal(err)
						}
						got, err := db.Fetch(context.Background(), metric, from, until)
						if err != nil {
							t.Fatal(err)
						}
						if want == nil || got == nil || want.FromTime() != got.FromTime || want.Step() != got.Step || len(want.Values()) != len(got.Values) {
							t.Fatalf("fetch shape round%d age%d", seq, age)
						}
						for i, value := range want.Values() {
							actual := got.Values[i]
							if math.IsNaN(value) && math.IsNaN(actual) {
								continue
							}
							if math.Float64bits(value) != math.Float64bits(actual) {
								t.Errorf("round%d age%d timestamp%d: cwhisper=%g shared=%g", seq, age, got.FromTime+i*got.Step, value, actual)
								break
							}
						}
					}
					_ = w.Close()
					if t.Failed() {
						return
					}
				}
				assertOriginalsReclaimed(t, p, metric)
			})
		}
	}
}
