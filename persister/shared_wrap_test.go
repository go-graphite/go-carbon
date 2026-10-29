package persister

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Drive the actual compressed file writer through multiple circular wraps. The
// late duplicate stays in the finest retention but changes a buffered rollup.
func TestMirrorRetentionWrapVisibleRollups(t *testing.T) {
	if storageClockMatrix(t) {
		return
	}
	now := time.Unix(1792432631, 0)
	previous := whisper.Now
	whisper.Now = func() time.Time { return now }
	defer func() { whisper.Now = previous }()
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		for _, xff := range []float32{0, 0.5, 1} {
			t.Run(fmt.Sprintf("%s/xff%g", method, xff), func(t *testing.T) {
				base := int64(1792432631)
				now = time.Unix(base+2, 0)
				root := t.TempDir()
				cache := &fakeCache{}
				p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
				p.schemas[0].Retentions = whisper.MustParseRetentionDefs("1s:20s,5s:2m,30s:10m")
				p.aggregation.Default.aggregationMethod = method
				p.aggregation.Default.xFilesFactor = float64(xff)
				db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true, Now: func() time.Time { return now }})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				p.SetMetricStore(db)
				p.SetMirrorFiles(true)
				const metric = "edge"
				for age := 18; age > 0; age-- {
					cache.add(metric, base-int64(age), float64(age)+0.1)
				}
				p.store(metric)
				for seq := 0; seq < 40; seq++ {
					stamp := base + int64(3*(seq+1))
					now = time.Unix(stamp+2, 0)
					for _, age := range []int64{3, 2, 1} {
						cache.add(metric, stamp-age, float64(seq)+float64(age)/4+0.1)
					}
					if seq >= 3 {
						cache.add(metric, stamp-17, 111.25+float64(seq))
						cache.add(metric, stamp-17, 222.5+float64(seq))
					}
					p.store(metric)
					now = time.Unix(stamp+3, 0)
					w, err := whisper.OpenWithOptions(filepath.Join(root, "original", "edge.wsp"), &whisper.Options{OutOfOrder: true})
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
						if want == nil || got == nil {
							t.Fatalf("nil fetch at round%d age%d", seq, age)
						}
						if want.FromTime() != got.FromTime || want.Step() != got.Step || len(want.Values()) != len(got.Values) {
							t.Fatalf("shape round%d age%d", seq, age)
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
			})
		}
	}
}
