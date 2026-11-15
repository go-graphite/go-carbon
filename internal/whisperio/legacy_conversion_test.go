package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestStoredConversionSnapshotPreservesPhysicalCoarseArchives(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1794800000, 0)
			clock := func() time.Time { return now }
			path := filepath.Join(t.TempDir(), "source.wsp")
			defs, err := whisper.ParseRetentionDefs("1s:1h,1m:1d,1h:30d,1d:400d")
			if err != nil {
				t.Fatal(err)
			}
			w, err := whisper.CreateWithOptions(path, defs, whisper.Average, .5, &whisper.Options{Compressed: compressed, Now: clock, PointsPerBlock: 32, OutOfOrder: true})
			if err != nil {
				t.Fatal(err)
			}
			m := store.Metadata{ID: 1, Generation: 1, MetricConfig: store.MetricConfig{Name: "stored", AggregationMethod: store.Average, XFilesFactor: .5}}
			for ai, def := range defs {
				step := def.SecondsPerPoint()
				m.Retentions = append(m.Retentions, store.Retention{Step: step, Count: def.NumberOfPoints()})
				var points []*whisper.TimeSeriesPoint
				for i := 0; i < 72; i++ {
					// Coarse values deliberately do not equal the fine reducer.
					p := &whisper.TimeSeriesPoint{Time: int(now.Unix())/step*step - (73-i)*step, Value: float64(ai*10000+i) + .125}
					points = append(points, p)
				}
				if err := w.UpdateManyForArchive(points, step*def.NumberOfPoints()); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			flag := os.O_RDONLY
			w, err = whisper.OpenWithOptions(path, &whisper.Options{OpenFileFlag: &flag, Now: clock, OutOfOrder: true})
			if err != nil {
				t.Fatal(err)
			}
			want := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(defs))}
			for ai := range defs {
				points, err := w.ArchivePoints(ai)
				if err != nil {
					t.Fatal(err)
				}
				want.Archives[ai].Retention = m.Retentions[ai]
				for _, p := range points {
					want.Archives[ai].Points = append(want.Archives[ai].Points, store.Point{Timestamp: int64(p.Time), Value: p.Value})
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			kv := mapContinuationIO{}
			codec := &ContinuationCodec{}
			if err := codec.Import(ctx, kv, path, m, clock); err != nil {
				t.Fatal(err)
			}
			before := mapContinuationIO{}
			for k, v := range kv {
				before[k] = append([]byte(nil), v...)
			}
			got, err := codec.StoredSnapshot(ctx, kv, m, clock)
			if err != nil {
				t.Fatal(err)
			}
			requireSnapshotPointBits(t, want, got)
			if !reflect.DeepEqual(before, kv) {
				t.Fatal("stored export mutated source continuation keys")
			}
			// Independent direct archive slots are the oracle above. Test the
			// exact coarse marker too, so recomputing from fine values cannot pass.
			if len(got.Archives[3].Points) == 0 || math.Float64bits(got.Archives[3].Points[0].Value) != math.Float64bits(30000.125) {
				t.Fatal("stored coarse marker changed")
			}
		})
	}
}
