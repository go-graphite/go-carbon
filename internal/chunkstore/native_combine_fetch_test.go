package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestNativeCombinedSparseArchivesRemainFetchable(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("bucket=%v/cache=%v", buckets, cached), func(t *testing.T) {
				ctx := context.Background()
				now := time.Unix(1000080, 0)
				open := func() *store.Store {
					var cache int64
					if cached {
						cache = 8 << 20
					}
					s, err := store.Open("/db", store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: &whisperio.ContinuationCodec{}, MetadataCacheSize: cache, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return now }}.WithFS(vfs.NewMem()))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := s.Close(); err != nil {
							t.Error(err)
						}
					})
					return s
				}
				source, target := open(), open()
				cfg := store.MetricConfig{Name: "native.sparse.fetch", Retentions: []store.Retention{{Step: 10, Count: 60}, {Step: 60, Count: 60}}, AggregationMethod: store.Sum}
				for _, s := range []*store.Store{source, target} {
					if _, err := s.Create(ctx, cfg); err != nil {
						t.Fatal(err)
					}
				}
				for _, input := range []struct {
					points []store.Point
					age    int
				}{
					{[]store.Point{{Timestamp: 999780, Value: 3}, {Timestamp: 999990, Value: math.Copysign(0, -1)}}, 600},
					{[]store.Point{{Timestamp: 999000, Value: 41}, {Timestamp: 999600, Value: 0}, {Timestamp: 999960, Value: 29}}, 3600},
				} {
					if err := source.UpdateManyForArchive(ctx, cfg.Name, input.points, input.age); err != nil {
						t.Fatal(err)
					}
				}
				want, err := source.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				if cached {
					if _, err := target.Fetch(ctx, cfg.Name, 999500, 1000079); err != nil {
						t.Fatal(err)
					}
				}
				pin, err := source.PinNativeSource()
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Close()
				var wire bytes.Buffer
				if err := pin.Write(ctx, []string{cfg.Name}, &wire); err != nil {
					t.Fatal(err)
				}
				request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{cfg.Name}, Combine: true}
				if _, err := target.NativeImport(ctx, request, &wire); err != nil {
					t.Fatal(err)
				}
				check := func() {
					t.Helper()
					for _, archive := range want.Archives {
						r := archive.Retention
						series, err := target.Fetch(ctx, cfg.Name, int(now.Unix())-r.MaxRetention()+2*r.Step, int(now.Unix())-1)
						if err != nil || series == nil || series.Step != r.Step {
							t.Fatal("wrong render archive", series, err)
						}
						for _, p := range archive.Points {
							i := (int(p.Timestamp) - series.FromTime) / series.Step
							if i < 0 || i >= len(series.Values) || math.Float64bits(series.Values[i]) != math.Float64bits(p.Value) {
								t.Fatalf("stored point invisible to Fetch step%d point%+v", r.Step, p)
							}
						}
					}
				}
				check()
				if err := target.UpdateManyForArchive(ctx, cfg.Name, []store.Point{{Timestamp: 1000050, Value: 71}}, 600); err != nil {
					t.Fatal(err)
				}
				check()
				got, err := target.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := target.VerifyTransferFetch(ctx, got); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
