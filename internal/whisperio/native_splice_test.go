package whisperio

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
	whisper "github.com/go-graphite/go-whisper"
)

func TestNativeSplicePreservesHistoryAndFutureWrites(t *testing.T) {
	for _, mode := range []struct {
		name                      string
		buckets, sealed, floating bool
	}{{"v6", false, false, false}, {"v7", true, false, false}, {"v9", true, true, false}, {"v11", true, true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1794000000, 0)
			clock := func() time.Time { return now }
			open := func() *store.Store {
				opts := coldOptions(&ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true})
				opts.Now = clock
				opts.TimeBuckets = mode.buckets
				if mode.sealed {
					opts.ColdPack = store.ColdPackOptions{Mode: "exact", FloatCodec: mode.floating, WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}
				}
				s, err := store.Open("/db", opts.WithFS(vfs.NewMem()))
				if err != nil {
					t.Fatal(err)
				}
				s.StopBackground()
				t.Cleanup(func() {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				})
				return s
			}
			source, target, reference := open(), open(), open()
			cfg := store.MetricConfig{Name: "splice." + mode.name, Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 3600, Count: 2000}}, AggregationMethod: store.Sum}
			for _, s := range []*store.Store{source, target, reference} {
				if _, err := s.Create(ctx, cfg); err != nil {
					t.Fatal(err)
				}
			}
			start := now.Unix() - 19000*60
			var history []store.Point
			for i := range 18000 {
				value := float64(i)
				if mode.floating {
					value = math.Sin(float64(i)/73) + .123
				}
				history = append(history, store.Point{Timestamp: start + int64(i)*60, Value: value})
			}
			if err := source.UpdateMany(ctx, cfg.Name, history); err != nil {
				t.Fatal(err)
			}
			if mode.sealed {
				for range 8 {
					if err := source.RunColdPack(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, s := range []*store.Store{target, reference} {
				var recent []store.Point
				for i := 18000; i < 18100; i++ {
					recent = append(recent, store.Point{Timestamp: start + int64(i)*60, Value: float64(-i)})
				}
				if err := s.UpdateMany(ctx, cfg.Name, recent); err != nil {
					t.Fatal(err)
				}
			}
			compareNativeCombine(t, source, target, reference, cfg.Name)
			if stats := target.NativeStats(); stats.CombineCopiedBlocks == 0 || stats.CombineMaterializedArchives != 0 {
				t.Fatalf("did not splice: %+v", stats)
			}
			records, err := target.ExportTransfer(ctx, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			kv := mapContinuationIO{}
			for _, item := range records[0].Keys {
				kv[string(item.Key)] = item.Value
			}
			fs := newContinuationFiles(kv, true).withClock(clock)
			w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
			if err != nil {
				t.Fatal(err)
			}
			if w.IsCompressed() {
				t.Fatal("authoritative output uses compressed propagation buffers")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := target.NativeStats(); stats.CombineCopiedBlocks == 0 || stats.CombineMaterializedArchives != 0 {
				t.Fatalf("did not splice: %+v", stats)
			}
			for _, point := range []store.Point{{Timestamp: start + 18101*60, Value: 77}, {Timestamp: start + 100*60, Value: 999}, {Timestamp: start + 18102*60, Value: math.Copysign(0, -1)}} {
				before := map[whisper.DirectLateResult]uint64{}
				for _, k := range whisper.DirectLateResults() {
					before[k] = whisper.DirectLateWrites(k)
				}
				for _, s := range []*store.Store{target, reference} {
					if err := s.UpdateMany(ctx, cfg.Name, []store.Point{point}); err != nil {
						t.Fatal(err)
					}
				}
				want, err := reference.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := target.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(want, got); err != nil {
					for _, k := range whisper.DirectLateResults() {
						t.Logf("direct %s: %d", k, whisper.DirectLateWrites(k)-before[k])
					}
					for ai, a := range want.Archives {
						for i, p := range a.Points {
							if i < len(got.Archives[ai].Points) && p != got.Archives[ai].Points[i] {
								t.Logf("archive %d point %d want=%+v got=%+v", ai, i, p, got.Archives[ai].Points[i])
								break
							}
						}
					}
					t.Fatal(fmt.Sprintf("after %v: %v", point, err))
				}
				if _, err := target.VerifyTransferFetch(ctx, got); err != nil {
					t.Fatal(err)
				}
			}
			// A following donor must combine with the already referenced target,
			// including its foreground replacements and retired shared payloads.
			records, err = source.ExportTransfer(ctx, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reference.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			var wire bytes.Buffer
			if err := pin.Write(ctx, []string{cfg.Name}, &wire); err != nil {
				t.Fatal(err)
			}
			if err := pin.Close(); err != nil {
				t.Fatal(err)
			}
			request := store.NativeRange{Operation: strings.Repeat("4", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{cfg.Name}, Combine: true}
			if _, err := target.NativeImport(ctx, request, &wire); err != nil {
				t.Fatal(err)
			}
			want, err := reference.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal("following donor", err)
			}
			if _, err := target.VerifyTransferFetch(ctx, got); err != nil {
				t.Fatal(err)
			}
		})
	}
}
