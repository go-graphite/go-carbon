package whisperio

import (
	"bytes"
	"context"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeColdBlockDifferential(t *testing.T) {
	for _, floating := range []bool{false, true} {
		t.Run(map[bool]string{false: "v9", true: "v11"}[floating], func(t *testing.T) {
			ctx := context.Background()
			open := func() *store.Store {
				opts := coldOptions(&ContinuationCodec{})
				opts.ColdPack = store.ColdPackOptions{Mode: "exact", FloatCodec: floating, WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}
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
			var want store.Snapshot
			if !floating {
				want = coldFixture(t, source)
			} else {
				cfg := store.MetricConfig{Name: "float.metric", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
				if _, err := source.Create(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				random := rand.New(rand.NewPCG(42, 17))
				points := make([]store.Point, 1800)
				for i := range points {
					points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: float64(random.IntN(16)) / 3}
				}
				if err := source.UpdateMany(ctx, cfg.Name, points); err != nil {
					t.Fatal(err)
				}
				var err error
				want, err = source.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
			}
			floatSweep(t, source, "exact", false)
			if floating {
				floatSweep(t, source, "float-repack", true)
				old, new := floatGroups(t, source, want.Metadata.Name)
				t.Logf("groups old/new %d/%d cold=%+v", old, new, source.ColdPackStats())
			}
			if source.ColdPackStats().Blocks == 0 {
				t.Fatal("fixture did not seal native blocks")
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			names := []string{want.Metadata.Name}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reference.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			var wire bytes.Buffer
			if err := pin.Write(ctx, names, &wire); err != nil {
				t.Fatal(err)
			}
			request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: names}
			if _, err := target.NativeImport(ctx, request, &wire); err != nil {
				t.Fatal(err)
			}
			a, err := reference.Snapshot(ctx, names[0])
			if err != nil {
				t.Fatal(err)
			}
			b, err := target.Snapshot(ctx, names[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(a, b); err != nil {
				t.Fatal(err)
			}
			t.Logf("source=%d reference=%d target=%d", source.TransferFormat(), reference.TransferFormat(), target.TransferFormat())
			if floating && target.TransferFormat() != 11 {
				t.Fatal("float format not durably fenced", target.TransferFormat())
			}
		})
	}
}
