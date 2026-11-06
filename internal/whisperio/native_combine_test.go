package whisperio

import (
	"bytes"
	"context"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func combineMemoryStore(t *testing.T, clock func() time.Time, floating bool) *store.Store {
	t.Helper()
	opts := coldOptions(&ContinuationCodec{})
	opts.Now = clock
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

func compareNativeCombine(t *testing.T, source, target, reference *store.Store, name string) {
	t.Helper()
	ctx := context.Background()
	before, err := source.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.ImportTransfer(ctx, before); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, []string{name}, &wire); err != nil {
		t.Fatal(err)
	}
	r := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{name}, Combine: true}
	if _, err := target.NativeImport(ctx, r, &wire); err != nil {
		t.Fatal(err)
	}
	a, err := reference.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	b, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(a, b); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCombineColdBlocksDifferential(t *testing.T) {
	for _, floating := range []bool{false, true} {
		t.Run(map[bool]string{false: "v9", true: "v11"}[floating], func(t *testing.T) {
			ctx := context.Background()
			clock := func() time.Time { return time.Unix(coldBase+2000*60, 0) }
			source, target, reference := combineMemoryStore(t, clock, floating), combineMemoryStore(t, clock, floating), combineMemoryStore(t, clock, floating)
			cfg := store.MetricConfig{Name: "combine.sealed", Retentions: []store.Retention{{Step: 60, Count: 10080}, {Step: 3600, Count: 8760}}, AggregationMethod: store.Average}
			if _, err := source.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			random := rand.New(rand.NewPCG(42, 17))
			points := make([]store.Point, 1800)
			for i := range points {
				value := float64(i % 71)
				if floating {
					value = float64(random.IntN(16)) / 3
				}
				points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: value}
			}
			if err := source.UpdateMany(ctx, cfg.Name, points); err != nil {
				t.Fatal(err)
			}
			floatSweep(t, source, "exact", false)
			if floating {
				floatSweep(t, source, "float-repack", true)
				_, groups := floatGroups(t, source, cfg.Name)
				if groups == 0 {
					t.Fatal("v11 fixture has no float groups")
				}
			}
			if source.ColdPackStats().Blocks == 0 {
				t.Fatal("fixture has no sealed blocks")
			}
			records, err := source.ExportTransfer(ctx, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range []*store.Store{target, reference} {
				if _, err := s.ImportTransfer(ctx, records); err != nil {
					t.Fatal(err)
				}
				if err := s.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: points[900].Timestamp, Value: 777}, {Timestamp: points[1799].Timestamp + 60, Value: 99}}); err != nil {
					t.Fatal(err)
				}
			}
			compareNativeCombine(t, source, target, reference, cfg.Name)
		})
	}
}

func TestNativeCombineOffGridDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1792400820+3*1440*60, 0) }
	path := filepath.Join(t.TempDir(), "offgrid.wsp")
	points := unalignedCWhisper(t, path, clock)
	cfg := store.MetricConfig{Name: "combine.offgrid", Retentions: []store.Retention{{Step: 60, Count: 14 * 1440}, {Step: 1800, Count: 2 * 365 * 48}}, AggregationMethod: store.Sum}
	m := store.Metadata{MetricConfig: cfg}
	kv := make(mapContinuationIO)
	codec := &ContinuationCodec{}
	if err := codec.Import(ctx, kv, path, m, clock); err != nil {
		t.Fatal(err)
	}
	raw := store.TransferMetric{Config: cfg, Version: store.TransferVersion{Name: cfg.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
	if err := kv.Visit(nil, func(k, v []byte) error { raw.Keys = append(raw.Keys, store.RelayoutKV{Key: k, Value: v}); return nil }); err != nil {
		t.Fatal(err)
	}
	source, target, reference := combineMemoryStore(t, clock, false), combineMemoryStore(t, clock, false), combineMemoryStore(t, clock, false)
	if _, err := source.ImportTransfer(ctx, []store.TransferMetric{raw}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*store.Store{target, reference} {
		if _, err := s.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		p := points[len(points)/2]
		if err := s.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: int64(p.Time / 60 * 60), Value: 77}}); err != nil {
			t.Fatal(err)
		}
	}
	compareNativeCombine(t, source, target, reference, cfg.Name)
}

func TestNativeCombineClassicUsesEncodedBlocks(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000000, 0) }
	cfg := store.MetricConfig{Name: "combine.classic", Retentions: []store.Retention{{Step: 10, Count: 600}}, AggregationMethod: store.Sum}
	codec := &ContinuationCodec{}
	makeRecord := func(point store.Point) store.TransferMetric {
		kv := make(mapContinuationIO)
		snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{point}}}}
		if err := codec.RestoreArchives(ctx, kv, snap, clock); err != nil {
			t.Fatal(err)
		}
		groups, ok, err := codec.NativeClassicBlocks(ctx, kv, snap.Metadata, clock)
		if err != nil || !ok || len(groups) != 1 {
			t.Fatalf("classic block path rejected: ok=%v groups=%d err=%v namespace=%v", ok, len(groups), err, kv)
		}
		r := store.TransferMetric{Config: cfg, Version: store.TransferVersion{Name: cfg.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
		if err := kv.Visit(nil, func(k, v []byte) error { r.Keys = append(r.Keys, store.RelayoutKV{Key: k, Value: v}); return nil }); err != nil {
			t.Fatal(err)
		}
		return r
	}
	left, right := makeRecord(store.Point{Timestamp: 996000, Value: 1}), makeRecord(store.Point{Timestamp: 997280, Value: 2})
	source, target, reference := combineMemoryStore(t, clock, false), combineMemoryStore(t, clock, false), combineMemoryStore(t, clock, false)
	if _, err := source.ImportTransfer(ctx, []store.TransferMetric{left}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*store.Store{target, reference} {
		if _, err := s.ImportTransfer(ctx, []store.TransferMetric{right}); err != nil {
			t.Fatal(err)
		}
	}
	compareNativeCombine(t, source, target, reference, cfg.Name)
	stats := target.NativeStats()
	if stats.CombineMaterializedArchives != 0 || stats.CombineCopiedBlocks != 2 || stats.CombinePointBlocks != 0 {
		t.Fatal("classic combine materialized disjoint archives", stats)
	}
}
