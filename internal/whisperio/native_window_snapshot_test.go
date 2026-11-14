package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestBoundedWindowSnapshotDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000000, 0) }
	codec := &ContinuationCodec{}
	rng := rand.New(rand.NewPCG(719, 81))
	for _, method := range []store.AggregationMethod{store.Sum, store.Average, store.Min, store.Max, store.Last} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			source := combineMemoryStore(t, clock, false)
			cfg := store.MetricConfig{Name: "bounded", Retentions: []store.Retention{{Step: 1, Count: 20000}, {Step: 60, Count: 3000}, {Step: 300, Count: 1000}}, AggregationMethod: method, XFilesFactor: 0.5}
			if _, err := source.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			var points []store.Point
			for i := 0; i < 19000; i++ {
				if rng.IntN(11) == 0 {
					continue
				}
				value := float64(i % 73)
				switch rng.IntN(50) {
				case 0:
					value = math.Copysign(0, -1)
				case 1:
					value = math.Inf(1)
				case 2:
					value = math.Inf(-1)
				case 3:
					value = math.Float64frombits(1)
				case 4:
					value = math.Float64frombits(0x7ff8000000000042)
				}
				points = append(points, store.Point{Timestamp: 980000 + int64(i), Value: value})
			}
			if err := source.UpdateMany(ctx, cfg.Name, points); err != nil {
				t.Fatal(err)
			}
			records, err := source.ExportTransfer(ctx, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			kv := mapContinuationIO{}
			for _, entry := range records[0].Keys {
				kv[string(entry.Key)] = entry.Value
			}
			full, err := source.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 100; i++ {
				from := int64(975000 + rng.IntN(30000))
				window := store.NativeWindow{From: from, Until: from + int64(1+rng.IntN(3000))}
				want, ws, we := store.SelectNativeWindow(full, window, clock().Unix())
				bounded, handled, err := codec.boundedWindowSnapshot(ctx, kv, full.Metadata, clock, window)
				if !handled {
					t.Fatal(handled, err)
				}
				if err != nil {
					if errors.Is(err, store.ErrConflict) && errors.Is(we, store.ErrConflict) {
						continue
					}
					t.Fatal(err)
				}
				got, gs, ge := store.SelectNativeWindow(bounded, window, clock().Unix())
				if (ge == nil) != (we == nil) || errors.Is(ge, store.ErrConflict) != errors.Is(we, store.ErrConflict) || !reflect.DeepEqual(ws, gs) {
					t.Fatalf("%+v errors %v/%v skips %v/%v", window, we, ge, ws, gs)
				}
				if we == nil {
					if err := store.EqualSnapshots(want, got); err != nil {
						t.Fatal(window, err)
					}
				}
			}
		})
	}
}

type exportReadIO struct {
	mapContinuationIO
	bytes int
}

func (kv *exportReadIO) Get(key []byte) ([]byte, error) {
	value, err := kv.mapContinuationIO.Get(key)
	kv.bytes += len(value)
	return value, err
}
func (kv *exportReadIO) Visit(prefix []byte, fn func([]byte, []byte) error) error {
	return kv.mapContinuationIO.Visit(prefix, func(k, v []byte) error { kv.bytes += len(v); return fn(k, v) })
}

func TestBoundedWindowSnapshotReads(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000000, 0) }
	source := combineMemoryStore(t, clock, false)
	cfg := store.MetricConfig{Name: "history", Retentions: []store.Retention{{Step: 1, Count: 200000}}, AggregationMethod: store.Sum}
	if _, err := source.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	points := make([]store.Point, 190000)
	for i := range points {
		points[i] = store.Point{Timestamp: 800000 + int64(i), Value: float64(i % 71)}
	}
	if err := source.UpdateMany(ctx, cfg.Name, points); err != nil {
		t.Fatal(err)
	}
	records, err := source.ExportTransfer(ctx, []string{cfg.Name})
	if err != nil {
		t.Fatal(err)
	}
	kv := &exportReadIO{mapContinuationIO: mapContinuationIO{}}
	for _, entry := range records[0].Keys {
		kv.mapContinuationIO[string(entry.Key)] = entry.Value
	}
	codec := &ContinuationCodec{}
	original, err := source.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := original.Metadata
	full, err := codec.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	fullBytes := kv.bytes
	kv.bytes = 0
	window := store.NativeWindow{From: 900000, Until: 900060}
	bounded, handled, err := codec.boundedWindowSnapshot(ctx, kv, m, clock, window)
	if err != nil || !handled {
		t.Fatal(handled, err)
	}
	boundedBytes := kv.bytes
	want, ws, we := store.SelectNativeWindow(full, window, clock().Unix())
	got, gs, ge := store.SelectNativeWindow(bounded, window, clock().Unix())
	if we != nil || ge != nil || !reflect.DeepEqual(ws, gs) {
		t.Fatal(we, ge, ws, gs)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	t.Logf("full bytes=%d bounded bytes=%d", fullBytes, boundedBytes)
	if boundedBytes*2 >= fullBytes {
		t.Fatal("bounded export did not prune physical history")
	}
}
