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

type windowCountingIO struct {
	mapContinuationIO
	revision uint64
	groups   int
}

func (m *windowCountingIO) ContinuationRevision() uint64 { return m.revision }
func (m *windowCountingIO) Get(k []byte) ([]byte, error) {
	if len(k) == 20 && k[0] == 'f' && k[9] == 'p' {
		m.groups++
	}
	return m.mapContinuationIO.Get(k)
}

func TestNativeWindowSeekDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(100000, 0) }
	codec := &ContinuationCodec{}
	rng := rand.New(rand.NewPCG(19, 42))
	for _, count := range []int{127, 129, 257, 4096} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			cfg := store.MetricConfig{Name: "seek", Retentions: []store.Retention{{Step: 10, Count: count}}, AggregationMethod: store.Sum}
			snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg, Revision: 7}, Archives: []store.Archive{{Retention: cfg.Retentions[0]}}}
			for i := range count {
				if i != 0 && rng.IntN(5) == 0 {
					continue
				}
				value := float64(rng.IntN(10))
				switch rng.IntN(16) {
				case 0:
					value = math.Copysign(0, -1)
				case 1:
					value = math.Inf(1)
				case 2:
					value = math.Float64frombits(0x7ff8000000000042)
				}
				snap.Archives[0].Points = append(snap.Archives[0].Points, store.Point{Timestamp: 20000 + int64(i*10), Value: value})
			}
			kv := &windowCountingIO{mapContinuationIO: mapContinuationIO{}, revision: 7}
			if err := codec.RestoreArchives(ctx, kv, snap, clock); err != nil {
				t.Fatal(err)
			}
			legacy := mapContinuationIO{}
			for k, v := range kv.mapContinuationIO {
				if len(k) != 10 || k[9] != 'w' {
					legacy[k] = v
				}
			}
			for range 60 {
				from := int64(20000 + rng.IntN(count*10))
				window := store.NativeWindow{From: from, Until: from + int64(1+rng.IntN(900))}
				want, ws, we := codec.NativeWindowSnapshot(ctx, legacy, snap.Metadata, clock, window)
				got, gs, ge := codec.NativeWindowSnapshot(ctx, kv, snap.Metadata, clock, window)
				if !errors.Is(ge, we) || !reflect.DeepEqual(ws, gs) {
					t.Fatalf("window %+v: errors %v/%v, skips %v/%v", window, we, ge, ws, gs)
				}
				if we == nil {
					if err := store.EqualSnapshots(want, got); err != nil {
						t.Fatal(window, err)
					}
				}
			}
			kv.groups = 0
			window := store.NativeWindow{From: 20000 + int64(count/2*10), Until: 20020 + int64(count/2*10)}
			if _, _, handled, err := codec.seekNativeWindow(ctx, kv, snap.Metadata, clock, window); err != nil || !handled {
				t.Fatal(handled, err)
			}
			if kv.groups > 2 {
				t.Fatalf("two-point window read %d groups", kv.groups)
			}
			kv.revision++ // An older writer can change data without updating our index.
			if _, _, handled, err := codec.seekNativeWindow(ctx, kv, snap.Metadata, clock, window); err != nil || handled {
				t.Fatal("stale index accepted", handled, err)
			}
		})
	}
}

func TestNativeWindowSeekReferencedGroups(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(10000, 0) }
	codec := &ContinuationCodec{}
	cfg := store.MetricConfig{Name: "references", Retentions: []store.Retention{{Step: 10, Count: 512}}, AggregationMethod: store.Sum}
	m := store.Metadata{MetricConfig: cfg}
	makeSide := func(parity int) mapContinuationIO {
		kv := mapContinuationIO{}
		snap := store.Snapshot{Metadata: m, Archives: []store.Archive{{Retention: cfg.Retentions[0]}}}
		for i := parity; i < 512; i += 2 {
			snap.Archives[0].Points = append(snap.Archives[0].Points, store.Point{Timestamp: 4000 + int64(i*10), Value: float64(i)})
		}
		if err := codec.RestoreArchives(ctx, kv, snap, clock); err != nil {
			t.Fatal(err)
		}
		return kv
	}
	keys, _, ok, err := codec.CombineNative(ctx, makeSide(0), makeSide(1), m, clock)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	kv := mapContinuationIO{}
	references := 0
	for _, entry := range keys {
		kv[string(entry.Key)] = entry.Value
		if len(entry.Key) == 20 && entry.Key[9] == 'a' {
			references++
		}
	}
	if references == 0 {
		t.Fatal("fixture has no referenced groups")
	}
	before, err := codec.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	window := store.NativeWindow{From: 5500, Until: 5600}
	source, _, err := store.SelectNativeWindow(before, window, clock().Unix())
	if err != nil {
		t.Fatal(err)
	}
	patch, _, handled, err := codec.PlanNativeWindowPatch(ctx, kv, source, clock, window)
	if err != nil || !handled {
		t.Fatal(handled, err)
	}
	for _, entry := range patch {
		if entry.Value == nil {
			delete(kv, string(entry.Key))
		} else {
			kv[string(entry.Key)] = entry.Value
		}
	}
	after, err := codec.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
	remaining := 0
	for key := range kv {
		if len(key) == 20 && key[9] == 'a' {
			remaining++
		}
	}
	if remaining >= references {
		t.Fatal("window references were not retired", remaining, references)
	}
	indexed, _, err := codec.NativeWindowSnapshot(ctx, kv, m, clock, window)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(source, indexed); err != nil {
		t.Fatal(err)
	}
}

func TestNativeWindowSeekCompressedIndex(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(30000, 0) }
	codec := &ContinuationCodec{}
	source := combineMemoryStore(t, clock, false)
	cfg := store.MetricConfig{Name: "compressed.seek", Retentions: []store.Retention{{Step: 1, Count: 20000}}, AggregationMethod: store.Sum}
	if _, err := source.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	points := make([]store.Point, 19000)
	for i := range points {
		points[i] = store.Point{Timestamp: 11000 + int64(i), Value: float64(i % 71)}
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
	snapshot, err := source.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	window := store.NativeWindow{From: 20000, Until: 20060}
	want, ws, err := store.SelectNativeWindow(snapshot, window, clock().Unix())
	if err != nil {
		t.Fatal(err)
	}
	got, gs, err := codec.NativeWindowSnapshot(ctx, kv, snapshot.Metadata, clock, window)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ws, gs) {
		t.Fatal(ws, gs)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	full, ok, err := codec.nativeFragments(ctx, kv, snapshot.Metadata, clock, 0)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	selected, ok, err := codec.nativeFragments(ctx, kv, snapshot.Metadata, clock, 0, window)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if len(full[0]) < 2 || len(selected[0]) >= len(full[0]) {
		t.Fatal("block index did not prune history", len(full[0]), len(selected[0]))
	}
}
