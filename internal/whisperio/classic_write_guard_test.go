package whisperio

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

var classicTestClock = func() time.Time { return time.Unix(1000000, 0) }

func mixedClassicFixture(t testing.TB) (mapContinuationIO, store.Snapshot) {
	t.Helper()
	kv, snapshot := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999780, Value: 6}, {Timestamp: 999990, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 999870, Value: math.Copysign(0, -1)}})
	// Pre-1cece5866 RestoreArchives used absolute slots. The first ordinary
	// write fills empty slot zero, creating the mixed-phase loss30 image.
	if err := (&ContinuationCodec{}).Update(context.Background(), kv, snapshot.Metadata, []store.Point{{Timestamp: 999800, Value: 1}}, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	return kv, snapshot
}

func requireClassicPoints(t testing.TB, codec *ContinuationCodec, kv store.ContinuationIO, m store.Metadata, want []store.Point) {
	t.Helper()
	got, err := codec.Snapshot(context.Background(), kv, m, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range want {
		found := false
		for _, q := range got.Archives[0].Points {
			if p.Timestamp == q.Timestamp && math.Float64bits(p.Value) == math.Float64bits(q.Value) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing point %+v (%x), got %+v", p, math.Float64bits(p.Value), got.Archives[0].Points)
		}
	}
}

func classicTransfer(kv mapContinuationIO, snapshot store.Snapshot) store.TransferMetric {
	record := store.TransferMetric{Config: snapshot.Metadata.MetricConfig, Version: store.TransferVersion{Name: snapshot.Metadata.Name}, Continuation: true}
	for key, value := range kv {
		record.Keys = append(record.Keys, store.RelayoutKV{Key: []byte(key), Value: value})
	}
	slices.SortFunc(record.Keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	return record
}

func TestClassicWriteReanchorsStoreTransaction(t *testing.T) {
	for _, cacheSize := range []int64{0, 1 << 20} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%d/batch=%t", cacheSize, batch), func(t *testing.T) {
				ctx := context.Background()
				codec := &ContinuationCodec{}
				kv, snapshot := mixedClassicFixture(t)
				opts := coldOptions(codec)
				opts.Now, opts.MetadataCacheSize = classicTestClock, cacheSize
				db, err := store.Open(t.TempDir(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				db.StopBackground()
				record := classicTransfer(kv, snapshot)
				other := record
				other.Config.Name, other.Version.Name = "unrelated", "unrelated"
				if _, err := db.ImportTransfer(ctx, []store.TransferMetric{record, other}); err != nil {
					t.Fatal(err)
				}
				beforeOther, err := db.ExportTransfer(ctx, []string{"unrelated"})
				if err != nil {
					t.Fatal(err)
				}
				name := snapshot.Metadata.Name
				first := []store.Point{{Timestamp: 999810, Value: 7}}
				conflict := []store.Point{{Timestamp: 999980, Value: 9}}
				before := ClassicReanchors()
				if batch {
					err = db.UpdateBatch(ctx, []store.MetricUpdate{{Name: name, Points: first}, {Name: name, Points: conflict}}, nil)
				} else {
					err = db.UpdateMany(ctx, name, append(first, conflict...))
				}
				if err != nil {
					t.Fatal(err)
				}
				after := ClassicReanchors()
				if after.ReanchoredArchives-before.ReanchoredArchives != 1 {
					t.Fatalf("repair count: %+v -> %+v", before, after)
				}
				exported, err := db.ExportTransfer(ctx, []string{name})
				if err != nil {
					t.Fatal(err)
				}
				gotKV := mapContinuationIO{}
				for _, pair := range exported[0].Keys {
					gotKV[string(pair.Key)] = pair.Value
				}
				requireClassicPoints(t, codec, gotKV, snapshot.Metadata, append(append(snapshot.Archives[0].Points, first...), conflict...))
				afterOther, err := db.ExportTransfer(ctx, []string{"unrelated"})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(beforeOther, afterOther) {
					t.Fatal("unrelated name changed")
				}
				// Subsequent corrections exercise the installed metadata cache.
				if err := db.UpdateMany(ctx, name, conflict); err != nil {
					t.Fatal(err)
				}
				if ClassicReanchors() != after {
					t.Fatal("repaired archive re-anchored again")
				}
			})
		}
	}
}

func TestClassicLoss30Reanchor(t *testing.T) {
	codec := &ContinuationCodec{DirectLateWrites: true}
	kv, snapshot := mixedClassicFixture(t)
	before := ClassicReanchors()
	input := []store.Point{{Timestamp: 999980, Value: 9}}
	if err := codec.Update(context.Background(), kv, snapshot.Metadata, input, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	requireClassicPoints(t, codec, kv, snapshot.Metadata, append(snapshot.Archives[0].Points, input...))
	plan, err := codec.PlanArchiveAnchors(context.Background(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Archives[0].Misplaced != 0 || plan.Archives[0].Blocked != "" {
		t.Fatalf("not consistent: %+v", plan)
	}
	series, err := codec.Fetch(context.Background(), kv, snapshot.Metadata, 999770, 1000000, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	if series.Values[(999780-int64(series.FromTime))/10] != 6 || series.Values[(999980-int64(series.FromTime))/10] != 9 {
		t.Fatalf("classic read lost old or new point: %+v", series)
	}
	if ClassicReanchors().ReanchoredArchives-before.ReanchoredArchives != 1 {
		t.Fatal("wrong counter")
	}
	state := ClassicReanchors()
	if err := codec.Update(context.Background(), kv, snapshot.Metadata, input, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	if ClassicReanchors() != state {
		t.Fatal("not idempotent")
	}
}

func TestClassicReanchorAliases(t *testing.T) {
	kv, snapshot := mixedClassicFixture(t)
	for key, value := range kv {
		if len(key) != 20 || key[9] != 'p' {
			continue
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			t.Fatal(err)
		}
		block.Present[0] |= 1 << 5
		block.Points[5] = store.Point{Timestamp: 999180, Value: 2}
		kv[key] = store.EncodePointBlock(&block)
	}
	before := ClassicReanchors()
	if err := (&ContinuationCodec{}).Update(context.Background(), kv, snapshot.Metadata, []store.Point{{Timestamp: 999980, Value: 9}}, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	if ClassicReanchors().AliasDrops-before.AliasDrops != 1 {
		t.Fatal("wrong alias drop counter")
	}
	requireClassicPoints(t, &ContinuationCodec{}, kv, snapshot.Metadata, snapshot.Archives[0].Points)
	got, err := (&ContinuationCodec{}).Snapshot(context.Background(), kv, snapshot.Metadata, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range got.Archives[0].Points {
		if p.Timestamp == 999180 {
			t.Fatal("older alias survived")
		}
	}
}

func TestClassicConcurrentReanchor(t *testing.T) {
	for _, cacheSize := range []int64{0, 1 << 20} {
		t.Run(fmt.Sprint(cacheSize), func(t *testing.T) {
			codec := &ContinuationCodec{}
			opts := coldOptions(codec)
			opts.Now, opts.MetadataCacheSize = classicTestClock, cacheSize
			db, err := store.Open(t.TempDir(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.StopBackground()
			kv, snap := mixedClassicFixture(t)
			if _, err := db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(kv, snap)}); err != nil {
				t.Fatal(err)
			}
			before := ClassicReanchors()
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := db.UpdateMany(context.Background(), snap.Metadata.Name, []store.Point{{Timestamp: 999980, Value: 9}}); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			if ClassicReanchors().ReanchoredArchives-before.ReanchoredArchives != 1 {
				t.Fatal("concurrent writers repeated repair")
			}
		})
	}
}

func TestClassicUpdateAllowsCorrectionAndRingWrap(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{}
	kv, snapshot := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999600, Value: 1}, {Timestamp: 999780, Value: 2}})
	now := int64(1000000)
	clock := func() time.Time { return time.Unix(now, 0) }
	for _, point := range []store.Point{{Timestamp: 999780, Value: 4}, {Timestamp: 1000200, Value: 5}} {
		if point.Timestamp > now {
			now = 1000400
		}
		if err := codec.Update(ctx, kv, snapshot.Metadata, []store.Point{point}, -1, clock, nil); err != nil {
			t.Fatal(err)
		}
	}
	got, err := codec.Snapshot(ctx, kv, snapshot.Metadata, clock)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]float64{999780: 4, 1000200: 5}
	for _, p := range got.Archives[0].Points {
		if v, ok := want[p.Timestamp]; ok && v == p.Value {
			delete(want, p.Timestamp)
		}
	}
	if len(want) != 0 {
		t.Fatalf("lost legitimate updates: %v", want)
	}
}
