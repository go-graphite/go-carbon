package v2convert

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func nativeTestBaseline() NativeBaseline {
	return NativeBaseline{Snapshot: v2part.Digest{1}, Source: v2part.Provenance{Incarnation: v2part.PartID{2}, Stream: "certified-native-baseline"}, Group: "test", Part: v2part.PartID{3}, Order: 7}
}

func nativeTestSnapshot(name string) chunkstore.Snapshot {
	retentions := []chunkstore.Retention{{Step: 60, Count: 32}, {Step: 600, Count: 64}}
	return chunkstore.Snapshot{Metadata: chunkstore.Metadata{MetricConfig: chunkstore.MetricConfig{Name: name, Retentions: retentions, AggregationMethod: chunkstore.Last, XFilesFactor: .5}, ID: 11, Generation: 8, Revision: 27, LastUpdate: time.Unix(1794700800, 42)}, Archives: []chunkstore.Archive{
		{Retention: retentions[0], Points: []chunkstore.Point{{Timestamp: 1794700800, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 1794700740, Value: math.Copysign(0, -1)}, {Timestamp: 1794700680, Value: math.Float64frombits(1)}}},
		{Retention: retentions[1], Points: []chunkstore.Point{{Timestamp: 1794699600, Value: 123.5}, {Timestamp: 1794700200, Value: -97.25}}},
	}}
}

func TestNativeRecordsPreserveArchivesAndAuthority(t *testing.T) {
	snapshot := nativeTestSnapshot("native.test")
	baseline := nativeTestBaseline()
	rows, err := NativeRecords(snapshot, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 6 {
		t.Fatalf("got %d rows", len(rows))
	}
	found := map[[2]int64]uint64{}
	for _, row := range rows {
		key, err := v2part.Decode(row.Key)
		if err != nil {
			t.Fatal(err)
		}
		if key.Part != baseline.Part || key.Metric != v2part.ID(snapshot.Metadata.Name) || key.Group != baseline.Group {
			t.Fatal("baseline identity changed")
		}
		if key.Archive == v2part.MetadataArchive {
			meta, err := v2core.DecodeRollupMetadata(key.Metric, key.Group, row.Value)
			if err != nil {
				t.Fatal(err)
			}
			if meta.ID != snapshot.Metadata.ID || meta.Revision != snapshot.Metadata.Revision || meta.LastUpdate != snapshot.Metadata.LastUpdate.UnixNano() || meta.Frontier.Order != baseline.Order || meta.Frontier.MaxTime != 1794700800 {
				t.Fatalf("wrong metadata %+v", meta)
			}
			continue
		}
		r, err := sealrollup.DecodeRecord(int(key.Archive), key.BlockStart, row.Value)
		if err != nil {
			t.Fatal(err)
		}
		if r.Order != baseline.Order || r.Correction {
			t.Fatal("invented original point order or correction")
		}
		found[[2]int64{int64(key.Archive), r.Sample.Time}] = r.Sample.Bits
	}
	for archive, a := range snapshot.Archives {
		for _, point := range a.Points {
			if bits, ok := found[[2]int64{int64(archive), point.Timestamp}]; !ok || bits != math.Float64bits(point.Value) {
				t.Fatal("lost or reduced original archive point")
			}
		}
	}
	mutations := []func(*chunkstore.Snapshot){
		func(s *chunkstore.Snapshot) { s.Metadata.ID = 0 },
		func(s *chunkstore.Snapshot) { s.Metadata.Revision = 0 },
		func(s *chunkstore.Snapshot) { s.Archives[0].Retention.Step++ },
		func(s *chunkstore.Snapshot) { s.Archives[0].Points[0].Timestamp++ },
		func(s *chunkstore.Snapshot) {
			s.Archives[0].Points = append(s.Archives[0].Points, chunkstore.Point{Timestamp: 1794700800 - 32*60, Value: 17})
		},
	}
	for i, mutate := range mutations {
		candidate := nativeTestSnapshot("native.test")
		mutate(&candidate)
		if _, err := NativeRecords(candidate, baseline); err == nil {
			t.Fatalf("mutation %d admitted", i)
		}
	}
	baseline.Snapshot = v2part.Digest{}
	if _, err := NativeRecords(snapshot, baseline); err == nil {
		t.Fatal("missing snapshot authority admitted")
	}
}

type nativeTestReader map[string]chunkstore.Snapshot

func (r nativeTestReader) Snapshot(ctx context.Context, name string) (chunkstore.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return chunkstore.Snapshot{}, err
	}
	return r[name], nil
}

func TestNativeMaterializationParallelDeterministicAndBounded(t *testing.T) {
	reader := nativeTestReader{"one": nativeTestSnapshot("one"), "two": nativeTestSnapshot("two")}
	baseline := nativeTestBaseline()
	one, err := MaterializeNativeWorkers(context.Background(), reader, []string{"two", "one"}, baseline, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{16, 32} {
		many, err := MaterializeNativeWorkers(context.Background(), reader, []string{"one", "two"}, baseline, 1<<20, workers)
		if err != nil || !reflect.DeepEqual(one, many) {
			t.Fatalf("workers%d differs: %v", workers, err)
		}
		first, second := t.TempDir(), t.TempDir()
		writer := CoreWriter{Source: baseline.Source}
		a, err := writer.Write(context.Background(), first, one)
		if err != nil {
			t.Fatal(err)
		}
		b, err := writer.Write(context.Background(), second, many)
		if err != nil || !slices.Equal(a, b) {
			t.Fatal("worker count changed output descriptors", err)
		}
		for _, path := range a {
			x, _ := os.ReadFile(filepath.Join(first, path))
			y, _ := os.ReadFile(filepath.Join(second, path))
			if !bytes.Equal(x, y) {
				t.Fatal("worker count changed output bytes")
			}
		}
	}
	if _, err := MaterializeNativeWorkers(context.Background(), reader, []string{"one", "two"}, baseline, 50, 32); err == nil {
		t.Fatal("memory bound bypassed")
	}
	if _, err := MaterializeNativeWorkers(context.Background(), reader, []string{"one", "one"}, baseline, 1<<20, 16); err == nil {
		t.Fatal("duplicate name admitted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := MaterializeNativeWorkers(ctx, reader, []string{"one"}, baseline, 1<<20, 16); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func testNativePlan(t *testing.T, directory string, names []string) NativeCheckpointPlan {
	t.Helper()
	plan := NativeCheckpointPlan{Version: 1, Baseline: nativeTestBaseline(), Now: 1794700800, Names: names}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" {
			continue
		}
		sum, size, err := FileSum(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		plan.Files = append(plan.Files, File{Name: entry.Name(), SHA256: sum, Bytes: size})
	}
	plan.Baseline.Snapshot, err = plan.InventoryDigest()
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestNativeCheckpointResolvedVisibilityAndExactReadback(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "continuation"}[continuation], func(t *testing.T) {
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "native")
			options := chunkstore.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(1794700800, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			if continuation {
				options.Continuation = &whisperio.ContinuationCodec{}
				options.TimeBuckets = true
			}
			store, err := chunkstore.Open(dir, options)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			for _, name := range []string{"kept", "deleted"} {
				config := nativeTestSnapshot(name).Metadata.MetricConfig
				if _, err := store.Create(ctx, config); err != nil {
					t.Fatal(err)
				}
				if err := store.UpdateMany(ctx, name, []chunkstore.Point{{Timestamp: 1794700740, Value: 13}, {Timestamp: 1794700800, Value: -0.0}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(ctx, "deleted"); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateMany(ctx, "kept", []chunkstore.Point{{Timestamp: 1794700740, Value: math.Copysign(0, -1)}}); err != nil {
				t.Fatal(err)
			}
			checkpoint := filepath.Join(t.TempDir(), "checkpoint")
			if err := store.Checkpoint(checkpoint); err != nil {
				t.Fatal(err)
			}
			plan := testNativePlan(t, checkpoint, []string{"kept"})
			reader, err := OpenNativeCheckpoint(ctx, checkpoint, plan)
			if err != nil {
				t.Fatal(err)
			}
			page, err := reader.ListCatalogPage(ctx, "", "", 100)
			if err != nil || len(page) != 1 || page[0].Name != "kept" {
				t.Fatal("deleted metric resurrected", page, err)
			}
			want, err := store.Snapshot(ctx, "kept")
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := reader.Snapshot(ctx, "kept")
			if err != nil || chunkstore.EqualSnapshots(want, snapshot) != nil {
				t.Fatal("resolved snapshot differs", err)
			}
			output := t.TempDir()
			receipt, err := ConvertNativeCheckpoint(ctx, reader, plan, output, 1<<20, 16)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Entries == 0 || len(receipt.Files) != 2 {
				t.Fatal("missing baseline output")
			}
			resume, err := ConvertNativeCheckpoint(ctx, reader, plan, output, 1<<20, 32)
			if err != nil || !reflect.DeepEqual(receipt, resume) {
				t.Fatal("resume changed baseline", err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if after := testNativePlan(t, checkpoint, []string{"kept"}); after.Baseline.Snapshot != plan.Baseline.Snapshot {
				t.Fatal("checkpoint was mutated by read-only conversion")
			}
			if _, err := OpenNativeCheckpoint(ctx, dir, testNativePlan(t, dir, []string{"kept"})); err == nil {
				t.Fatal("live store lock bypassed")
			}
			if err := os.WriteFile(filepath.Join(checkpoint, "unbound"), []byte("x"), 0600); err != nil {
				t.Fatal(err)
			}
			if r, err := OpenNativeCheckpoint(ctx, checkpoint, plan); err == nil {
				r.Close()
				t.Fatal("unbound checkpoint entry admitted")
			}
		})
	}
}
