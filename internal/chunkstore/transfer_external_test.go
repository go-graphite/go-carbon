package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func transferStore(t *testing.T, continuation, buckets bool, direct ...bool) *store.Store {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "originals")
	if err := os.MkdirAll(source, 0750); err != nil {
		t.Fatal(err)
	}
	options := store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(1000000, 0) }}
	if continuation {
		options.Continuation = &whisperio.ContinuationCodec{DirectLateWrites: len(direct) == 0 || direct[0]}
		options.IncrementalSource = source
		options.TimeBuckets = buckets
	}
	s, err := store.Open(filepath.Join(root, "db"), options)
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
func putTransferPoints(t *testing.T, s *store.Store, name string, points []store.Point) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Metadata(ctx, name); errors.Is(err, store.ErrNotFound) {
		if _, err := s.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 600}, {Step: 60, Count: 1000}}, AggregationMethod: store.Sum}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.UpdateMany(ctx, name, points); err != nil {
		t.Fatal(err)
	}
}

func TestTransferRawMergeVerifyDelete(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"v6", true, false}, {"v7", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			source := transferStore(t, mode.continuation, mode.buckets)
			target := transferStore(t, mode.continuation, mode.buckets)
			name := "move.history"
			var points []store.Point
			for ts := int64(995000); ts < 999900; ts += 10 {
				points = append(points, store.Point{Timestamp: ts, Value: float64(ts % 91)})
			}
			putTransferPoints(t, source, name, points)
			records, err := source.ExportTransfer(ctx, []string{name})
			if err != nil {
				t.Fatal(err)
			}
			var wire bytes.Buffer
			if err := store.WriteTransfer(&wire, records); err != nil {
				t.Fatal(err)
			}
			records, err = store.ReadTransfer(bytes.NewReader(wire.Bytes()), int64(wire.Len()))
			if err != nil {
				t.Fatal(err)
			}
			proof, err := target.ImportTransfer(ctx, records)
			if err != nil {
				t.Fatal(err)
			}
			if err := target.VerifyTransfer(ctx, proof); err != nil {
				t.Fatal(err)
			}
			want, err := source.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			// Real overlap and a target-only point: target wins, history remains.
			putTransferPoints(t, target, name, []store.Point{{Timestamp: 999880, Value: 123.25}, {Timestamp: 999910, Value: 456.5}})
			targetBefore, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			proof, err = target.ImportTransfer(ctx, records)
			if err != nil {
				t.Fatal(err)
			}
			if !proof[0].Merged {
				t.Fatal("did not merge")
			}
			if err := target.VerifyTransfer(ctx, proof); err != nil {
				t.Fatal(err)
			}
			got, err = target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			for ai, archive := range targetBefore.Archives {
				values := map[int64]float64{}
				for _, p := range got.Archives[ai].Points {
					values[p.Timestamp] = p.Value
				}
				for _, p := range archive.Points {
					if values[p.Timestamp] != p.Value {
						t.Fatalf("archive %d target point %+v lost", ai, p)
					}
				}
			}
			for ai, archive := range want.Archives {
				if len(got.Archives[ai].Points) < len(archive.Points) {
					t.Fatal("history count decreased")
				}
			}
			// An in-flight source write makes deletion fail atomically.
			putTransferPoints(t, source, name, []store.Point{{Timestamp: 999920, Value: 789}})
			if err := source.DeleteTransfer(ctx, []store.TransferVersion{records[0].Version}); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("stale delete: %v", err)
			}
			records, err = source.ExportTransfer(ctx, []string{name})
			if err != nil {
				t.Fatal(err)
			}
			proof, err = target.ImportTransfer(ctx, records)
			if err != nil {
				t.Fatal(err)
			}
			if err := target.VerifyTransfer(ctx, proof); err != nil {
				t.Fatal(err)
			}
			if err := source.DeleteTransfer(ctx, []store.TransferVersion{records[0].Version}); err != nil {
				t.Fatal(err)
			}
			if err := source.DeleteTransfer(ctx, []store.TransferVersion{records[0].Version}); err != nil {
				t.Fatal("lost-response retry", err)
			}
			if _, err := source.Metadata(ctx, name); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("source remains", err)
			}
		})
	}
}

func TestTransferWireRejectsDamageAndOversize(t *testing.T) {
	s := transferStore(t, false, false)
	putTransferPoints(t, s, "wire", []store.Point{{Timestamp: 999990, Value: 1}})
	records, err := s.ExportTransfer(context.Background(), []string{"wire"})
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := store.WriteTransfer(&wire, records); err != nil {
		t.Fatal(err)
	}
	valid := wire.Bytes()
	for i, b := range [][]byte{valid[:len(valid)-1], append(append([]byte(nil), valid...), 0), bytes.Repeat([]byte{255}, 32)} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if _, err := store.ReadTransfer(bytes.NewReader(b), 1<<20); err == nil {
				t.Fatal("invalid wire accepted")
			}
		})
	}
	damaged := bytes.Clone(valid)
	damaged[len(damaged)-1] ^= 1
	if _, err := store.ReadTransfer(bytes.NewReader(damaged), 1<<20); err == nil {
		t.Fatal("bad checksum accepted")
	}
	if _, err := store.ReadTransfer(bytes.NewReader(valid), int64(len(valid)-1)); err == nil {
		t.Fatal("oversize accepted")
	}
}

func TestTransferRejectsForgedPointSampleBeforePublication(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprint(continuation), func(t *testing.T) {
			ctx := context.Background()
			source := transferStore(t, continuation, continuation)
			target := transferStore(t, continuation, continuation)
			putTransferPoints(t, source, "sample.forged", []store.Point{{Timestamp: 999990, Value: 42}})
			records, err := source.ExportTransfer(ctx, []string{"sample.forged"})
			if err != nil {
				t.Fatal(err)
			}
			records[0].PointDigest = "wrong"
			if _, err := target.ImportTransfer(ctx, records); err == nil {
				t.Fatal("forged point sample accepted")
			}
			if _, err := target.Metadata(ctx, "sample.forged"); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("failed sample became visible", err)
			}
		})
	}
}
