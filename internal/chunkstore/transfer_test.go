package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestRetainTransferSlotsSparseAliases(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	// Fewer points than capacity can still alias; do not just truncate by count.
	// The non-aliased old point must survive despite the wide timestamp span.
	points := []Point{{Timestamp: 10, Value: 1}, {Timestamp: 20, Value: 2}, {Timestamp: 70, Value: negativeZero}, {Timestamp: 90, Value: math.Inf(1)}}
	got := retainTransferSlots(points, Retention{Step: 10, Count: 6})
	want := []Point{{Timestamp: 20, Value: 2}, {Timestamp: 70, Value: negativeZero}, {Timestamp: 90, Value: math.Inf(1)}}
	if len(got) != len(want) {
		t.Fatal(got)
	}
	for i, p := range want {
		if got[i].Timestamp != p.Timestamp || math.Float64bits(got[i].Value) != math.Float64bits(p.Value) {
			t.Fatalf("point %d changed: got=%+v want=%+v", i, got[i], p)
		}
	}
}

func TestVerifyTransferCopyRequiresStableRevisionAcrossSync(t *testing.T) {
	ctx := t.Context()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true})
	defer closeTestStore(t, source)
	target, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true})
	defer closeTestStore(t, target)
	const name = "transfer.recover"
	createTestMetric(t, source, name)
	if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
		t.Fatal(err)
	}
	records, err := source.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	target.transferReadTestHook = func() {
		target.transferReadTestHook = nil
		if _, err := target.ImportTransfer(ctx, records); err != nil {
			t.Fatal(err)
		}
	}
	if copied, err := target.VerifyTransferCopy(ctx, records); err != nil || !slices.Equal(copied, []bool{false}) {
		t.Fatalf("copy published after the sync: %v, %v", copied, err)
	}
	if copied, err := target.VerifyTransferCopy(ctx, records); err != nil || !slices.Equal(copied, []bool{true}) {
		t.Fatalf("durable copy: %v, %v", copied, err)
	}
	target.transferReadTestHook = func() {
		if err := target.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 2}}); err != nil {
			t.Fatal(err)
		}
	}
	if copied, err := target.VerifyTransferCopy(ctx, records); err != nil || !slices.Equal(copied, []bool{false}) {
		t.Fatalf("copy revised during verification: %v, %v", copied, err)
	}
}

func TestTransferDurableAcrossUnsyncedForegroundCrash(t *testing.T) {
	ctx := context.Background()
	sourceFS, targetFS := vfs.NewStrictMem(), vfs.NewStrictMem()
	source, now := openTestStore(t, "/source", Options{fs: sourceFS, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	createTestMetric(t, source, "transfer.crash")
	if err := source.UpdateMany(ctx, "transfer.crash", []Point{{Timestamp: int64(now - 60), Value: 42.5}}); err != nil {
		t.Fatal(err)
	}
	records, err := source.ExportTransfer(ctx, []string{"transfer.crash"})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := openTestStore(t, "/target", Options{fs: targetFS, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	proofs, err := target.ImportTransfer(ctx, records)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.VerifyTransfer(ctx, proofs); err != nil {
		t.Fatal(err)
	}
	if err := source.DeleteTransfer(ctx, []TransferVersion{records[0].Version}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		s  *Store
		fs *vfs.MemFS
	}{{source, sourceFS}, {target, targetFS}} {
		entry.fs.SetIgnoreSyncs(true)
		closeTestStore(t, entry.s)
		entry.fs.ResetToSyncedState()
		entry.fs.SetIgnoreSyncs(false)
	}
	source, _ = openTestStore(t, "/source", Options{fs: sourceFS, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	target, _ = openTestStore(t, "/target", Options{fs: targetFS, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	if _, err := source.Metadata(ctx, "transfer.crash"); !errors.Is(err, ErrNotFound) {
		t.Fatal("source delete not durable", err)
	}
	if err := target.VerifyTransfer(ctx, proofs); err != nil {
		t.Fatal("target receipt not durable", err)
	}
	snapshot, err := target.Snapshot(ctx, "transfer.crash")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 42.5 {
		t.Fatal("target history lost")
	}
	// Concurrent normal writes may advance a verified generation. Replacement
	// cannot reuse that receipt, and an arbitrary digest never verifies.
	if err := target.UpdateMany(ctx, "transfer.crash", []Point{{Timestamp: int64(now - 30), Value: 84}}); err != nil {
		t.Fatal(err)
	}
	if err := target.VerifyTransfer(ctx, proofs); err != nil {
		t.Fatal("normal write invalidates receipt", err)
	}
	altered := append([]TransferProof(nil), proofs...)
	altered[0].Digest = "wrong"
	if err := target.VerifyTransfer(ctx, altered); err == nil {
		t.Fatal("forged proof accepted")
	}
	if _, err := target.Replace(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := target.VerifyTransfer(ctx, proofs); !errors.Is(err, ErrConflict) {
		t.Fatal("replacement inherited receipt", err)
	}
}

// A crash after private WAL staging or partial catalog publication must leave only
// complete visible names. Replaying the transfer uses fresh reserved IDs and
// must retain the same points and durable proofs as an uninterrupted import.
func TestTransferInterruptedImportResumes(t *testing.T) {
	for _, phase := range []string{"staged-batch", "published-group"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true})
			defer closeTestStore(t, source)
			names := make([]string, 17)
			for i := range names {
				names[i] = fmt.Sprintf("transfer.resume.%02d", i)
				createTestMetric(t, source, names[i])
				if err := source.UpdateMany(ctx, names[i], []Point{{Timestamp: int64(now - 60), Value: math.Float64frombits(1 << 63)}, {Timestamp: int64(now - 120), Value: float64(i) + 0.25}}); err != nil {
					t.Fatal(err)
				}
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			fs := vfs.NewStrictMem()
			opts := Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour}
			target, _ := openTestStore(t, "/target", opts)
			interrupted, cancel := context.WithCancel(ctx)
			defer cancel()
			calls := 0
			if phase == "staged-batch" {
				target.nativeTestHook = func(step string) error {
					if step == "transfer-stage-batch" {
						calls++
						cancel()
						return context.Canceled
					}
					return nil
				}
			} else {
				target.transferApplyTestHook = func() {
					calls++
					if calls == 2 {
						cancel()
					}
				}
			}
			if _, err := target.ImportTransfer(interrupted, records); !errors.Is(err, context.Canceled) || calls == 0 {
				t.Fatalf("interruption calls=%d err=%v", calls, err)
			}
			closeForCrashRecovery(t, target, fs)
			target, _ = openTestStore(t, "/target", opts)
			defer closeTestStore(t, target)
			for _, name := range names {
				got, err := target.Snapshot(ctx, name)
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				want, err := source.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := EqualSnapshots(want, got); err != nil {
					t.Fatalf("partially published %s: %v", name, err)
				}
			}
			proofs, err := target.ImportTransfer(ctx, records)
			if err != nil {
				t.Fatal(err)
			}
			if err := target.VerifyTransfer(ctx, proofs); err != nil {
				t.Fatal(err)
			}
			for _, name := range names {
				want, err := source.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := EqualSnapshots(want, got); err != nil {
					t.Fatalf("resumed %s: %v", name, err)
				}
			}
		})
	}
}

func TestTransferRetireRequiresAllCurrentVersions(t *testing.T) {
	ctx := context.Background()
	s, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	defer closeTestStore(t, s)
	for _, name := range []string{"retire.a", "retire.b"} {
		createTestMetric(t, s, name)
		if err := s.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	records, err := s.ExportTransfer(ctx, []string{"retire.a", "retire.b"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StageTransferDelete(ctx, strings.Repeat("1", 64), []TransferVersion{records[0].Version}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetireTransferredStore(ctx, strings.Repeat("1", 64)); !errors.Is(err, ErrConflict) {
		t.Fatal("unverified member retired", err)
	}
	pending, err := s.PendingTransferDelete(ctx, strings.Repeat("1", 64), 256)
	if err != nil || len(pending) != 1 || pending[0] != "retire.b" {
		t.Fatal("pending", pending, err)
	}
	if err := s.StageTransferDelete(ctx, strings.Repeat("1", 64), []TransferVersion{records[1].Version}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMany(ctx, "retire.a", []Point{{Timestamp: int64(now - 30), Value: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetireTransferredStore(ctx, strings.Repeat("1", 64)); !errors.Is(err, ErrConflict) {
		t.Fatal("late source write was deleted", err)
	}
	updated, err := s.ExportTransfer(ctx, []string{"retire.a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StageTransferDelete(ctx, strings.Repeat("1", 64), []TransferVersion{updated[0].Version}); err != nil {
		t.Fatal(err)
	}
	high, err := s.CatalogHighWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.RetireTransferredStore(ctx, strings.Repeat("1", 64)); err != nil || n != 2 {
		t.Fatal("retire", n, err)
	}
	if page, err := s.List(ctx, ""); err != nil || len(page) != 0 {
		t.Fatal("catalog not empty", page, err)
	}
	if n, err := s.RetireTransferredStore(ctx, strings.Repeat("1", 64)); err != nil || n != 0 {
		t.Fatal("retirement retry", n, err)
	}
	m, err := s.Create(ctx, MetricConfig{Name: "retire.a", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average})
	if err != nil {
		t.Fatal(err)
	}
	if m.ID <= high {
		t.Fatal("retirement reused ID")
	}
}

func TestTransferRetirementMarkersAreOperationScoped(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	createTestMetric(t, s, "scope.metric")
	records, err := s.ExportTransfer(ctx, []string{"scope.metric"})
	if err != nil {
		t.Fatal(err)
	}
	first, second := strings.Repeat("1", 64), strings.Repeat("2", 64)
	if err := s.StageTransferDelete(ctx, first, []TransferVersion{records[0].Version}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetireTransferredStore(ctx, second); !errors.Is(err, ErrConflict) {
		t.Fatal("another operation authorized retirement", err)
	}
	pending, err := s.PendingTransferDelete(ctx, second, 256)
	if err != nil || len(pending) != 1 {
		t.Fatal("pending operation scope", pending, err)
	}
	if _, err := s.RetireTransferredStore(ctx, first); err != nil {
		t.Fatal(err)
	}
}

func TestTransferCachedMetadataAndUnorderedIDsMatchReference(t *testing.T) {
	ctx := context.Background()
	s, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, MetadataCacheSize: 4 << 20, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	names := []string{"order.z", "order.a", "order.q", "order.b"}
	for i, name := range names {
		createTestMetric(t, s, name)
		if err := s.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	requested := []string{names[2], names[0], names[3], names[1]}
	got, err := s.ExportTransfer(ctx, requested)
	if err != nil {
		t.Fatal(err)
	}
	view := s.db.NewSnapshot()
	defer view.Close()
	for i, name := range requested {
		m, err := metadataFrom(view, name)
		if err != nil {
			t.Fatal(err)
		}
		want, err := s.transferFrom(ctx, view, m)
		if err != nil {
			t.Fatal(err)
		}
		if got[i].Version != want.Version || transferDigest(got[i]) != transferDigest(want) {
			t.Fatal("ordered export changed bytes", name)
		}
	}
	if s.Stats().TransferExport.MetadataHits != uint64(len(names)) {
		t.Fatal("existing metadata cache not used")
	}
	target, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	proofs, err := target.ImportTransfer(ctx, got)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(proofs)
	if err := target.VerifyTransfer(ctx, proofs); err != nil {
		t.Fatal("unordered proof verification", err)
	}
	if err := target.UpdateMany(ctx, names[0], []Point{{Timestamp: int64(now - 50), Value: 99}}); err != nil {
		t.Fatal(err)
	}
	if err := target.VerifyTransfer(ctx, proofs); err != nil {
		t.Fatal("newer foreground revision invalidated a durable receipt", err)
	}
	proofs[0].Target.ID++
	if err := target.VerifyTransfer(ctx, proofs); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong target identity accepted", err)
	}
}
