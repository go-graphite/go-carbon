package chunkstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

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
