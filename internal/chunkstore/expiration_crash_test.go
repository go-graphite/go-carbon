package chunkstore

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestExpirationBatchCrashRecovery(t *testing.T) {
	for _, action := range []string{"lost", "synced", "flushed", "recreated", "periodic"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				strict := vfs.NewStrictMem()
				fs := &walSyncFS{FS: strict}
				options := Options{fs: fs}
				if action == "periodic" {
					options.SyncInterval = time.Second
				}
				s, now := openTestStore(t, "/store", options)
				old := createTestMetric(t, s, "expired")
				keep := createTestMetric(t, s, "keep")
				second := createTestMetric(t, s, "also-expired")
				for _, m := range []Metadata{old, keep, second} {
					if err := s.UpdateMany(ctx, m.Name, []Point{{Timestamp: int64(now), Value: 42}}); err != nil {
						t.Fatal(err)
					}
				}
				old, _ = s.Metadata(ctx, old.Name)
				keep, _ = s.Metadata(ctx, keep.Name)
				second, _ = s.Metadata(ctx, second.Name)
				retention := old.Retentions[0]
				oldChunk := chunkKey(old, 0, (now/retention.Step%retention.Count)/chunkSlots)
				_, closer, err := s.db.Get(oldChunk)
				if err != nil {
					t.Fatalf("missing initial chunk: %v", err)
				}
				closer.Close()
				batch := s.db.NewBatch()
				summary := SidecarSummary{Present: true, Points: 2, Oldest: int64(now)}
				if err := s.commitSidecars(batch, old.Name, &summary, s.commit); err != nil {
					t.Fatal(err)
				}
				batch.Close()
				if err := s.logSync(); err != nil {
					t.Fatal(err)
				}
				s.pendingSync.Store(false)
				_, cursor, _, err := s.CatalogChanges(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}
				initialSyncs := fs.syncs.Load()
				outcomes, err := s.DeleteBatchIfUnchanged(ctx, []Metadata{old, second})
				if err != nil || len(outcomes) != 2 || outcomes[0] != nil || outcomes[1] != nil {
					t.Fatalf("outcomes=%v: %v", outcomes, err)
				}
				if fs.syncs.Load() != initialSyncs {
					t.Fatal("expiration synced its WAL")
				}
				if _, err := s.Snapshot(ctx, old.Name); !errors.Is(err, ErrNotFound) {
					t.Fatalf("deleted snapshot: %v", err)
				}
				if s.Sidecars().Count != 0 {
					t.Fatal("deleted sidecars remained")
				}
				if _, _, reset, err := s.CatalogChanges(ctx, cursor); err != nil || !reset {
					t.Fatalf("index invalidation=%v %v", reset, err)
				}
				switch action {
				case "synced":
					if err := s.logSync(); err != nil {
						t.Fatal(err)
					}
				case "flushed":
					if err := s.db.Flush(); err != nil {
						t.Fatal(err)
					}
				case "recreated":
					m := createTestMetric(t, s, old.Name)
					if m.ID == old.ID {
						t.Fatal("reused deleted identity")
					}
					if err := s.UpdateMany(ctx, old.Name, []Point{{Timestamp: int64(now), Value: 7}}); err != nil {
						t.Fatal(err)
					}
				case "periodic":
					time.Sleep(time.Second)
					synctest.Wait()
					if fs.syncs.Load() != initialSyncs+1 {
						t.Fatal("expiration missed periodic sync")
					}
				}
				closeForRecovery(t, s, strict, true)
				reopened, _ := openTestStore(t, "/store", Options{fs: strict})
				defer closeTestStore(t, reopened)
				snap, err := reopened.Snapshot(ctx, old.Name)
				wantPresent := action == "lost" || action == "recreated"
				if wantPresent {
					want := 42.0
					if action == "recreated" {
						want = 7
					}
					if err != nil || len(snap.Archives[0].Points) != 1 || snap.Archives[0].Points[0].Value != want {
						t.Fatalf("recovered snapshot=%+v: %v", snap, err)
					}
				} else if !errors.Is(err, ErrNotFound) {
					t.Fatalf("recovered delete: %v", err)
				}
				page, err := reopened.ListPage(ctx, "", "", 10)
				expectedCount := 1
				if wantPresent {
					expectedCount++
				}
				if action == "lost" {
					expectedCount++
				}
				if err != nil || len(page) != expectedCount {
					t.Fatalf("recovered catalog=%v %v", page, err)
				}
				sidecars := uint64(0)
				if action == "lost" {
					sidecars = 1
				}
				if reopened.Sidecars().Count != sidecars {
					t.Fatalf("recovered sidecars=%+v", reopened.Sidecars())
				}
				if _, err := reopened.Snapshot(ctx, keep.Name); err != nil {
					t.Fatal(err)
				}
				secondSnapshot, secondErr := reopened.Snapshot(ctx, second.Name)
				if action == "lost" {
					if secondErr != nil || len(secondSnapshot.Archives[0].Points) != 1 || secondSnapshot.Archives[0].Points[0].Value != 42 {
						t.Fatalf("batch partially survived: %+v %v", secondSnapshot, secondErr)
					}
				} else if !errors.Is(secondErr, ErrNotFound) {
					t.Fatalf("batch partially deleted: %v", secondErr)
				}
				if action != "lost" {
					for _, key := range [][]byte{revisionKey(old), oldChunk} {
						_, closer, err := reopened.db.Get(key)
						if closer != nil {
							closer.Close()
						}
						if !errors.Is(err, pebble.ErrNotFound) {
							t.Fatalf("old data survived recovered delete: %x: %v", key, err)
						}
					}
				}
			})
		})
	}
}
