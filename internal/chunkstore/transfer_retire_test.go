package chunkstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestTransferRetirementReleasesStripeAndPreservesReader(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	target, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	defer closeTestStore(t, source)
	defer closeTestStore(t, target)
	name := "retire.reader"
	base := int64(now - now%60)
	for _, s := range []*Store{source, target} {
		createTestMetric(t, s, name)
		if err := s.UpdateMany(ctx, name, []Point{{Timestamp: base - 120, Value: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	old, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	reader := target.db.NewSnapshot()
	defer reader.Close()
	records, err := source.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	target.transferRetireTestHook = func() {
		once.Do(func() {
			done := make(chan error, 1)
			go func() { done <- target.UpdateMany(ctx, name, []Point{{Timestamp: base - 60, Value: 9}}) }()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("retirement still holds the writer stripe")
			}
			snap, err := snapshotFrom(reader, name)
			if err != nil {
				t.Error(err)
			} else if err := EqualSnapshots(old, snap); err != nil {
				t.Error(err)
			}
		})
	}
	if _, err := target.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 2 {
		t.Fatalf("foreground point missing: %+v", got.Archives)
	}
	snap, err := snapshotFrom(reader, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := EqualSnapshots(old, snap); err != nil {
		t.Fatal(err)
	}
	n, err := target.GenerationKeysForTest(old.Metadata)
	if err != nil || n != 0 {
		t.Fatal(fmt.Sprintf("retired keys=%d err=%v", n, err))
	}
}
