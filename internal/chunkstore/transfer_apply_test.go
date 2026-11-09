package chunkstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestTransferPreparationLetsCollidingForegroundWriteFinish(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	target, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	defer closeTestStore(t, source)
	defer closeTestStore(t, target)
	name := "transfer.optimistic"
	base := int64(now - now%60)
	for _, s := range []*Store{source, target} {
		createTestMetric(t, s, name)
	}
	var collision string
	for i := 0; ; i++ {
		collision = fmt.Sprintf("foreground.collision.%d", i)
		if target.metricStripe(collision) == target.metricStripe(name) {
			break
		}
	}
	createTestMetric(t, target, collision)
	if err := source.UpdateMany(ctx, name, []Point{{Timestamp: base - 180, Value: 1}, {Timestamp: base - 120, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	records, err := source.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	preparations := 0
	target.transferApplyTestHook = func() {
		preparations++
		once.Do(func() {
			done := make(chan error, 1)
			go func() {
				if err := target.UpdateMany(ctx, collision, []Point{{Timestamp: base - 60, Value: 11}}); err != nil {
					done <- err
					return
				}
				// The target changes after preparation; publication must retry.
				done <- target.UpdateMany(ctx, name, []Point{{Timestamp: base - 120, Value: 9}, {Timestamp: base - 60, Value: 7}})
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("preparation held a foreground writer stripe")
			}
		})
	}
	proofs, err := target.ImportTransfer(ctx, records)
	if err != nil {
		t.Fatal(err)
	}
	if preparations != 2 {
		t.Fatalf("preparations=%d, expected one revision retry", preparations)
	}
	if err := target.VerifyTransfer(ctx, proofs); err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]float64{base - 180: 1, base - 120: 9, base - 60: 7}
	for _, p := range got.Archives[0].Points {
		if value, ok := want[p.Timestamp]; !ok || value != p.Value {
			t.Fatalf("lost/reordered foreground write: %+v", got.Archives[0])
		}
		delete(want, p.Timestamp)
	}
	if len(want) != 0 {
		t.Fatal("missing points", want)
	}
}

func TestTransferAbsentPreparationRetriesConcurrentCreate(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	target, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	defer closeTestStore(t, source)
	defer closeTestStore(t, target)
	name := "transfer.create.race"
	createTestMetric(t, source, name)
	if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
		t.Fatal(err)
	}
	records, err := source.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	target.transferApplyTestHook = func() {
		once.Do(func() {
			createTestMetric(t, target, name)
			if err := target.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 99}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	proofs, err := target.ImportTransfer(ctx, records)
	if err != nil {
		t.Fatal(err)
	}
	if !proofs[0].Merged {
		t.Fatal("concurrent creation was overwritten")
	}
	got, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 1 || got.Archives[0].Points[0].Value != 99 {
		t.Fatal(got)
	}
}
