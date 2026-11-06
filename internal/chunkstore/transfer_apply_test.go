package chunkstore

import (
	"context"
	"fmt"
	"math"
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

func TestTransferContendedGroupUsesSingleStripeQueues(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source-split", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	target, _ := openTestStore(t, "/target-split", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	defer closeTestStore(t, source)
	defer closeTestStore(t, target)
	now -= now % 60
	names := []string{"split.first", "split.last"}
	if target.metricStripe(names[0]) > target.metricStripe(names[1]) {
		names[0], names[1] = names[1], names[0]
	}
	for _, name := range names {
		createTestMetric(t, source, name)
		createTestMetric(t, target, name)
		if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 120), Value: math.Float64frombits(0x3ff0000000000001)}}); err != nil {
			t.Fatal(err)
		}
		if err := target.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: math.Float64frombits(1 << 63)}}); err != nil {
			t.Fatal(err)
		}
	}
	var collision string
	for i := 0; ; i++ {
		collision = fmt.Sprintf("split.independent.%d", i)
		if target.metricStripe(collision) == target.metricStripe(names[0]) {
			break
		}
	}
	createTestMetric(t, target, collision)
	records, err := source.ExportTransfer(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	held := &target.metricMu[target.metricStripe(names[1])]
	held.Lock()
	var once sync.Once
	prepared := make(chan struct{})
	target.transferApplyTestHook = func() { once.Do(func() { close(prepared) }) }
	done := make(chan error, 1)
	go func() {
		proofs, e := target.ImportTransfer(ctx, records)
		if e == nil {
			e = target.VerifyTransfer(ctx, proofs)
		}
		done <- e
	}()
	select {
	case <-prepared:
	case e := <-done:
		held.Unlock()
		t.Fatalf("publication failed before preparation: %v", e)
	case <-time.After(2 * time.Second):
		held.Unlock()
		<-done
		t.Fatal("preparation did not finish")
	}
	written := make(chan error, 1)
	go func() { written <- target.UpdateMany(ctx, collision, []Point{{Timestamp: int64(now - 60), Value: 7}}) }()
	select {
	case e := <-written:
		if e != nil {
			held.Unlock()
			<-done
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		held.Unlock()
		<-done
		t.Fatal("publication retained a sibling stripe while waiting")
	}
	held.Unlock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if stats := target.TransferApplyStats(); stats.SplitGroups == 0 || stats.StripeHolds[1] < 2 {
		t.Fatalf("did not split contended group: %+v", stats)
	}
	for _, name := range names {
		got, e := target.Snapshot(ctx, name)
		if e != nil {
			t.Fatal(e)
		}
		if len(got.Archives[0].Points) != 2 {
			t.Fatal("lost archive points", got.Archives[0])
		}
		for _, p := range got.Archives[0].Points {
			want := uint64(0x3ff0000000000001)
			if p.Timestamp == int64(now-60) {
				want = 1 << 63
			}
			if math.Float64bits(p.Value) != want {
				t.Fatalf("changed value bits %x != %x", math.Float64bits(p.Value), want)
			}
		}
	}
}
