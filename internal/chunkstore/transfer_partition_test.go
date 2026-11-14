package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// A contended group publishes its free, unchanged names in one commit and
// prepares again only the names whose targets changed. The committed bytes
// must equal the single-name publication it replaces, with the same live
// writes landing between preparation and commit.
func TestTransferPartitionedSplitMatchesSingles(t *testing.T) {
	ctx := context.Background()
	run := func(singles bool) ([][2][]byte, TransferApplyStats, int) {
		source, now := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
		target, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
		t.Cleanup(func() { closeTestStore(t, source); closeTestStore(t, target) })
		target.transferSplitSingles = singles
		base := int64(now - now%60)
		names := make([]string, 8)
		for i := range names {
			names[i] = fmt.Sprintf("partition.%d", i)
			createTestMetric(t, source, names[i])
			createTestMetric(t, target, names[i])
			if err := source.UpdateMany(ctx, names[i], []Point{{Timestamp: base - 240, Value: float64(i) + 0.25}, {Timestamp: base - 180, Value: -float64(i)}}); err != nil {
				t.Fatal(err)
			}
			if err := target.UpdateMany(ctx, names[i], []Point{{Timestamp: base - 120, Value: 1e9 + float64(i)}}); err != nil {
				t.Fatal(err)
			}
		}
		records, err := source.ExportTransfer(ctx, names)
		if err != nil {
			t.Fatal(err)
		}
		// Live writes to names[0] land after the first two preparations: the
		// group fails validation, then names[0] fails again on its own.
		calls := 0
		target.transferApplyTestHook = func() {
			calls++
			if calls <= 2 {
				if err := target.UpdateMany(ctx, names[0], []Point{{Timestamp: base - 60*int64(calls), Value: 100 + float64(calls)}}); err != nil {
					t.Error(err)
				}
			}
		}
		proofs, err := target.ImportTransfer(ctx, records)
		if err != nil {
			t.Fatal(err)
		}
		target.transferApplyTestHook = nil
		if err := target.VerifyTransfer(ctx, proofs); err != nil {
			t.Fatal(err)
		}
		raw, err := target.RawKeyValuesForTest()
		if err != nil {
			t.Fatal(err)
		}
		return raw, target.TransferApplyStats(), calls
	}
	want, singleStats, singleCalls := run(true)
	got, partStats, partCalls := run(false)
	if len(got) != len(want) {
		t.Fatalf("%d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i][0], want[i][0]) || !bytes.Equal(got[i][1], want[i][1]) {
			t.Fatalf("key %d differs: %x=%x, want %x=%x", i, got[i][0], got[i][1], want[i][0], want[i][1])
		}
	}
	// Singles: group + 8 names + one retry of names[0]. Partitioned: group,
	// one round for all 8 names, one retry of names[0] alone.
	if singleCalls != 10 || partCalls != 3 {
		t.Fatalf("preparation rounds singles=%d partitioned=%d", singleCalls, partCalls)
	}
	if singleStats.StripeHolds[1] < 8 || partStats.StripeHolds[1] != 1 {
		t.Fatalf("commits singles=%v partitioned=%v", singleStats.StripeHolds, partStats.StripeHolds)
	}
}
