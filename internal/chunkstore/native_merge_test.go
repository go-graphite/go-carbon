package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeMergePinnedCachedAndRevisedBitExact(t *testing.T) {
	ctx := context.Background()
	donor, now := openTestStore(t, "/donor", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, donor)
	target, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	names := []string{"merge.cached", "merge.revised", "merge.existing"}
	want := make(map[string]Snapshot)
	for _, name := range names {
		createTestMetric(t, donor, name)
		if err := donor.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: math.Float64frombits(1 << 63)}, {Timestamp: int64(now - 30), Value: 1.0000000000000002}}); err != nil {
			t.Fatal(err)
		}
		var err error
		want[name], err = donor.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
	}
	createTestMetric(t, target, names[2])
	if err := target.UpdateMany(ctx, names[2], []Point{{Timestamp: int64(now - 10), Value: 99}}); err != nil {
		t.Fatal(err)
	}
	existing, err := target.Snapshot(ctx, names[2])
	if err != nil {
		t.Fatal(err)
	}
	var scans int
	donor.SetNativePendingSnapshot(func() (map[string]struct{}, error) { scans++; return map[string]struct{}{names[0]: {}}, nil })
	before := time.Now()
	pin, err := donor.PinNativeMergeSource("history-copy-1")
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	if scans != 0 || pin.MergeSession != "history-copy-1" || pin.SnapshotStartedAt.Before(before) || pin.SnapshotPinnedAt.Before(pin.SnapshotStartedAt) || pin.SnapshotPinnedAt.After(time.Now()) {
		t.Fatal("invalid snapshot boundary", pin, scans)
	}
	if err := donor.UpdateMany(ctx, names[1], []Point{{Timestamp: int64(now - 30), Value: 42}, {Timestamp: int64(now - 1), Value: 777}}); err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := pin.Write(ctx, names, &wire); err != nil {
		t.Fatal(err)
	}
	request := NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 3, Names: names, MergeSession: pin.MergeSession}
	request.Combine = true
	if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); !errors.Is(err, ErrFormat) {
		t.Fatal("merge entered combine path", err)
	}
	request.Combine = false
	receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.CopiedCount != 2 || receipt.DeferredCount != 1 || receipt.IsCopied(2) {
		t.Fatal(receipt)
	}
	for _, name := range names[:2] {
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := EqualSnapshots(want[name], got); err != nil {
			t.Fatal(name, err)
		}
	}
	got, err := target.Snapshot(ctx, names[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := EqualSnapshots(existing, got); err != nil {
		t.Fatal("existing target changed", err)
	}
}
