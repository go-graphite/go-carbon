package chunkstore

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativePendingCheckDifferentialAndFlushRace(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	names := []string{"pending.queued", "pending.flushed", "pending.stable", "pending.new"}
	for _, name := range names {
		createTestMetric(t, source, name)
		if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: math.Copysign(0, -1)}, {Timestamp: int64(now - 30), Value: 42}}); err != nil {
			t.Fatal(err)
		}
	}
	pending := map[string]bool{names[0]: true, names[1]: true}
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
		out := map[string]struct{}{}
		for n, p := range pending {
			if p {
				out[n] = struct{}{}
			}
		}
		return out, nil
	})
	old, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	checks := 0
	source.SetNativePendingCheck(func(name string) bool { checks++; return pending[name] })
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) { t.Fatal("whole-cache snapshot called"); return nil, nil })
	current, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if checks != 0 {
		t.Fatal("pin checked live cache", checks)
	}
	for _, window := range []*NativeWindow{nil, {From: int64(now - 60), Until: int64(now)}} {
		var a, b bytes.Buffer
		if err := old.WriteWindow(ctx, names, &a, window); err != nil {
			t.Fatal(err)
		}
		if err := current.WriteWindow(ctx, names, &b, window); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a.Bytes(), b.Bytes()) {
			t.Fatal("stable-cache export differs bit for bit")
		}
	}
	// A pending write may commit and be confirmed between pin and the cache
	// check. The subsequent revision snapshot must still omit its stale history.
	source.SetNativePendingCheck(func(name string) bool {
		if name == names[1] {
			if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 10), Value: 9}}); err != nil {
				t.Fatal(err)
			}
			return false
		}
		return name == names[0] || name == names[3]
	})
	var wire bytes.Buffer
	if err := current.Write(ctx, names, &wire); err != nil {
		t.Fatal(err)
	}
	stream, err := NewTransferStream(&wire)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for range stream.Count {
		record, err := stream.Next()
		if err != nil {
			t.Fatal(err)
		}
		seen[record.Config.Name] = record.Version.ID != 0
	}
	if _, _, err := stream.Finish(); err != nil {
		t.Fatal(err)
	}
	if seen[names[0]] || seen[names[1]] || !seen[names[2]] || seen[names[3]] {
		t.Fatal("pending/confirmed/new exclusions differ", seen)
	}
	// The explicit merge contract continues to own its delta backfill and must
	// never consult either cache callback.
	source.SetNativePendingCheck(func(string) bool { t.Fatal("merge consulted cache"); return true })
	merge, err := source.PinNativeMergeSource("test-merge")
	if err != nil {
		t.Fatal(err)
	}
	defer merge.Close()
	wire.Reset()
	if err := merge.Write(ctx, names, &wire); err != nil {
		t.Fatal(err)
	}
}
