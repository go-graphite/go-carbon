package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeWindowCrashResume(t *testing.T) {
	for _, stage := range []string{"combine-staged", "combine-ingested", "combine-before-publish", "combine-name-published", "combine-receipt", "combine-published"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, source)
			now -= now % 60
			name := "window.resume"
			createTestMetric(t, source, name)
			if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 120), Value: 1}, {Timestamp: int64(now - 60), Value: 2}, {Timestamp: int64(now), Value: 3}}); err != nil {
				t.Fatal(err)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			window := &NativeWindow{From: int64(now - 90), Until: int64(now - 40)}
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, []string{name}, &wire, window); err != nil {
				t.Fatal(err)
			}
			fs := vfs.NewStrictMem()
			opts := Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			target, _ := openTestStore(t, "/target", opts)
			createTestMetric(t, target, name)
			outside := math.Float64frombits(0x8000000000000000)
			if err := target.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 120), Value: outside}, {Timestamp: int64(now - 60), Value: 42}}); err != nil {
				t.Fatal(err)
			}
			if err := target.syncWAL(); err != nil {
				t.Fatal(err)
			}
			request := combineTestRange([]string{name})
			request.Window = window
			target.nativeTestHook = func(got string) error {
				if got == stage {
					fs.SetIgnoreSyncs(true)
					return errors.New("crash")
				}
				return nil
			}
			if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("no crash")
			}
			closeTestStore(t, target)
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			target, _ = openTestStore(t, "/target", opts)
			defer closeTestStore(t, target)
			// No donor is needed once the staged intent is durable. Earlier crashes
			// can retransmit exactly the same checksummed window.
			receipt, found, err := target.ResumeNativeCombine(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				receipt, err = target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
			}
			if err != nil || receipt.CopiedCount != 1 {
				t.Fatal(receipt, err)
			}
			snap, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			points := snap.Archives[0].Points
			if len(points) != 2 || points[0].Timestamp != int64(now-120) || math.Float64bits(points[0].Value) != math.Float64bits(outside) || points[1].Timestamp != int64(now-60) || points[1].Value != 42 {
				t.Fatal(points)
			}
			// A completed retry must preserve foreground writes made after recovery.
			if err := target.UpdateMany(ctx, name, []Point{{Timestamp: int64(now), Value: 99}}); err != nil {
				t.Fatal(err)
			}
			if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err != nil {
				t.Fatal(err)
			}
			snap, err = target.Snapshot(ctx, name)
			if err != nil || len(snap.Archives[0].Points) != 3 || snap.Archives[0].Points[2].Value != 99 {
				t.Fatal(snap, err)
			}
		})
	}
}
