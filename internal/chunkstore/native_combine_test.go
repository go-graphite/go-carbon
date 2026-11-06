package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func combineTestRange(names []string) NativeRange {
	return NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: true}
}

func TestNativeCombineConcurrentWriter(t *testing.T) {
	ctx := context.Background()
	open := func(path string) *Store {
		s, _ := openTestStore(t, path, Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MetadataCacheSize: 1 << 20, MemTableSize: 1 << 20})
		t.Cleanup(func() { closeTestStore(t, s) })
		return s
	}
	source, target, reference := open("/source"), open("/target"), open("/reference")
	name := "native.combine.concurrent"
	for _, s := range []*Store{source, target, reference} {
		createTestMetric(t, s, name)
		if err := s.UpdateMany(ctx, name, []Point{{Timestamp: 1790000040, Value: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, []string{name}, &wire); err != nil {
		t.Fatal(err)
	}
	changed := false
	target.nativeTestHook = func(stage string) error {
		if stage != "combine-before-publish" || changed {
			return nil
		}
		changed = true
		done := make(chan error, 1)
		go func() {
			for _, s := range []*Store{target, reference} {
				if err := s.UpdateMany(ctx, name, []Point{{Timestamp: 1790000040, Value: 42}, {Timestamp: 1790000100, Value: 7}}); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("foreground writer blocked during native preparation")
		}
	}
	if _, err := target.NativeImport(ctx, combineTestRange([]string{name}), bytes.NewReader(wire.Bytes())); err != nil {
		t.Fatal(err)
	}
	records, err := source.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	want, _ := reference.Snapshot(ctx, name)
	got, _ := target.Snapshot(ctx, name)
	if err := EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	if target.NativeStats().CombineRetries != 1 {
		t.Fatal("writer did not force exactly one revision retry", target.NativeStats())
	}
}

func TestNativeCombineCrashResume(t *testing.T) {
	for _, stage := range []string{"combine-reserved", "combine-staged", "combine-ingested", "combine-prepared", "combine-before-publish", "combine-name-published", "combine-receipt", "combine-published"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, source)
			names := []string{"combine.crash.a", "combine.crash.b"}
			for _, name := range names {
				createTestMetric(t, source, name)
				if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.Write(ctx, names, &wire); err != nil {
				t.Fatal(err)
			}
			fs := vfs.NewStrictMem()
			target, _ := openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			for _, name := range names {
				createTestMetric(t, target, name)
				if err := target.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 42}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := target.syncWAL(); err != nil {
				t.Fatal(err)
			}
			var publishedID uint64
			target.nativeTestHook = func(got string) error {
				if got != stage {
					return nil
				}
				if stage == "combine-name-published" {
					// Persist the first piece with a later foreground write,
					// then crash before the range acknowledgement exists.
					if err := target.UpdateMany(ctx, names[0], []Point{{Timestamp: int64(now - 30), Value: 99}}); err != nil {
						return err
					}
					m, _ := target.Metadata(ctx, names[0])
					publishedID = m.ID
					if err := target.syncWAL(); err != nil {
						return err
					}
				}
				fs.SetIgnoreSyncs(true)
				return errors.New("simulated crash")
			}
			request := combineTestRange(names)
			if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("crash was not injected")
			}
			closeTestStore(t, target)
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			target, _ = openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, target)
			receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
			if err != nil {
				t.Fatal("resume", err)
			}
			if receipt.CopiedCount != 2 || receipt.DeferredCount != 0 {
				t.Fatal(receipt)
			}
			for _, name := range names {
				snapshot, err := target.Snapshot(ctx, name)
				if err != nil || len(snapshot.Archives[0].Points) == 0 || snapshot.Archives[0].Points[0].Value != 42 {
					t.Fatal("lost target precedence", snapshot, err)
				}
			}
			if publishedID != 0 {
				m, _ := target.Metadata(ctx, names[0])
				if m.ID != publishedID {
					t.Fatal("already committed piece was imported twice", m.ID, publishedID)
				}
				snapshot, _ := target.Snapshot(ctx, names[0])
				if len(snapshot.Archives[0].Points) != 2 || snapshot.Archives[0].Points[1].Value != 99 {
					t.Fatal("post-piece foreground point lost", snapshot)
				}
			}
		})
	}
}
