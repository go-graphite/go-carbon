package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeLiveCreateAtPublication(t *testing.T) {
	ctx := context.Background()
	source, _ := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	target, now := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	createTestMetric(t, source, "native.race")
	if err := source.UpdateMany(ctx, "native.race", []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, []string{"native.race"}, &wire); err != nil {
		t.Fatal(err)
	}
	target.nativeTestHook = func(stage string) error {
		if stage == "before-publish" {
			done := make(chan struct{})
			go func() {
				defer close(done)
				createTestMetric(t, target, "native.race")
				if err := target.UpdateMany(ctx, "native.race", []Point{{Timestamp: int64(now - 30), Value: 42}}); err != nil {
					t.Error(err)
				}
			}()
			<-done
		}
		return nil
	}
	request := NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{"native.race"}}
	receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.CopiedCount != 0 || receipt.DeferredCount != 1 {
		t.Fatal(receipt)
	}
	snapshot, err := target.Snapshot(ctx, "native.race")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 42 {
		t.Fatal("live create overwritten", snapshot)
	}
}

func TestNativePendingNameAppearsAfterCandidates(t *testing.T) {
	ctx := context.Background()
	source, _ := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	target, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	name := "native.pending-race"
	createTestMetric(t, source, name)
	var pending atomic.Bool
	target.SetTransferPending(func(string) bool { return pending.Load() })
	candidates, err := target.NativeCandidates(ctx, []string{name})
	if err != nil || !nativeBit(candidates, 0) {
		t.Fatal("initial candidate", candidates, err)
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
	target.nativeTestHook = func(stage string) error {
		if stage == "before-publish" {
			pending.Store(true) // live intake queues a name without creating its catalog yet
		}
		return nil
	}
	request := NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{name}, Candidates: candidates}
	receipt, err := target.NativeImport(ctx, request, &wire)
	if err != nil || receipt.CopiedCount != 0 || receipt.DeferredCount != 1 {
		t.Fatal("pending name published", receipt, err)
	}
	if _, err := target.Metadata(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Fatal("pending name acquired a native catalog", err)
	}
	candidates, err = target.NativeCandidates(ctx, []string{name})
	if err != nil || nativeBit(candidates, 0) {
		t.Fatal("pending name remained a candidate", candidates, err)
	}
}

func TestNativeCrashVisibilityAndReceipt(t *testing.T) {
	for _, stage := range []string{"reserved", "staged", "ingested", "before-publish", "published"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, source)
			createTestMetric(t, source, "native.crash")
			if err := source.UpdateMany(ctx, "native.crash", []Point{{Timestamp: int64(now - 60), Value: 42}}); err != nil {
				t.Fatal(err)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.Write(ctx, []string{"native.crash"}, &wire); err != nil {
				t.Fatal(err)
			}
			fs := vfs.NewStrictMem()
			target, _ := openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			target.nativeTestHook = func(got string) error {
				if got == stage {
					fs.SetIgnoreSyncs(true)
					return errors.New("simulated crash")
				}
				return nil
			}
			request := NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{"native.crash"}}
			if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("crash not injected")
			}
			closeTestStore(t, target)
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			target, _ = openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, target)
			_, visible := target.Metadata(ctx, "native.crash")
			_, receiptErr := target.NativeReceipt(ctx, request)
			if (visible == nil) != (receiptErr == nil) {
				t.Fatal("catalog and receipt visibility diverged", visible, receiptErr)
			}
			if stage == "published" && receiptErr != nil {
				t.Fatal("durable receipt lost", receiptErr)
			}
			receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
			if err != nil {
				t.Fatal("resume", err)
			}
			if receipt.CopiedCount != 1 {
				t.Fatal(receipt)
			}
			snapshot, err := target.Snapshot(ctx, "native.crash")
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 42 {
				t.Fatal(snapshot)
			}
		})
	}
}

func TestNativeSourceDefersQueuedAndFlushedAfterPin(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	names := []string{"native.pending", "native.flushed", "native.stable"}
	for _, name := range names {
		createTestMetric(t, source, name)
		if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
		// Simulate a persister committing and confirming between pin/cache scan.
		if err := source.UpdateMany(ctx, names[1], []Point{{Timestamp: int64(now - 30), Value: 2}}); err != nil {
			return nil, err
		}
		return map[string]struct{}{names[0]: {}}, nil
	})
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, names, &wire); err != nil {
		t.Fatal(err)
	}
	stream, err := NewTransferStream(&wire)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for range stream.Count {
		r, err := stream.Next()
		if err != nil {
			t.Fatal(err)
		}
		seen[r.Config.Name] = r.Version.ID != 0
	}
	if _, _, err := stream.Finish(); err != nil {
		t.Fatal(err)
	}
	if seen[names[0]] || seen[names[1]] || !seen[names[2]] {
		t.Fatal("queued/flush race was not deferred", seen)
	}
}

// A catalog can become readable before its WAL Sync completes. Receipt readers
// and duplicate imports must wait for the original range's durable ack.
func TestNativeReceiptWaitsForPublicationSync(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	createTestMetric(t, source, "native.sync")
	if err := source.UpdateMany(ctx, "native.sync", []Point{{Timestamp: int64(now - 60), Value: 42}}); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, []string{"native.sync"}, &wire); err != nil {
		t.Fatal(err)
	}
	fs := &nativeSyncFS{FS: vfs.NewMem()}
	target, _ := openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	gate := &nativeSyncGate{entered: make(chan struct{}), release: make(chan struct{})}
	target.nativeTestHook = func(stage string) error {
		if stage == "before-publish" {
			fs.gate.Store(gate)
		}
		return nil
	}
	request := NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{"native.sync"}}
	imported := make(chan error, 1)
	go func() { _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); imported <- err }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not reach WAL Sync")
	}
	// Publication is already serialized and visible. The physical sync must
	// not prevent unrelated metric creation or another range's publication.
	// The WAL goroutine can enter Sync before ApplyNoSyncWait returns to
	// the publisher. Wait for lock progress while physical Sync stays gated.
	creationAvailable := make(chan struct{})
	go func() {
		target.mu.Lock()
		target.mu.Unlock()
		close(creationAvailable)
	}()
	select {
	case <-creationAvailable:
	case <-time.After(5 * time.Second):
		close(gate.release)
		t.Fatal("publication held the creation lock across WAL Sync")
	}
	queried := make(chan error, 1)
	go func() { _, err := target.NativeReceipt(ctx, request); queried <- err }()
	duplicate := make(chan error, 1)
	go func() {
		_, err := target.NativeImport(ctx, request, bytes.NewReader([]byte("must not be read")))
		duplicate <- err
	}()
	select {
	case err := <-queried:
		close(gate.release)
		t.Fatalf("receipt returned before durable Sync: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	select {
	case err := <-duplicate:
		close(gate.release)
		t.Fatalf("duplicate returned before durable Sync: %v", err)
	default:
	}
	close(gate.release)
	for _, ch := range []chan error{imported, queried, duplicate} {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeReservationsReleaseCreationLockBeforeSync(t *testing.T) {
	fs := &nativeSyncFS{FS: vfs.NewMem()}
	target, _ := openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	gate := &nativeSyncGate{entered: make(chan struct{}), release: make(chan struct{})}
	fs.gate.Store(gate)
	type result struct {
		intent nativeIntent
		err    error
	}
	done := make(chan result, 2)
	for _, id := range []string{strings.Repeat("1", 64), strings.Repeat("2", 64)} {
		go func() { intent, err := target.reserveNativeIDs(id, 16); done <- result{intent, err} }()
	}
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		close(gate.release)
		t.Fatal("reservation did not reach WAL Sync")
	}
	deadline := time.Now().Add(time.Second)
	for !target.mu.TryLock() {
		if time.Now().After(deadline) {
			close(gate.release)
			t.Fatal("ID reservation held the creation lock across WAL Sync")
		}
		time.Sleep(time.Millisecond)
	}
	target.mu.Unlock()
	select {
	case <-done:
		close(gate.release)
		t.Fatal("reservation acknowledged before durable Sync")
	default:
	}
	close(gate.release)
	first, second := <-done, <-done
	if first.err != nil || second.err != nil || first.intent.First == second.intent.First {
		t.Fatal("concurrent reservations failed or overlap", first, second)
	}
	high, err := target.CatalogHighWatermark(context.Background())
	if err != nil || high != 32 {
		t.Fatal("sequence lost a reservation", high, err)
	}
}

type nativeSyncGate struct {
	entered, release chan struct{}
	once             sync.Once
}
type nativeSyncFS struct {
	vfs.FS
	gate atomic.Pointer[nativeSyncGate]
}

func (fs *nativeSyncFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err == nil && strings.HasSuffix(name, ".log") {
		return nativeSyncFile{File: f, fs: fs}, nil
	}
	return f, err
}

type nativeSyncFile struct {
	vfs.File
	fs *nativeSyncFS
}

func (f nativeSyncFile) Sync() error     { f.wait(); return f.File.Sync() }
func (f nativeSyncFile) SyncData() error { f.wait(); return f.File.SyncData() }
func (f nativeSyncFile) wait() {
	if gate := f.fs.gate.Load(); gate != nil {
		gate.once.Do(func() { close(gate.entered) })
		<-gate.release
	}
}
