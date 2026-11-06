package chunkstore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeCombineReservationsReleaseCreationLockBeforeSync(t *testing.T) {
	fs := &nativeSyncFS{FS: vfs.NewMem()}
	target, _ := openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	gate := &nativeSyncGate{entered: make(chan struct{}), release: make(chan struct{})}
	fs.gate.Store(gate)
	type result struct {
		first uint64
		err   error
	}
	done := make(chan result, 2)
	for _, name := range []string{"combine.reserve.a", "combine.reserve.b"} {
		request := combineTestRange([]string{name})
		id, namesHash, err := request.Identity()
		if err != nil {
			t.Fatal(err)
		}
		intent := nativeCombineIntent{Request: request, Receipt: NativeReceipt{Identity: id, Count: 1, NamesSHA256: namesHash}}
		go func() { first, err := target.reserveNativeCombineIDs(&intent, 16); done <- result{first, err} }()
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
			t.Fatal("combine ID reservation held creation across WAL Sync")
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
	a, b := <-done, <-done
	if a.err != nil || b.err != nil || a.first == b.first {
		t.Fatal("concurrent reservations failed or overlap", a, b)
	}
	if high, err := target.CatalogHighWatermark(context.Background()); err != nil || high != 32 {
		t.Fatal("sequence lost a reservation", high, err)
	}
}

func TestNativeCombineReceiptWaitsForSyncWithoutWriterLocks(t *testing.T) {
	ctx := context.Background()
	source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	name := "combine.receipt.sync"
	createTestMetric(t, source, name)
	if err := source.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
		t.Fatal(err)
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
	fs := &nativeSyncFS{FS: vfs.NewMem()}
	target, _ := openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, target)
	createTestMetric(t, target, name)
	gate := &nativeSyncGate{entered: make(chan struct{}), release: make(chan struct{})}
	target.nativeTestHook = func(stage string) error {
		if stage == "combine-receipt" {
			fs.gate.Store(gate)
		}
		return nil
	}
	request := combineTestRange([]string{name})
	imported := make(chan error, 1)
	go func() { _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); imported <- err }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		close(gate.release)
		t.Fatal("receipt did not reach WAL Sync")
	}
	if !target.mu.TryLock() {
		close(gate.release)
		t.Fatal("receipt held creation across WAL Sync")
	}
	target.mu.Unlock()
	stripe := &target.metricMu[target.metricStripe(name)]
	if !stripe.TryLock() {
		close(gate.release)
		t.Fatal("receipt held writer stripe across WAL Sync")
	}
	stripe.Unlock()
	queried, duplicate := make(chan error, 1), make(chan error, 1)
	go func() { _, err := target.NativeReceipt(ctx, request); queried <- err }()
	go func() {
		_, err := target.NativeImport(ctx, request, bytes.NewReader([]byte("must not be read")))
		duplicate <- err
	}()
	select {
	case err := <-queried:
		close(gate.release)
		t.Fatalf("receipt returned before durable Sync: %v", err)
	case err := <-duplicate:
		close(gate.release)
		t.Fatalf("duplicate returned before durable Sync: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(gate.release)
	for _, ch := range []chan error{imported, queried, duplicate} {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
}
