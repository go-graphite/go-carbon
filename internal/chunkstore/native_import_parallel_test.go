package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Two imports must actually enter preparation together. Cancel one while the
// other is pinned, write foreground data, then resume without the donor. Check
// every archive against the serial reference, and recover both receipts after
// reopen. This exercises ID allocation, revision retries and intent isolation.
func TestNativeConcurrentImportsCancelResumeAndReopen(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	source := nativeMemoryStore(t, true, true)
	reference := nativeMemoryStore(t, true, true)
	codec := &parallelCombineCodec{ContinuationCodec: &whisperio.ContinuationCodec{}, started: make(chan string, 64), release: make(chan struct{})}
	fs := vfs.NewMem()
	opts := store.Options{NativeImportConcurrency: 2, NativeCombineWorkers: 1, NativeCombineMemory: 2 << 30, ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: func() time.Time { return time.Unix(1000000, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(fs)
	target, err := store.Open("/parallel-import", opts)
	if err != nil {
		t.Fatal(err)
	}
	target.StopBackground()
	defer func() {
		if target != nil {
			target.Close()
		}
	}()
	names := []string{"imports.a", "imports.b", "imports.c", "imports.d"}
	for _, name := range names {
		putTransferPoints(t, source, name, []store.Point{{Timestamp: 999800, Value: 11}, {Timestamp: 999900, Value: 12}})
		for _, s := range []*store.Store{target, reference} {
			putTransferPoints(t, s, name, []store.Point{{Timestamp: 999900, Value: 42}})
		}
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var requests [2]store.NativeRange
	var done [2]chan error
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := range requests {
		group := names[2*i : 2*i+2]
		var wire bytes.Buffer
		if err := pin.Write(ctx, group, &wire); err != nil {
			t.Fatal(err)
		}
		requests[i] = store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: fmt.Sprintf("%064x", i+3), Source: "donor", End: 2, Names: group, Combine: true}
		done[i] = make(chan error, 1)
		work := ctx
		if i == 0 {
			work = cancelled
		}
		go func(i int, raw []byte, work context.Context) {
			_, err := target.NativeImport(work, requests[i], bytes.NewReader(raw))
			done[i] <- err
		}(i, bytes.Clone(wire.Bytes()), work)
	}
	for range requests {
		select {
		case <-codec.started:
		case <-ctx.Done():
			t.Fatal("imports serialized", ctx.Err())
		}
	}
	cancel()
	if err := <-done[0]; !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled import", err)
	}
	for _, s := range []*store.Store{target, reference} {
		for _, name := range []string{names[0], names[2]} {
			putTransferPoints(t, s, name, []store.Point{{Timestamp: 999900, Value: 99}})
		}
	}
	close(codec.release)
	if err := <-done[1]; err != nil {
		t.Fatal("independent import", err)
	}
	records, err := source.ExportTransfer(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target = nil
	opts.Continuation = &whisperio.ContinuationCodec{}
	target, err = store.Open("/parallel-import", opts)
	if err != nil {
		t.Fatal(err)
	}
	target.StopBackground()
	if receipt, err := target.NativeImport(ctx, requests[0], nil); err != nil || receipt.CopiedCount != 2 {
		t.Fatal("source-free resume", receipt, err)
	}
	for _, request := range requests {
		if receipt, err := target.NativeImport(ctx, request, nil); err != nil || receipt.CopiedCount != 2 {
			t.Fatal("receipt after reopen", receipt, err)
		}
	}
	for _, name := range names {
		want, err := reference.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal(name, err)
		}
		if _, err := target.VerifyTransferFetch(ctx, got); err != nil {
			t.Fatal(name, err)
		}
	}
}
