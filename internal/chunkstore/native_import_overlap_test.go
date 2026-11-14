package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestNativeConcurrentImportsRetryOverlappingName(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	codec := &parallelCombineCodec{ContinuationCodec: &whisperio.ContinuationCodec{}, started: make(chan string, 64), release: make(chan struct{})}
	target, err := store.Open("/overlap", store.Options{NativeImportConcurrency: 2, NativeCombineWorkers: 1, NativeCombineMemory: 2 << 30, ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: func() time.Time { return time.Unix(1000000, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	target.StopBackground()
	defer target.Close()
	reference := nativeMemoryStore(t, true, true)
	const name = "imports.overlap"
	for _, s := range []*store.Store{target, reference} {
		putTransferPoints(t, s, name, []store.Point{{Timestamp: 999900, Value: 42}})
	}
	done := make(chan error, 2)
	for i := range 2 {
		source := nativeMemoryStore(t, true, true)
		// Distinct donor timestamps in every archive make the two unions
		// commutative. Conflicting donor values intentionally follow arrival
		// order under target-wins; the canary keeps that donor order fixed.
		putTransferPoints(t, source, name, []store.Point{{Timestamp: int64(999720 + 80*i), Value: float64(i + 11)}, {Timestamp: 999900, Value: -1}})
		records, err := source.ExportTransfer(ctx, []string{name})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reference.ImportTransfer(ctx, records); err != nil {
			t.Fatal(err)
		}
		pin, err := source.PinNativeSource()
		if err != nil {
			t.Fatal(err)
		}
		var wire bytes.Buffer
		if err := pin.Write(ctx, []string{name}, &wire); err != nil {
			t.Fatal(err)
		}
		pin.Close()
		request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: fmt.Sprintf("%064x", i+3), Source: fmt.Sprint("donor", i), End: 1, Names: []string{name}, Combine: true}
		go func() {
			r, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
			if err == nil && r.CopiedCount != 1 {
				err = fmt.Errorf("incomplete receipt: %+v", r)
			}
			done <- err
		}()
	}
	for range 2 {
		select {
		case <-codec.started:
		case <-ctx.Done():
			t.Fatal("overlapping imports serialized", ctx.Err())
		}
	}
	for _, s := range []*store.Store{target, reference} {
		putTransferPoints(t, s, name, []store.Point{{Timestamp: 999900, Value: 99}})
	}
	close(codec.release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if target.NativeStats().CombineRetries == 0 {
		t.Fatal("stale preparations did not retry")
	}
	want, err := reference.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("lost donor history or foreground overwrite", err)
	}
	if _, err := target.VerifyTransferFetch(ctx, got); err != nil {
		t.Fatal(err)
	}
}
