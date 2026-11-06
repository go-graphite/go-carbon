package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type parallelCombineCodec struct {
	*whisperio.ContinuationCodec
	started  chan string
	release  chan struct{}
	failName string
	active   atomic.Int32
}

var errParallelPrepare = errors.New("injected name preparation failure")

func (c *parallelCombineCodec) CombineNative(ctx context.Context, a, b store.ContinuationIO, m store.Metadata, clock func() time.Time) ([]store.RelayoutKV, store.NativeCombineClass, bool, error) {
	c.active.Add(1)
	defer c.active.Add(-1)
	select {
	case c.started <- m.Name:
	case <-ctx.Done():
		return nil, store.NativeCombineClass{}, false, ctx.Err()
	}
	if m.Name == c.failName {
		return nil, store.NativeCombineClass{}, false, errParallelPrepare
	}
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, store.NativeCombineClass{}, false, ctx.Err()
	}
	return c.ContinuationCodec.CombineNative(ctx, a, b, m, clock)
}

func TestNativeCombinePreparesIndependentNamesConcurrently(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source := nativeMemoryStore(t, true, true)
	reference := nativeMemoryStore(t, true, true)
	codec := &parallelCombineCodec{ContinuationCodec: &whisperio.ContinuationCodec{}, started: make(chan string, 8), release: make(chan struct{})}
	target, err := store.Open("/parallel", store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: func() time.Time { return time.Unix(1000000, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	target.StopBackground()
	defer target.Close()
	var names []string
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("parallel.%02d", i)
		names = append(names, name)
		putTransferPoints(t, source, name, []store.Point{{Timestamp: 999800, Value: float64(i)}})
		for _, s := range []*store.Store{target, reference} {
			putTransferPoints(t, s, name, []store.Point{{Timestamp: 999900, Value: float64(-i)}})
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
	rawWire := bytes.Clone(wire.Bytes())
	request := store.NativeRange{Operation: strings.Repeat("7", 64), Plan: strings.Repeat("8", 64), Stream: strings.Repeat("9", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: true}
	done := make(chan error, 1)
	go func() { _, err := target.NativeImport(ctx, request, bytes.NewReader(rawWire)); done <- err }()
	first, second := "", ""
	select {
	case first = <-codec.started:
	case <-ctx.Done():
	}
	select {
	case second = <-codec.started:
	case <-time.After(2 * time.Second):
	}
	close(codec.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if first == "" || second == "" || first == second {
		t.Fatalf("preparation remained serial: %q %q", first, second)
	}
	records, err := source.ExportTransfer(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
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
	}
	// A later job failure must cancel a blocked earlier job and join every
	// worker before returning, leaving all live catalog revisions unchanged.
	before := make(map[string]store.Metadata)
	for _, name := range names {
		before[name], err = target.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
	}
	codec.failName, codec.release = names[1], make(chan struct{})
	request.Operation = strings.Repeat("6", 64)
	if _, err := target.NativeImport(ctx, request, bytes.NewReader(rawWire)); !errors.Is(err, errParallelPrepare) {
		t.Fatal("failed preparation did not cancel workers", err)
	}
	if active := codec.active.Load(); active != 0 {
		t.Fatal("worker outlived import", active)
	}
	for _, name := range names {
		after, err := target.Metadata(ctx, name)
		if err != nil || after.ID != before[name].ID || after.Revision != before[name].Revision {
			t.Fatal("failed preparation published a name", name, after, err)
		}
	}
	codec.failName, codec.started = "", make(chan string, len(names))
	close(codec.release)
	if receipt, err := target.NativeImport(ctx, request, nil); err != nil || receipt.CopiedCount != len(names) {
		t.Fatal("source-free resume after failed parallel preparation", receipt, err)
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
			t.Fatal("resumed", name, err)
		}
	}
}
