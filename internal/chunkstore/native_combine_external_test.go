package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeCombineDifferential(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"v6", true, false}, {"v7", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			source := nativeMemoryStore(t, mode.continuation, mode.buckets)
			target := nativeMemoryStore(t, mode.continuation, mode.buckets)
			reference := nativeMemoryStore(t, mode.continuation, mode.buckets)
			var names []string
			for i := range 65 {
				name := fmt.Sprintf("native.combine.%03d", 64-i)
				names = append(names, name)
				putTransferPoints(t, source, name, []store.Point{{Timestamp: 999800, Value: math.Copysign(0, -1)}, {Timestamp: 999820, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 999880, Value: math.Inf(1)}, {Timestamp: 999890, Value: float64(i)}})
				if i%3 != 0 {
					for _, s := range []*store.Store{target, reference} {
						putTransferPoints(t, s, name, []store.Point{{Timestamp: 999880, Value: math.Inf(-1)}, {Timestamp: 999910, Value: 7}, {Timestamp: 999920, Value: math.Copysign(0, -1)}})
					}
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
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reference.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: true}
			receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			if receipt.CopiedCount != len(names) || receipt.DeferredCount != 0 {
				t.Fatal(receipt)
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
					t.Fatalf("%s: %v", name, err)
				}
			}
			// A lost acknowledgement cannot cause a second merge after newer
			// foreground points have arrived, even with a completely invalid body.
			putTransferPoints(t, target, names[0], []store.Point{{Timestamp: 999930, Value: 99}})
			before, _ := target.Metadata(ctx, names[0])
			if _, err := target.NativeImport(ctx, request, strings.NewReader("not a stream")); err != nil {
				t.Fatal(err)
			}
			after, _ := target.Metadata(ctx, names[0])
			if before.ID != after.ID || before.Generation != after.Generation || before.Revision != after.Revision {
				t.Fatal("durable retry changed foreground history", before, after)
			}
		})
	}
}
