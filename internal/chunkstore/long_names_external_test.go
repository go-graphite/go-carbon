package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestLongNamesNativeRoundTrip(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"v6", true, false}, {"v7", true, true}} {
		for _, combine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/combine=%t", mode.name, combine), func(t *testing.T) {
				ctx := context.Background()
				source := nativeMemoryStore(t, mode.continuation, mode.buckets)
				target := nativeMemoryStore(t, mode.continuation, mode.buckets)
				var names []string
				for _, size := range []int{300, 4096} {
					name := "long." + strings.Repeat("x", size) + ".value"
					names = append(names, name)
					putTransferPoints(t, source, name, []store.Point{{Timestamp: 999800, Value: 42}, {Timestamp: 999820, Value: 7}})
				}
				// The verified transfer fallback must use the same name budget.
				records, err := source.ExportTransfer(ctx, names)
				if err != nil {
					t.Fatal(err)
				}
				verified := nativeMemoryStore(t, mode.continuation, mode.buckets)
				proofs, err := verified.ImportTransfer(ctx, records)
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := store.EncodeTransferProofs(proofs)
				if err != nil {
					t.Fatal(err)
				}
				proofs, err = store.ReadTransferProofs(bytes.NewReader(encoded))
				if err != nil {
					t.Fatal(err)
				}
				if err := verified.VerifyTransfer(ctx, proofs); err != nil {
					t.Fatal(err)
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
				// Exercise both the buffered and streaming transfer decoders.
				if _, err := store.ReadTransfer(bytes.NewReader(wire.Bytes()), 16<<20); err != nil {
					t.Fatal(err)
				}
				request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: combine}
				receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				if receipt.CopiedCount != len(names) || receipt.DeferredCount != 0 {
					t.Fatal(receipt)
				}
				for _, name := range names {
					want, err := source.Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					got, err := target.Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					if got.Metadata.Name != name {
						t.Fatal("metric key changed")
					}
					if err := store.EqualSnapshots(want, got); err != nil {
						t.Fatal(err)
					}
				}
				if n, err := target.PurgeInvalidNames(ctx); err != nil || n != 0 {
					t.Fatalf("valid long names purged: %d, %v", n, err)
				}
				if again, err := target.NativeImport(ctx, request, nil); err != nil || again.Digest != receipt.Digest {
					t.Fatalf("receipt retry failed: %v", err)
				}
			})
		}
	}
}
