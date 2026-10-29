package chunkstore_test

import (
	"context"
	"fmt"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestTransferMaintainsCatalogAndSidecarInventory(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			ctx := context.Background()
			source := transferStore(t, true, buckets, false)
			target := transferStore(t, true, buckets, false)
			names := []string{"move.delete", "move.retire"}
			for _, name := range names {
				var points []store.Point
				for ts := int64(995000); ts < 999900; ts += 10 {
					points = append(points, store.Point{Timestamp: ts, Value: float64(ts % 91)})
				}
				putTransferPoints(t, source, name, points)
				putTransferPoints(t, source, name, []store.Point{{Timestamp: 996000, Value: 900}})
			}
			inventory := source.Sidecars()
			if !inventory.Ready || inventory.Count != 2 || inventory.Points == 0 {
				t.Fatalf("fixture has no sidecars: %+v", inventory)
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			_, cursor, _, err := target.CatalogChanges(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := target.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			if got := target.Sidecars(); got != inventory {
				t.Fatalf("imported inventory: %+v want %+v", got, inventory)
			}
			changes, cursor, reset, err := target.CatalogChanges(ctx, cursor)
			if err != nil || reset || len(changes) != 2 {
				t.Fatalf("import catalog changes: %v reset=%v %v", changes, reset, err)
			}
			// An overlapping import restores authoritative archives without a sidecar.
			if _, err := target.ImportTransfer(ctx, records[:1]); err != nil {
				t.Fatal(err)
			}
			if got := target.Sidecars(); got.Count != 1 || got.Points == 0 {
				t.Fatal(got)
			}
			changes, _, reset, err = target.CatalogChanges(ctx, cursor)
			if err != nil || reset || len(changes) != 1 || changes[0].Name != names[0] {
				t.Fatalf("overlap catalog changes: %v reset=%v %v", changes, reset, err)
			}
			_, cursor, _, err = source.CatalogChanges(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := source.DeleteTransfer(ctx, []store.TransferVersion{records[0].Version}); err != nil {
				t.Fatal(err)
			}
			if got := source.Sidecars(); got.Count != 1 {
				t.Fatal(got)
			}
			if _, cursor, reset, err = source.CatalogChanges(ctx, cursor); err != nil || !reset {
				t.Fatalf("delete failed to invalidate catalog: reset=%v %v", reset, err)
			}
			if err := source.StageTransferDelete(ctx, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", []store.TransferVersion{records[1].Version}); err != nil {
				t.Fatal(err)
			}
			if n, err := source.RetireTransferredStore(ctx, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"); err != nil || n != 1 {
				t.Fatalf("retire: %d %v", n, err)
			}
			if got := source.Sidecars(); !got.Ready || got.Count != 0 || got.Points != 0 || got.Oldest != 0 {
				t.Fatal(got)
			}
			if _, _, reset, err := source.CatalogChanges(ctx, cursor); err != nil || !reset {
				t.Fatalf("retire failed to invalidate catalog: reset=%v %v", reset, err)
			}
		})
	}
}
