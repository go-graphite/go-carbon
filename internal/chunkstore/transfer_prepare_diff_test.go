package chunkstore_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Compare the complete encoded store as well as decoded point bits against
// group-first preparation, including absent names, overlaps and repeated imports.
func TestTransferPartitionFirstMatchesGroup(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"continuation", true, false}, {"buckets", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			source := transferStore(t, mode.continuation, mode.buckets)
			names := make([]string, 32)
			values := []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.Float64frombits(0x3ff0000000000001), math.Inf(1), math.Inf(-1)}
			for i := range names {
				names[i] = fmt.Sprintf("prepare.diff.%02d", i)
				putTransferPoints(t, source, names[i], []store.Point{{Timestamp: 999820, Value: values[i%len(values)]}, {Timestamp: 999880, Value: 3}})
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			var targets []*store.Store
			for _, group := range []bool{true, false} {
				target := transferStore(t, mode.continuation, mode.buckets)
				target.SetTransferPrepareGroupForTest(group)
				for i, name := range names[:16] {
					putTransferPoints(t, target, name, []store.Point{{Timestamp: 999880, Value: values[(i+1)%len(values)]}, {Timestamp: 999940, Value: 7}})
				}
				for range 2 {
					proofs, err := target.ImportTransfer(ctx, records)
					if err != nil {
						t.Fatal(err)
					}
					if err := target.VerifyTransfer(ctx, proofs); err != nil {
						t.Fatal(err)
					}
				}
				targets = append(targets, target)
			}
			for _, name := range names {
				want, err := targets[0].Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := targets[1].Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(want, got); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			}
			want, err := targets[0].RawKeyValuesForTest()
			if err != nil {
				t.Fatal(err)
			}
			got, err := targets[1].RawKeyValuesForTest()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatal("encoded store differs from group-first preparation")
			}
		})
	}
}
