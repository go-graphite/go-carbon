package chunkstore_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestTransferViewPreparationBitExact(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"continuation", true, false}, {"bucketed", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			source := transferStore(t, mode.continuation, mode.buckets)
			target := transferStore(t, mode.continuation, mode.buckets)
			rng := rand.New(rand.NewSource(8144))
			var names []string
			specials := []uint64{0, 1, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234}
			for i := 0; i < 40; i++ {
				name := fmt.Sprintf("view.differential.%03d", i)
				names = append(names, name)
				var points []store.Point
				for j := 0; j < 80; j++ {
					bits := rng.Uint64()
					if j < len(specials) {
						bits = specials[j]
					}
					points = append(points, store.Point{Timestamp: 999000 + int64(j*10), Value: math.Float64frombits(bits)})
				}
				putTransferPoints(t, source, name, points)
				if i%4 != 0 {
					putTransferPoints(t, target, name, points[20:])
				}
			}
			records, err := source.ExportTransfer(context.Background(), names)
			if err != nil {
				t.Fatal(err)
			}
			store.CheckTransferViewForTest(t, target, records)
		})
	}
}
