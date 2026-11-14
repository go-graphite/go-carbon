package chunkstore_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Golden digests are produced by the pre-buffer-reuse import reader. Compare
// float bits, archive order and proof bytes rather than floating-point equality.
func TestImportDecodeCurrentReaderDifferential(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{
		{"exact", false, false}, {"continuation", true, false}, {"bucketed", true, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			source := transferStore(t, mode.continuation, mode.buckets)
			target := transferStore(t, mode.continuation, mode.buckets)
			names := make([]string, 12)
			specials := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234}
			for i := range names {
				names[i] = fmt.Sprintf("decode.oracle.%02d", i)
				var pts []store.Point
				for j := 0; j < 350; j++ {
					pts = append(pts, store.Point{Timestamp: 996000 + int64(j*10), Value: math.Float64frombits(specials[j%len(specials)])})
				}
				putDecodePoints(t, source, names[i], pts)
				if i%3 != 0 {
					putDecodePoints(t, target, names[i], pts[150:])
				}
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			proofs, err := target.ImportTransfer(ctx, records)
			if err != nil {
				t.Fatal(err)
			}
			if err := target.VerifyTransfer(ctx, proofs); err != nil {
				t.Fatal(err)
			}
			var bits []uint64
			for _, name := range names {
				snap, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				for _, a := range snap.Archives {
					bits = append(bits, uint64(a.Retention.Step), uint64(a.Retention.Count), uint64(len(a.Points)))
					for _, p := range a.Points {
						bits = append(bits, uint64(p.Timestamp), math.Float64bits(p.Value))
					}
				}
			}
			raw, err := json.Marshal(struct {
				Proofs []store.TransferProof
				Bits   []uint64
			}{proofs, bits})
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]string{
				"exact":        "f9b9c79278d49eb540e4749e64e02c547ad1fbfb2d07c078eab5f397b86261d9",
				"continuation": "ecd89ae08864dba9dba91fb731595187dc877f46ad627f2771339fb32a1d81d5",
				"bucketed":     "d52d4e7d5eec03b9a4c0d9ee27dd0676b5f30c939d5d0d782fdb1409676b65d5",
			}[mode.name]
			got := fmt.Sprintf("%x", sha256.Sum256(raw))
			if got != want {
				t.Fatalf("current reader differential: got %s want %s", got, want)
			}
		})
	}
}

func putDecodePoints(t *testing.T, s *store.Store, name string, points []store.Point) {
	t.Helper()
	_, err := s.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 1024}}, AggregationMethod: store.Last})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMany(context.Background(), name, points); err != nil {
		t.Fatal(err)
	}
}
