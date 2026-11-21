package v2core

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Ten original apply batches in one seal must have exactly the same stored
// winners as ten independent seals, including corrections and nonfinite bits.
func TestSealIntervalStoredWinnersDifferential(t *testing.T) {
	t.Parallel()
	for method := sealrollup.Average; method <= sealrollup.First; method++ {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			_, policy := rollupTestPolicy(t, "cadence.metric", "1s:1h,1m:1d,1h:1y", method, .1)
			cfg := coreTestConfig()
			cfg.Shards = 1
			cfg.Producers = 1
			cfg.Rollups = true
			cfg.MaxRows = 4096
			roots := []string{t.TempDir(), t.TempDir()}
			es := make([]*coreEngine, 2)
			for i := range es {
				cfg.SealInterval = time.Duration(1+9*i) * time.Second
				e, err := openCore(roots[i], cfg)
				if err != nil {
					t.Fatal(err)
				}
				es[i] = e
			}
			defer func() {
				for _, e := range es {
					_ = e.close()
				}
			}()
			now := int64(1794700800)
			values := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1, math.Float64bits(7)}
			for epoch := int64(0); epoch < 40; epoch++ {
				clock := now + epoch*60
				var rows []row
				for j := int64(0); j < 60; j++ {
					rows = append(rows, row{ID: 1, Time: clock - 60 + j, Bits: values[(j+epoch)%int64(len(values))], Seq: uint64(j + 1)})
				}
				for _, age := range []int64{120, 1800, 3500, 7200, 18000} {
					rows = append(rows, row{ID: 1, Time: clock - age, Bits: values[epoch%int64(len(values))], Seq: 101})
				}
				order := uint64(epoch + 1)
				for i, e := range es {
					if err := e.submitBatches(0, []coreBatch{{ID: 1, Policy: policy, Order: order, Clock: clock, Points: rows}}, order, i == 0 || epoch%10 == 9); err != nil {
						t.Fatal(err)
					}
				}
			}
			get := func(e *coreEngine) []sealrollup.Record {
				pin := e.owners[0].db.NewSnapshot()
				defer pin.Close()
				index, err := NewRollupSnapshot(context.Background(), pin, nil, nil, nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				records, err := index.Records(context.Background(), "cadence.metric")
				if err != nil {
					t.Fatal(err)
				}
				slices.SortFunc(records, func(a, b sealrollup.Record) int {
					if a.Archive != b.Archive {
						return a.Archive - b.Archive
					}
					if a.Slot < b.Slot {
						return -1
					}
					if a.Slot > b.Slot {
						return 1
					}
					return 0
				})
				// Correction records choose the physical correction stream based on
				// prior seal boundaries. Compare the complete logical winner, including
				// all value bits/order/sequence, independently of that placement hint.
				for i := range records {
					records[i].Correction = false
				}
				return records
			}
			want, got := get(es[0]), get(es[1])
			if !reflect.DeepEqual(got, want) {
				for i := range got {
					if i < len(want) && got[i] != want[i] {
						t.Fatalf("stored winner%d differs: got%+v want%+v", i, got[i], want[i])
					}
				}
				t.Fatalf("stored winners differ: got%d want%d", len(got), len(want))
			}
		})
	}
}
