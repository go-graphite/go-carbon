package v2daemon

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/points"
)

// Compare the former per-producer seal path with the threshold-driven TCP
// adapter, including duplicate timestamps, out-of-order points and IEEE bits.
func TestApplicationOwnerThresholdDifferential(t *testing.T) {
	type image struct {
		records map[string][]sealrollup.Delta
		reads   [][]uint64
	}
	var images []image
	for _, eager := range []bool{true, false} {
		func() {
			r, err := v2read.New(2, 16<<20, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			r.Identities = &v2core.PolicyCatalog{}
			s, err := Open(t.TempDir(), Config{Layer: 3, Producers: 3, Shards: 2, MaxCoreRows: 4096, GroupInterval: time.Hour, Hooks: v2core.Hooks{Identities: r.Identities, PublishBatches: r.Publish}})
			if err != nil {
				r.Close()
				t.Fatal(err)
			}
			defer s.Close()
			defer r.Close()
			resolve := func(name string) ([]byte, error) {
				return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
			}
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
			owners := make([]*applicationOwner, 3)
			for i, p := range s.Producers {
				owners[i] = &applicationOwner{p: p, resolve: resolve}
			}
			before := s.Core.Observe().Seals
			for round := range 6 {
				for i, o := range owners {
					name := fmt.Sprintf("threshold.%d", i)
					data := []points.Point{{Timestamp: 1000 + int64(round%3), Value: math.Float64frombits(bits[round])}, {Timestamp: 999, Value: float64(round) + .25}}
					if err = o.Append([]string{name, name}, data, nil, 1010); err != nil {
						t.Fatal(err)
					}
					if eager {
						err = o.p.Seal()
					} else {
						err = o.Seal()
					}
					if err != nil {
						t.Fatal(err)
					}
					if err = o.Drain(); err != nil {
						t.Fatal(err)
					}
					if eager {
						if err = s.Core.AppendBatches(i, nil, o.p.cursor, true); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if !eager && s.Core.Observe().Seals != before {
				t.Fatal("producer ticks forced small owner seals")
			}
			for i, o := range owners {
				if err = s.Core.AppendBatches(i, nil, o.p.cursor, true); err != nil {
					t.Fatal(err)
				}
			}
			im := image{records: make(map[string][]sealrollup.Delta)}
			for _, db := range s.Core.DBs() {
				snap := db.NewSnapshot()
				err = v2core.ScanRollups(snap, func(p v2core.RollupPolicy, _ v2core.RollupMetadata, records []sealrollup.Record) error {
					for _, record := range records {
						im.records[string(p.Name)] = append(im.records[string(p.Name)], record.Delta)
					}
					return nil
				})
				snap.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(im.records) != len(owners) {
				t.Fatal("missing stored point records", len(im.records))
			}
			for _, records := range im.records {
				sort.Slice(records, func(i, j int) bool {
					a, b := records[i], records[j]
					if a.Archive != b.Archive {
						return a.Archive < b.Archive
					}
					return a.Slot < b.Slot
				})
			}
			for i := range owners {
				var values []uint64
				err = r.FetchMany(context.Background(), []string{fmt.Sprintf("threshold.%d", i)}, 990, 1010, 1010, func(_ string, v *v2read.Series) error {
					for _, x := range v.Values {
						values = append(values, math.Float64bits(x))
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				im.reads = append(im.reads, values)
			}
			images = append(images, im)
		}()
	}
	if !reflect.DeepEqual(images[0], images[1]) {
		t.Fatalf("batching changed stored point bits or reads: %+v / %+v", images[0], images[1])
	}
}

func TestApplicationOwnerExpiryDrainFences(t *testing.T) {
	s, err := Open(t.TempDir(), Config{Layer: 3, Producers: 1, Shards: 1, MaxCoreRows: 4096, GroupInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o := &applicationOwner{p: s.Producers[0], fenceDrain: true, resolve: func(name string) ([]byte, error) {
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}}}})
	}}
	if err = o.Append([]string{"expiry.fenced"}, []points.Point{{Timestamp: 1000, Value: 1}}, nil, 1001); err != nil {
		t.Fatal(err)
	}
	if err = o.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = o.Drain(); err != nil {
		t.Fatal(err)
	}
	if got := s.Core.DurableFrontiers()[0][0]; got != o.p.cursor {
		t.Fatal("expiry reservation could release before frontier", got, o.p.cursor)
	}
}
