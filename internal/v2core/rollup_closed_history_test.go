package v2core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// This fixture deliberately uses only pre-history-cache APIs so it can also run
// against an overlay of the unchanged reducer runtime.
func TestClosedHistoryLogicalDifferential(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	closedHistoryLogicalDifferential(t)
}

// The eager reducer checks each epoch; the original fixed hashes independently
// guard resolved timestamps and floating-point bits across restart cuts.
func closedHistoryLogicalDifferential(t *testing.T) {
	t.Helper()
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for method := sealrollup.Average; method <= sealrollup.First; method++ {
		for _, schema := range []string{"1s:1h,1m:1d,1h:1y", "10s:6h,5m:7d,1h:1y,1d:10y"} {
			t.Run(fmt.Sprintf("%d/%s", method, schema), func(t *testing.T) {
				// Subtests own their engines; the parent's environment stays set
				// until all of them finish.
				t.Parallel()
				_, policy := rollupTestPolicy(t, "history.metric", schema, method, .1)
				cfg := coreTestConfig()
				cfg.Shards = 1
				cfg.Producers = 1
				cfg.Rollups = true
				cfg.MaxRows = 1024

				root := t.TempDir()
				e, err := Open(root, cfg, Hooks{Now: func() time.Time { return time.Unix(1, 0) }})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if e != nil {
						_ = e.Close()
					}
				}()
				h := sha256.New()
				type logicalKey struct {
					archive int
					slot    int64
				}
				type logicalValue struct {
					time int64
					bits uint64
				}
				state := map[logicalKey]sealrollup.Sample{}
				var frontier sealrollup.Frontier
				var definition RollupPolicy
				if err := json.Unmarshal(policy, &definition); err != nil {
					t.Fatal(err)
				}
				prepared, err := sealrollup.Prepare(definition.Config)
				if err != nil {
					t.Fatal(err)
				}
				var scratch [8]byte
				put := func(v uint64) { binary.LittleEndian.PutUint64(scratch[:], v); h.Write(scratch[:]) }
				now := int64(1794700800)
				var order uint64
				bits := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1, math.Float64bits(7)}
				for epoch := int64(0); epoch < 40; epoch++ {
					now += 600
					order++
					var rows []Row
					for j := int64(0); j < 60; j++ {
						rows = append(rows, Row{ID: 1, Time: now - 600 + j*10, Bits: bits[(j+epoch)%int64(len(bits))], Seq: uint64(j + 1)})
					}
					for _, age := range []int64{120, 1800, 3500, 7200, 18000, 100000} {
						stamp := now - age
						rows = append(rows, Row{ID: 1, Time: stamp, Bits: math.Float64bits(float64(epoch)), Seq: 100}, Row{ID: 1, Time: stamp, Bits: bits[epoch%int64(len(bits))], Seq: 101})
					}
					if err = e.AppendBatches(0, []Batch{{ID: 1, Policy: policy, Order: order, Clock: now, Points: rows}}, order, true); err != nil {
						t.Fatal(err)
					}
					orderProofObserve(t, t.Name(), e)
					snap := e.DBs()[0].NewSnapshot()
					index, err := NewRollupSnapshot(context.Background(), snap, nil, nil, nil, 0)
					if err != nil {
						t.Fatal(err)
					}
					rs, err := index.Records(context.Background(), "history.metric")
					if err != nil {
						t.Fatal(err)
					}
					sort.Slice(rs, func(i, j int) bool {
						if rs[i].Archive != rs[j].Archive {
							return rs[i].Archive < rs[j].Archive
						}
						return rs[i].Slot < rs[j].Slot
					})

					samples := make([]sealrollup.Sample, len(rows))
					for i, r := range rows {
						samples[i] = sealrollup.Sample{Time: r.Time, Bits: r.Bits, Sequence: r.Seq}
					}
					plan, err := prepared.Plan(frontier, func(a int, physical int64) (sealrollup.Sample, bool) {
						r, ok := state[logicalKey{a, physical}]
						return r, ok
					}, nil, []sealrollup.Batch{{Order: order, Now: now, Points: samples}})
					if err != nil {
						t.Fatal(err)
					}
					for _, r := range plan.Records {
						state[logicalKey{r.Archive, r.Slot}] = r.Sample
					}
					frontier = plan.Next
					actual := map[logicalKey]logicalValue{}
					expected := map[logicalKey]logicalValue{}
					for k, p := range state {
						expected[k] = logicalValue{p.Time, p.Bits}
					}
					for _, r := range rs {
						actual[logicalKey{r.Archive, r.Slot}] = logicalValue{r.Sample.Time, r.Sample.Bits}
					}
					if !reflect.DeepEqual(actual, expected) {
						for k, w := range expected {
							if g, ok := actual[k]; !ok || g != w {
								t.Errorf("epoch%d slot%+v got%+v want%+v", epoch, k, g, w)
							}
						}
						for k, g := range actual {
							if _, ok := expected[k]; !ok {
								t.Errorf("epoch%d extra%+v=%+v", epoch, k, g)
							}
						}
						t.FailNow()
					}
					for _, r := range rs {
						put(uint64(r.Archive))
						put(uint64(r.Slot))
						put(uint64(r.Sample.Time))
						put(r.Sample.Bits)
					}
					snap.Close()
					if epoch == 17 || epoch == 31 {
						if err = e.Close(); err != nil {
							t.Fatal(err)
						}
						e, err = Open(root, cfg, Hooks{Now: func() time.Time { return time.Unix(1, 0) }})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				digest := fmt.Sprintf("%x", h.Sum(nil))
				want := map[string]string{"1|1s:1h,1m:1d,1h:1y": "77d2e2318b1bb77f9ae712432710481d61dd4bbf9eec2240ad86a87040ffb35a", "1|10s:6h,5m:7d,1h:1y,1d:10y": "ecfb0b3c3240c4d017c917a28583f0612b352b36ed8f3b57199e07edc9ec649e", "2|1s:1h,1m:1d,1h:1y": "77d2e2318b1bb77f9ae712432710481d61dd4bbf9eec2240ad86a87040ffb35a", "2|10s:6h,5m:7d,1h:1y,1d:10y": "ecfb0b3c3240c4d017c917a28583f0612b352b36ed8f3b57199e07edc9ec649e", "3|1s:1h,1m:1d,1h:1y": "e6229811686773aaf5d6b15df84f61e522b117c90d1cea4efe151b7d4b050d0c", "3|10s:6h,5m:7d,1h:1y,1d:10y": "4b1b0f4257117339a6dfb9deae0a23f741e1d9d5c8ec4355e30494f9b0344bab", "4|1s:1h,1m:1d,1h:1y": "ac108d2434a770397318f833fd63cfb149d24e5c66601353c90857d3b821d875", "4|10s:6h,5m:7d,1h:1y,1d:10y": "aba854fb9be683c01cd1358367e30db0be20e0fcd8fa3c02cf227c18cd9a8cf3", "5|1s:1h,1m:1d,1h:1y": "ccd5c1e40f7d978a69d0da353ecd81cfd9879bfe61d484cd2fd7d859641598ca", "5|10s:6h,5m:7d,1h:1y,1d:10y": "16412db176b5ac5daf90a14c3cafda06c43732f1eb4c9a62f8d89011765c129b", "6|1s:1h,1m:1d,1h:1y": "b6005cf522ab2b6ad9084ec808ef6f39dd750ce0432c0d66de758be55f93a65e", "6|10s:6h,5m:7d,1h:1y,1d:10y": "f5141ea80a366a27aac56e428c7514ff94c657339bf51affbf683937d28bc39e"}[fmt.Sprintf("%d|%s", method, schema)]
				if digest != want {
					t.Fatalf("resolved history digest got %s want %s", digest, want)
				}
				t.Logf("history digest %s", digest)
			})
		}
	}
}
