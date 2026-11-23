package v2core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"math/rand"
	"testing"
	"time"
)

// The corpus is the seal-plan digest corpus. The oracle is the unmodified
// parent revision's render surface: every occupied slot, timestamp and float bit.
// Physical plans change when seals stop materializing windows.
var sealRenderGolden = map[string]string{
	"seed1/p1": "2d6426eee258f47bb7159805e76396d79987a7c45927421ad071069a8e351777",
	"seed1/p4": "2d6426eee258f47bb7159805e76396d79987a7c45927421ad071069a8e351777",
	"seed2/p1": "fd39881b2dabfc6f48dd275950f06b58a6d1d66eb937476d5cbf0fbfc634c508",
	"seed2/p4": "fd39881b2dabfc6f48dd275950f06b58a6d1d66eb937476d5cbf0fbfc634c508",
	"seed3/p1": "5a9a61ec68656f512d71ed72b04b2816df598d1997f3be2372f72fcc29b23784",
	"seed3/p4": "5a9a61ec68656f512d71ed72b04b2816df598d1997f3be2372f72fcc29b23784",
	"seed4/p1": "40077bf3bf25a212f95ed2a6712ce34685a83914571e358dca8c2a73181faa37",
	"seed4/p4": "40077bf3bf25a212f95ed2a6712ce34685a83914571e358dca8c2a73181faa37",
	"seed5/p1": "4382f130babffa72f8403cdc4d2feea769aabbe9802c6fd72371691430728546",
	"seed5/p4": "4382f130babffa72f8403cdc4d2feea769aabbe9802c6fd72371691430728546",
	"seed6/p1": "10ba18ee84aeb203d494352d0c88a2ee20ee9681708b6449649a175d182d0b70",
	"seed6/p4": "10ba18ee84aeb203d494352d0c88a2ee20ee9681708b6449649a175d182d0b70",
}

func TestSealRenderDifferentialDigest(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	configs := []sealrollup.Config{
		{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}},
		{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}}},
		{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}, {Step: 60, Count: 10080}, {Step: 600, Count: 52560}}},
		{Method: sealrollup.Max, Archives: []sealrollup.Archive{{Step: 60, Count: 1440}, {Step: 3600, Count: 24 * 400}}},
	}
	groups := []v2part.Group{"g", "group-b", "ga"}
	for seed := int64(1); seed <= 6; seed++ {
		for _, parallelism := range []int{1, 4} {
			rng := rand.New(rand.NewSource(seed))
			cfg := coreTestConfig()
			cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 1, 1, true, 1<<20
			e, err := Open(t.TempDir(), cfg, Hooks{Now: func() time.Time { return time.Unix(1, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			e.owners[0].rollups.parallelism = parallelism
			seen := map[uint64]bool{}
			names := 300 + rng.Intn(700)
			type metricSpec struct {
				policy []byte
				cfg    sealrollup.Config
			}
			specs := make([]metricSpec, names)
			for i := range specs {
				var name string
				switch rng.Intn(4) {
				case 0:
					name = fmt.Sprintf("s.%d", rng.Intn(5000))
				case 1:
					name = fmt.Sprintf("containers.k8s.prod.cluster-%02d.ns-%03d.pod-%x.cpu", rng.Intn(3), rng.Intn(9), rng.Intn(1<<20))
				case 2:
					name = fmt.Sprintf("containers.k8s.prod.%08d", rng.Intn(100000))
				default:
					name = fmt.Sprintf("x%d.y", i)
				}
				name = fmt.Sprintf("%s.n%d", name, i)
				cfg := configs[rng.Intn(len(configs))]
				raw, err := EncodeRollupPolicy(RollupPolicy{Name: v2part.ID(name), Group: groups[rng.Intn(len(groups))], Config: cfg})
				if err != nil {
					t.Fatal(err)
				}
				specs[i] = metricSpec{policy: raw, cfg: cfg}
			}
			h := sha256.New()
			order := uint64(1)
			base := int64(1_700_000_000) // seals cross month boundaries below
			for seal := 0; seal < 4; seal++ {
				var batches []Batch
				clock := base + int64(seal)*86400*11
				for i := range specs {
					if rng.Intn(3) == 0 {
						continue
					}
					id := uint64(i + 1)
					for k := 0; k <= rng.Intn(3)/2; k++ {
						rows := make([]Row, 1+rng.Intn(4))
						for j := range rows {
							step := specs[i].cfg.Archives[0].Step
							offset := int64(rng.Intn(40)) * step
							if rng.Intn(8) == 0 { // late point, possibly in the previous month
								offset += int64(rng.Intn(40)) * 86400
							}
							rows[j] = Row{ID: id, Time: clock - offset, Bits: uint64(rng.Int63()), Seq: uint64(rng.Intn(3) + 1)}
						}
						b := Batch{ID: id, Policy: specs[i].policy, Order: order, Clock: clock, Points: rows}
						if seen[id] {
							_ = rng.Intn(2)
						}
						order++
						batches = append(batches, b)
					}
				}
				rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
				for i := range batches {
					batches[i].Order = order
					order++
				}
				if err := e.AppendBatches(0, batches, order, true); err != nil {
					t.Fatal(err)
				}
				for _, b := range batches {
					seen[b.ID] = true
				}
				orderProofObserve(t, t.Name(), e)
				snap := e.DBs()[0].NewSnapshot()
				index, err := NewRollupSnapshot(context.Background(), snap, nil, nil, nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				for i, spec := range specs {
					var policy RollupPolicy
					if err := json.Unmarshal(spec.policy, &policy); err != nil {
						t.Fatal(err)
					}
					fmt.Fprintf(h, "seal %d metric %d\n", seal, i)
					for archive, a := range spec.cfg.Archives {
						err := index.ScanArchive(context.Background(), string(policy.Name), archive, 0, a.Count, func(r sealrollup.Record) error {
							fmt.Fprintf(h, "%d %d %d %016x\n", archive, r.Slot, r.Sample.Time, r.Sample.Bits)
							return nil
						})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				snap.Close()
			}
			key := fmt.Sprintf("seed%d/p%d", seed, parallelism)
			got := fmt.Sprintf("%x", h.Sum(nil))
			if want := sealRenderGolden[key]; want != got {
				t.Errorf("%s: render digest %s, want %s", key, got, want)
			}
			t.Logf("render golden %q: %q,", key, got)
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
