package v2core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"os"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// sealPlanDigest hashes every planned key, value, record and part spec.
func sealPlanDigest(plan coreSealPlan) string {
	var h io.Writer = sha256.New()
	sum := h.(hash.Hash)
	if dump := os.Getenv("SEAL_DIGEST_DUMP"); dump != "" {
		f, _ := os.OpenFile(dump, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		defer f.Close()
		h = io.MultiWriter(h, f)
	}
	// The part UUID is random per seal: mask it in keys and specs.
	for i, file := range plan.Files {
		part := plan.PartSpecs[i].Scope.Part
		fmt.Fprintf(h, "file %d %d\n", i, len(file))
		for _, kv := range file {
			fmt.Fprintf(h, "%x %x", bytes.Replace(kv.Key, part[:], make([]byte, len(part)), 1), kv.Value)
			if kv.Record != nil {
				fmt.Fprintf(h, " %+v", *kv.Record)
			}
			h.Write([]byte{'\n'})
		}
	}
	for _, spec := range plan.PartSpecs {
		spec.Scope.Part = v2part.PartID{}
		// Parallel lanes merge policies in map order (pre-existing); compare as a set.
		spec.Policies = slices.Clone(spec.Policies)
		slices.SortFunc(spec.Policies, func(a, b v2part.ArchivePolicy) int {
			if c := bytes.Compare(a.Digest[:], b.Digest[:]); c != 0 {
				return c
			}
			return int(a.Archive) - int(b.Archive)
		})
		fmt.Fprintf(h, "spec %+v\n", spec)
	}
	return hex.EncodeToString(sum.Sum(nil))
}

// Golden digests were recorded on the parent revision before the seal CPU
// changes and re-recorded once when coarse rollups moved to window closure
// (metadata Version 3): raw records and metadata identity stayed identical;
// only closed-window coarse records, their month envelopes, the version and
// watermark fields and the part bounds derived from them changed.
// Planning must stay byte-identical on randomized multi-group, multi-month,
// duplicate-batch, fresh-name and late-point seals.
var sealPlanGolden = map[string]string{
	"seed1/p1": "6259e16ee3a81329b0cf348163f683e4",
	"seed1/p4": "6259e16ee3a81329b0cf348163f683e4",
	"seed2/p1": "8676afd189175e46a9837c31f9943c26",
	"seed2/p4": "8676afd189175e46a9837c31f9943c26",
	"seed3/p1": "4695d0035bfbacc766af8fbd5a8b2026",
	"seed3/p4": "4695d0035bfbacc766af8fbd5a8b2026",
	"seed4/p1": "7c16dbb9edd19da727bc51a571c2a18b",
	"seed4/p4": "7c16dbb9edd19da727bc51a571c2a18b",
	"seed5/p1": "69f01d70ff5050a12994be9b02a8673c",
	"seed5/p4": "69f01d70ff5050a12994be9b02a8673c",
	"seed6/p1": "b8040620ea2052c14aafee2838936bed",
	"seed6/p4": "b8040620ea2052c14aafee2838936bed",
}

func TestSealPlanDifferentialDigest(t *testing.T) {
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
			r := newCoreRollups()
			r.parallelism = parallelism
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
			var digests []string
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
							rows[j] = Row{Time: clock - offset, Bits: uint64(rng.Int63()), Seq: uint64(rng.Intn(3) + 1)}
						}
						b := Batch{ID: id, Policy: specs[i].policy, Order: order, Clock: clock, Points: rows}
						if m := r.metrics[id]; m != nil && rng.Intn(2) == 0 {
							b.Registered = m.PolicyIdentity
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
				plan, err := r.plan(SealInput{Batches: batches})
				if err != nil {
					t.Fatal(err)
				}
				digests = append(digests, sealPlanDigest(plan))
				plan.Commit()
			}
			key := fmt.Sprintf("seed%d/p%d", seed, parallelism)
			got := sealPlanDigestJoin(digests)
			if want, ok := sealPlanGolden[key]; ok && want != got {
				t.Errorf("%s: digest %s, want %s", key, got, want)
			}
			t.Logf("golden %q: %q,", key, got)
		}
	}
}

func sealPlanDigestJoin(d []string) string {
	h := sha256.New()
	for _, s := range d {
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
