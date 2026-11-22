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
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// sealPlanDigest hashes every planned key, value, record and part spec.
func sealPlanDigest(plan coreSealPlan) string { return sealPlanDigestMode(plan, false) }

// sealPlanDecodedDigest hashes metadata rows as the full encoding of what they
// decode to, so it is independent of whether envelopes store the definition.
func sealPlanDecodedDigest(plan coreSealPlan) string { return sealPlanDigestMode(plan, true) }

func sealPlanDigestMode(plan coreSealPlan, decoded bool) string {
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
		// The goldens cover what per-slot readers resolve: a slot's
		// superseded versions (followed by a record of the same key) and the
		// record kind are outside them; TestRollupHistoryDifferential and
		// TestDenseRecordVersions cover those.
		superseded := func(j int) bool {
			return file[j].Record != nil && j+1 < len(file) && bytes.Equal(file[j].Key, file[j+1].Key)
		}
		n := 0
		for j := range file {
			if !superseded(j) {
				n++
			}
		}
		fmt.Fprintf(h, "file %d %d\n", i, n)
		for j, kv := range file {
			if superseded(j) {
				continue
			}
			value := kv.Value
			if decoded && kv.Record == nil {
				k, err := v2part.Decode(kv.Key)
				if err != nil {
					panic(err)
				}
				m, err := DecodeRollupMetadata(k.Metric, k.Group, value)
				if err != nil {
					panic(err)
				}
				value = AppendRollupMetadata(nil, m)
			}
			fmt.Fprintf(h, "%x %x", bytes.Replace(kv.Key, part[:], make([]byte, len(part)), 1), value)
			if r := kv.Record; r != nil {
				fmt.Fprintf(h, " %+v", struct {
					sealrollup.Delta
					Correction bool
				}{r.Delta, r.Correction})
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
		fmt.Fprint(h, withoutSeq(fmt.Sprintf("spec %+v\n", spec)))
	}
	return hex.EncodeToString(sum.Sum(nil))
}

// Golden digests were recorded on the parent revision before the seal CPU
// changes and re-recorded once when coarse rollups moved to window closure
// (metadata Version 3): raw records and metadata identity stayed identical;
// only closed-window coarse records, their month envelopes, the version and
// watermark fields and the part bounds derived from them changed.
// Planning must stay byte-identical on randomized multi-group, multi-month,
// duplicate-batch, fresh-name and late-point seals. Since envelopes stopped
// storing the definition these goldens are checked on the decoded surface
// (sealPlanDecodedDigest): every decoded record and envelope, including the
// rebuilt definition bytes, is unchanged. sealPlanSlimGolden is the physical
// surface, re-recorded once when the definition left the envelope.
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
	t.Parallel()
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
			var digests, decoded []string
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
				decoded = append(decoded, sealPlanDecodedDigest(plan))
				plan.Commit()
			}
			key := fmt.Sprintf("seed%d/p%d", seed, parallelism)
			got := sealPlanDigestJoin(digests)
			if want := sealPlanSlimGolden[key]; want != got {
				t.Errorf("%s: digest %s, want %s", key, got, want)
			}
			if got, want := sealPlanDigestJoin(decoded), sealPlanGolden[key]; got != want {
				t.Errorf("%s: decoded digest %s, want %s", key, got, want)
			}
			t.Logf("golden %q: %q,", key, got)
		}
	}
}

// withoutSeq drops a zero Provenance.Seq from a %+v dump, so digests recorded
// before the field existed still cover every other field byte for byte.
func withoutSeq(s string) string { return strings.Replace(s, " Seq:0}", "}", 1) }

func sealPlanDigestJoin(d []string) string {
	h := sha256.New()
	for _, s := range d {
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

var sealPlanSlimGolden = map[string]string{
	"seed1/p1": "59b674e7856391cc484494820c34ad5f",
	"seed1/p4": "59b674e7856391cc484494820c34ad5f",
	"seed2/p1": "570c089f52c26badae3013ee693f0162",
	"seed2/p4": "570c089f52c26badae3013ee693f0162",
	"seed3/p1": "0bb8fea55700cc58e36df1a476f2f040",
	"seed3/p4": "0bb8fea55700cc58e36df1a476f2f040",
	"seed4/p1": "bb2631bde16893aa9650291907842d84",
	"seed4/p4": "bb2631bde16893aa9650291907842d84",
	"seed5/p1": "4f230a74481563452348f565c73f1b49",
	"seed5/p4": "4f230a74481563452348f565c73f1b49",
	"seed6/p1": "df4b4dff5ad8945103aff9f9668383c3",
	"seed6/p4": "df4b4dff5ad8945103aff9f9668383c3",
}
