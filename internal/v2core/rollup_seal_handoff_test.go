package v2core

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestSealHandoffByteDifferential writes every planned part of randomized seals
// twice: through the reference byte path (full keys, writePlannedRollupPart)
// and through the tuple handoff (writePlannedRollupTuples). Part file bytes and
// descriptors must be identical. Seals cover 1 and 3 planner lanes, several
// groups, month partitions (late points), idle expiry and idle closure stages,
// and envelopes. Mutated tuple streams (order, key fields) must be detected.
func TestSealHandoffByteDifferential(t *testing.T) {
	configs := []sealrollup.Config{
		{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}}},
		{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}, {Step: 60, Count: 10080}, {Step: 600, Count: 52560}}},
		{Method: sealrollup.Max, Archives: []sealrollup.Archive{{Step: 60, Count: 1440}, {Step: 3600, Count: 24 * 400}}},
	}
	groups := []v2part.Group{"g", "group-b"}
	dir := t.TempDir()
	var parts, multiSegment, idle, partitions, mutations int
	for seed := int64(1); seed <= 3; seed++ {
		for _, lanes := range []int{1, 3} {
			rng := rand.New(rand.NewSource(seed*7 + int64(lanes)))
			r := newCoreRollups()
			r.parallelism = lanes
			expired := map[v2part.ID]bool{}
			r.expire = func(p RollupPolicy, _ RollupMetadata, _ time.Time) bool { return expired[p.Name] }
			r.setExpiryTTL(expiryWindow(time.Second)) // plans run an hour after writes
			names := 500 + rng.Intn(400)
			type metricSpec struct {
				name   v2part.ID
				policy []byte
				cfg    sealrollup.Config
			}
			specs := make([]metricSpec, names)
			for i := range specs {
				name := v2part.ID(fmt.Sprintf("containers.k8s.prod.cluster-%02d.pod-%x.cpu.n%d", rng.Intn(3), rng.Intn(1<<20), i))
				cfg := configs[rng.Intn(len(configs))]
				raw, err := EncodeRollupPolicy(RollupPolicy{Name: name, Group: groups[rng.Intn(len(groups))], Config: cfg})
				if err != nil {
					t.Fatal(err)
				}
				specs[i] = metricSpec{name: name, policy: raw, cfg: cfg}
			}
			order := uint64(1)
			base := int64(1_700_000_000)
			for seal := 0; seal < 4; seal++ {
				clock := base + int64(seal)*86400*11
				r.now = func() time.Time { return time.Unix(clock+3600, 0) }
				clear(expired)
				var batches []Batch
				for i := range specs {
					if rng.Intn(3) == 0 {
						if seal > 0 && rng.Intn(4) == 0 {
							expired[specs[i].name] = true
						}
						continue
					}
					rows := make([]Row, 1+rng.Intn(4))
					for j := range rows {
						offset := int64(rng.Intn(40)) * specs[i].cfg.Archives[0].Step
						if rng.Intn(8) == 0 {
							offset += int64(rng.Intn(40)) * 86400
						}
						rows[j] = Row{Time: clock - offset, Bits: uint64(rng.Int63()), Seq: uint64(rng.Intn(3) + 1)}
					}
					batches = append(batches, Batch{ID: uint64(i + 1), Policy: specs[i].policy, Order: order, Clock: clock, Points: rows})
					order++
				}
				plan, err := r.planParts(SealInput{Batches: batches})
				if err != nil {
					t.Fatal(err)
				}
				orderProofPlanned(r, &plan)
				for _, s := range r.stages {
					if s.batches == 0 {
						idle++
					}
				}
				files, partSpecs := expandPlannedParts(plan.Parts, plannedOwners(r))
				seen := map[int64]bool{}
				for i, part := range plan.Parts {
					// The owner's part workers stamp the durable source stream.
					part.Spec.Source.Incarnation, part.Spec.Source.Stream = v2part.PartID{byte(seed)}, "handoff"
					part.Spec.Source.Seq = uint64(seal + 1)
					partSpecs[i].Source = part.Spec.Source
					parts++
					seen[part.Spec.Scope.Partition] = true
					if len(part.Segments) > 1 {
						multiSegment++
					}
					oldPath := filepath.Join(dir, fmt.Sprintf("old-%d-%d-%d-%d.sst", seed, lanes, seal, i))
					newPath := filepath.Join(dir, fmt.Sprintf("new-%d-%d-%d-%d.sst", seed, lanes, seal, i))
					want, err := writePlannedRollupPart(oldPath, files[i], partSpecs[i])
					if err != nil {
						t.Fatal(err)
					}
					got, err := writePlannedRollupTuples(newPath, part)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("seed%d lanes%d seal%d part%d: descriptor\n got %+v\nwant %+v", seed, lanes, seal, i, got, want)
					}
					wantBytes, _ := os.ReadFile(oldPath)
					gotBytes, _ := os.ReadFile(newPath)
					if len(wantBytes) == 0 || !bytes.Equal(gotBytes, wantBytes) {
						t.Fatalf("seed%d lanes%d seal%d part%d: part bytes differ (%d vs %d)", seed, lanes, seal, i, len(gotBytes), len(wantBytes))
					}
					if i == 0 && seal == 1 {
						mutations += checkHandoffMutations(t, dir, part, wantBytes)
					}
				}
				if len(seen) > 1 {
					partitions++
				}
				plan.Commit()
			}
		}
	}
	if parts == 0 || multiSegment == 0 || idle == 0 || partitions == 0 || mutations == 0 {
		t.Fatalf("coverage: parts=%d multi-lane parts=%d idle stages=%d multi-partition seals=%d mutations=%d", parts, multiSegment, idle, partitions, mutations)
	}
	t.Logf("parts=%d multi-lane parts=%d idle stages=%d multi-partition seals=%d mutations=%d", parts, multiSegment, idle, partitions, mutations)
}

// checkHandoffMutations rewrites part with broken tuple order or key fields;
// each must fail to write or change the part bytes. It returns the count run.
func checkHandoffMutations(t *testing.T, dir string, part plannedRollupPart, want []byte) int {
	t.Helper()
	flat := slices.Concat(part.Segments...)
	other := slices.IndexFunc(flat, func(kv plannedKV) bool { return kv.name != flat[0].name })
	firstRecord := slices.IndexFunc(flat, func(kv plannedKV) bool { return kv.record != nil })
	if other < 0 || firstRecord < 0 {
		t.Fatal("mutation needs two metrics and a record")
	}
	mutate := map[string]func([]plannedKV){
		"swap metrics": func(kvs []plannedKV) { kvs[0], kvs[other] = kvs[other], kvs[0] },
		"swap last":    func(kvs []plannedKV) { n := len(kvs); kvs[n-1], kvs[n-2] = kvs[n-2], kvs[n-1] },
		"slot":         func(kvs []plannedKV) { r := *kvs[firstRecord].record; r.Slot += 7; kvs[firstRecord].record = &r },
		"archive":      func(kvs []plannedKV) { r := *kvs[firstRecord].record; r.Archive++; kvs[firstRecord].record = &r },
		"metric": func(kvs []plannedKV) {
			kvs[firstRecord].name, kvs[firstRecord].vshard = flat[other].name, flat[other].vshard
		},
		"name":     func(kvs []plannedKV) { kvs[firstRecord].name = flat[other].name },
		"vshard":   func(kvs []plannedKV) { kvs[firstRecord].vshard ^= 1 },
		"envelope": func(kvs []plannedKV) { kvs[len(kvs)-1].value = append([]byte{1}, kvs[len(kvs)-1].value...) },
	}
	n := 0
	for name, f := range mutate {
		kvs := slices.Clone(flat)
		f(kvs)
		path := filepath.Join(dir, "mutated.sst")
		_, err := writePlannedRollupTuples(path, plannedRollupPart{Spec: part.Spec, Segments: [][]plannedKV{kvs}})
		if err == nil {
			got, _ := os.ReadFile(path)
			if bytes.Equal(got, want) {
				t.Errorf("mutation %q not detected by the byte differential", name)
			}
			_ = os.Remove(path)
		}
		n++
	}
	return n
}
