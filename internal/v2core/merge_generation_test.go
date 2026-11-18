package v2core

import (
	"context"
	"crypto/rand"
	"math/bits"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// sizeTier is the former tier key: sixteen times wide in compressed bytes.
func sizeTier(d v2part.Descriptor) int { return (bits.Len64(d.Bytes) - 1) / 4 }

type generationRun struct {
	records    []sealrollup.Record
	merges     int
	maxRewrite int // most rewrites of any sealed record
	repeats    int // merge inputs whose records were already merged at this tier or above
	readBytes  uint64
	liveParts  int
}

func inventoryDescriptors(t *testing.T, o *coreOwner) []v2part.Descriptor {
	t.Helper()
	o.gc.mu.Lock()
	defer o.gc.mu.Unlock()
	var out []v2part.Descriptor
	for _, v := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(v)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, d)
	}
	return out
}

// runGenerationSeals writes one seal part per order and drains every due merge
// after each seal, as the background worker would. Each part carries the
// lineage of its records: how often they were rewritten and the tier of their
// last merge. mergeAppend rewrites the same slots, so a merge output is about
// one input's size, the shape that kept size-tier outputs in their own tier.
func runGenerationSeals(t *testing.T, seals, restartAt int, byGeneration bool) generationRun {
	t.Helper()
	root := t.TempDir()
	e := mergeFixtureIdle(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	type lineage struct{ rewrites, tier int }
	ancestry := map[v2part.Digest]lineage{}
	var r generationRun
	for n := 1; n <= seals; n++ {
		if n == restartAt {
			// Generations come from durable descriptors, not engine memory.
			if err := e.close(); err != nil {
				t.Fatal(err)
			}
			e = nil
			e = mergeFixtureIdle(t, root, nil)
		}
		mergeAppend(t, e, uint64(n))
		o := e.owners[0]
		for _, d := range inventoryDescriptors(t, o) {
			if _, ok := ancestry[d.SHA256]; !ok {
				if byGeneration && partGeneration(d.Scope.Part) != 0 {
					t.Fatalf("seal part has generation %d", partGeneration(d.Scope.Part))
				}
				ancestry[d.SHA256] = lineage{0, -1}
			}
		}
		for {
			s := o.db.NewSnapshot()
			inputs, err := o.selectMerge(s)
			_ = s.Close()
			if err != nil {
				t.Fatal(err)
			}
			if len(inputs) == 0 {
				break
			}
			if len(inputs) != sealedMergeFanIn {
				t.Fatalf("fan-in %d", len(inputs))
			}
			tier := mergeTier(inputs[0])
			next := lineage{tier: tier}
			for _, d := range inputs {
				a, ok := ancestry[d.SHA256]
				if !ok || mergeTier(d) != tier {
					t.Fatalf("input outside its tier: known=%v tier %d != %d", ok, mergeTier(d), tier)
				}
				if a.tier >= tier {
					r.repeats++
				}
				next.rewrites = max(next.rewrites, a.rewrites+1)
				r.readBytes += d.Bytes
			}
			changed, err := o.mergeSealedParts(nil)
			if err != nil || !changed {
				t.Fatalf("merge %v %v", changed, err)
			}
			r.merges++
			for _, d := range inventoryDescriptors(t, o) {
				if _, ok := ancestry[d.SHA256]; ok {
					continue
				}
				if byGeneration && (partGeneration(d.Scope.Part) != tier+1 || next.rewrites != tier+1) {
					t.Fatalf("output generation %d, rewrites %d, after a tier %d merge", partGeneration(d.Scope.Part), next.rewrites, tier)
				}
				ancestry[d.SHA256] = next
				r.maxRewrite = max(r.maxRewrite, next.rewrites)
			}
		}
	}
	o := e.owners[0]
	s := o.db.NewSnapshot()
	r.records = mergeRead(t, s)
	_ = s.Close()
	r.liveParts = mergeCount(o)
	return r
}

// A record is rewritten at most once per merge tier over a long run of seals,
// across a restart, with renders unchanged. Size tiers fail the same check.
func TestMergeGenerationRewritesOncePerTier(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	const seals = 512
	bound := (bits.Len(seals) - 1) / 4 // floor(log16 seals): 2

	t.Setenv("CORE_MERGE_MAX_BYTES", "1") // every part is over the cap: no merges
	reference := runGenerationSeals(t, seals, 0, false)
	if reference.merges != 0 || reference.liveParts < seals {
		t.Fatalf("reference merged: %d merges, %d parts", reference.merges, reference.liveParts)
	}
	t.Setenv("CORE_MERGE_MAX_BYTES", "")

	got := runGenerationSeals(t, seals, seals/2+7, true)
	if got.repeats != 0 || got.maxRewrite > bound || got.merges == 0 {
		t.Fatalf("generation tiers: %d repeated tier merges, %d rewrites > %d, %d merges", got.repeats, got.maxRewrite, bound, got.merges)
	}
	if !reflect.DeepEqual(got.records, reference.records) {
		t.Fatal("generation merges changed exact records")
	}

	// Mutation check: the former size tiers must fail the same property.
	old := mergeTier
	mergeTier = sizeTier
	size := runGenerationSeals(t, seals, 0, false)
	mergeTier = old
	if size.repeats == 0 && size.maxRewrite <= bound {
		t.Fatal("size tiers also rewrite once per tier: the check cannot tell them apart")
	}
	if !reflect.DeepEqual(size.records, reference.records) {
		t.Fatal("size-tier merges changed exact records")
	}
	t.Logf("generation: %d merges, %d input bytes, max %d rewrites, %d live parts; size: %d merges, %d input bytes, max %d rewrites (%d repeated), %d live parts",
		got.merges, got.readBytes, got.maxRewrite, got.liveParts, size.merges, size.readBytes, size.maxRewrite, size.repeats, size.liveParts)
}

func TestMergeGenerationTag(t *testing.T) {
	var id v2part.PartID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	id[11] ^= 0xff // never the tag
	if partGeneration(id) != 0 {
		t.Fatal("untagged part has a generation")
	}
	random := id
	setPartGeneration(&id, 3)
	if partGeneration(id) != 3 || [11]byte(id[:11]) != [11]byte(random[:11]) {
		t.Fatalf("tag round trip: %x", id)
	}
	setPartGeneration(&id, 300)
	if partGeneration(id) != 255 {
		t.Fatal("generation does not saturate")
	}

	// A TTL rewrite keeps its input's generation; a merge goes one above.
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= sealedMergeFanIn; n++ {
		mergeAppend(t, e, n)
	}
	if mergeDrain(t, o) != 1 {
		t.Fatal("no merge")
	}
	parts := inventoryDescriptors(t, o)
	if len(parts) != 1 || partGeneration(parts[0].Scope.Part) != 1 {
		t.Fatalf("merge output: %d parts", len(parts))
	}
	b, err := parts[0].MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if d, err := v2part.ParseDescriptor(b); err != nil || d.Scope.Part != parts[0].Scope.Part {
		t.Fatalf("tagged descriptor does not round trip: %v", err)
	}
	for _, c := range []struct {
		ttl  int64
		want int
	}{{1, 1}, {0, 2}} {
		d, err := mergePartFiles(context.Background(), o.dir, filepath.Join(t.TempDir(), "out.sst"), parts, c.ttl, nil)
		if err != nil {
			t.Fatal(err)
		}
		if partGeneration(d.Scope.Part) != c.want {
			t.Fatalf("ttl %d: generation %d, want %d", c.ttl, partGeneration(d.Scope.Part), c.want)
		}
	}
}
