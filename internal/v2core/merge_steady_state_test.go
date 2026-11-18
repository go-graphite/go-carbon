package v2core

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// The steady-state merge simulator drives pickMergeTier, the production
// selector policy, over hours of time-only seals that a short host arm cannot
// reach. A merge output is modelled as the sum of its inputs less the metric
// envelopes that collapse to one: with a seal part of S bytes of which a
// fraction e is envelope, sixteen inputs write sum - 15*e*S. Host evidence
// (time-only 10 s seals, 96 owners) calibrates it: first-generation merges
// read 16 seal parts of 12.60 MB and wrote 27.5% of that (e = 0.773); the
// earlier cadence read 7.41 MB parts and wrote 18.9% (e = 0.865).
type mergeSimPart struct {
	seals    int // seals covered
	rewrites int // sum over covered seals of rewrites so far
	depth    int // most rewrites of any covered seal
}

type mergeSimResult struct {
	seals                 int
	sealed, read, written float64
	merges                int
	rewritesPerSeal       float64
	maxDepth              int
	liveMean              float64
	liveMax               int
	lastHourMax           int
	vmBelow               int // merges whose output < 1.7x their largest input
}

func simulateMerges(tierOf func(v2part.Descriptor) int, seals int, sealBytes, envelope, jitter float64, seed uint64) mergeSimResult {
	old := mergeTier
	mergeTier = tierOf
	defer func() { mergeTier = old }()
	rng := rand.New(rand.NewPCG(seed, 0x6d657267))
	envBytes := envelope * sealBytes
	var (
		r     mergeSimResult
		live  []v2part.Descriptor
		meta  = map[v2part.Digest]mergeSimPart{}
		next  uint64
		liveN int
	)
	r.seals = seals
	newDescriptor := func(b float64, generation int) v2part.Descriptor {
		next++
		var d v2part.Descriptor
		d.Scope.Group = "g"
		binary.BigEndian.PutUint64(d.Scope.Part[:8], next)
		if generation > 0 {
			setPartGeneration(&d.Scope.Part, generation)
		}
		binary.BigEndian.PutUint64(d.SHA256[:8], next)
		d.Bytes = uint64(b)
		return d
	}
	lastHour := seals - 360
	for s := 0; s < seals; s++ {
		b := sealBytes * (1 + jitter*(2*rng.Float64()-1))
		d := newDescriptor(b, 0)
		live = append(live, d)
		meta[d.SHA256] = mergeSimPart{seals: 1}
		r.sealed += b
		for {
			inputs := pickMergeTier(live, 150<<30)
			if inputs == nil {
				break
			}
			var in, largest float64
			var out mergeSimPart
			generation := 0
			gone := map[v2part.Digest]bool{}
			for _, d := range inputs {
				m := meta[d.SHA256]
				in += float64(d.Bytes)
				largest = max(largest, float64(d.Bytes))
				out.seals += m.seals
				out.rewrites += m.rewrites + m.seals
				out.depth = max(out.depth, m.depth+1)
				generation = max(generation, partGeneration(d.Scope.Part))
				gone[d.SHA256] = true
				delete(meta, d.SHA256)
			}
			ob := in - float64(len(inputs)-1)*envBytes
			if ob < 1.7*largest {
				r.vmBelow++
			}
			r.merges++
			r.read += in
			r.written += ob
			r.maxDepth = max(r.maxDepth, out.depth)
			kept := live[:0]
			for _, d := range live {
				if !gone[d.SHA256] {
					kept = append(kept, d)
				}
			}
			o := newDescriptor(ob, generation+1)
			live = append(kept, o)
			meta[o.SHA256] = out
		}
		liveN += len(live)
		r.liveMax = max(r.liveMax, len(live))
		if s >= lastHour {
			r.lastHourMax = max(r.lastHourMax, len(live))
		}
	}
	r.liveMean = float64(liveN) / float64(seals)
	var rewrites int
	for _, m := range meta {
		rewrites += m.rewrites
	}
	r.rewritesPerSeal = float64(rewrites) / float64(seals)
	return r
}

// generationTier is the production tier key, captured before any test swaps it.
var generationTier = mergeTier

func TestMergeSelectorSteadyState(t *testing.T) {
	hours := 6.0
	if s := os.Getenv("MERGE_SIM_HOURS"); s != "" {
		var err error
		if hours, err = strconv.ParseFloat(s, 64); err != nil {
			t.Fatal(err)
		}
	}
	seals := int(hours * 360) // time-only 10 s seals
	bound := int(math.Ceil(math.Log(float64(seals)) / math.Log(16)))
	type owner struct {
		name            string
		bytes, envelope float64
	}
	owners := []owner{
		{"T10 10s seals", 12.60e6, 0.773},
		{"A1 cadence", 7.41e6, 0.865},
	}
	// Production groups seal parts of every size: sweep one 16x tier decade
	// log-uniformly at both calibrations.
	const sweep = 32
	for _, e := range []float64{0.773, 0.865} {
		for i := 0; i < sweep; i++ {
			b := float64(1<<20) * math.Pow(16, float64(i)/sweep)
			owners = append(owners, owner{fmt.Sprintf("sweep e=%.3f %.2fMiB", e, b/(1<<20)), b, e})
		}
	}
	type pair struct{ a, b mergeSimResult }
	results := make([]pair, len(owners))
	var aw, bw, as, bs float64
	for i, o := range owners {
		seed := uint64(i + 1)
		results[i] = pair{
			simulateMerges(sizeTier, seals, o.bytes, o.envelope, 0.10, seed),
			simulateMerges(generationTier, seals, o.bytes, o.envelope, 0.10, seed),
		}
		a, b := results[i].a, results[i].b
		t.Logf("%-26s A: w/s %.3f r/s %.3f rw/seal %.2f depth %d merges %d live %.1f/%d last-h %d vm<1.7 %d | B: w/s %.3f r/s %.3f rw/seal %.2f depth %d merges %d live %.1f/%d last-h %d vm<1.7 %d",
			o.name, a.written/a.sealed, a.read/a.sealed, a.rewritesPerSeal, a.maxDepth, a.merges, a.liveMean, a.liveMax, a.lastHourMax, a.vmBelow,
			b.written/b.sealed, b.read/b.sealed, b.rewritesPerSeal, b.maxDepth, b.merges, b.liveMean, b.liveMax, b.lastHourMax, b.vmBelow)
		if i >= 2 {
			aw += a.written / a.sealed
			bw += b.written / b.sealed
			as += a.rewritesPerSeal
			bs += b.rewritesPerSeal
		}
		if b.maxDepth > bound {
			t.Errorf("%s: generation tiers rewrote a record %d times, want <= ceil(log16 %d) = %d", o.name, b.maxDepth, seals, bound)
		}
		if b.vmBelow != 0 {
			t.Errorf("%s: %d generation merges grew less than 1.7x their largest input", o.name, b.vmBelow)
		}
	}
	n := float64(len(owners) - 2)
	t.Logf("sweep mean over %d owners, %d seals (%.1f h): written/sealed A %.3f B %.3f (B/A %.2f), rewrites/seal A %.2f B %.2f",
		len(owners)-2, seals, hours, aw/n, bw/n, bw/aw, as/n, bs/n)
	if bw > aw {
		t.Errorf("generation tiers wrote more than size tiers over the sweep: %.3f > %.3f", bw/n, aw/n)
	}
}
