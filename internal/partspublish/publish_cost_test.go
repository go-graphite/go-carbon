package partspublish

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

// Opt-in so ordinary tests do not create a performance fixture or report
// machine-dependent timing. Run under strace to count actual fsync calls.
func TestPublicationCosts(t *testing.T) {
	root := os.Getenv("PARTS_PUBLISH_COST_DIR")
	if root == "" {
		t.Skip("opt-in publication timing")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("cost directory must not exist")
	}
	p := testPublisher(t, root, 1, nil)
	const parts = 16
	const perPart = 4096
	var refs []Ref
	var planNS, commitNS int64
	for n := 1; n <= parts; n++ {
		r := testRequest(t, uint64(n), fmt.Sprintf("cost-%03d", n), 1)
		template := r.Added[r.Envelope.Parts[0].Name][0]
		var blocks []pr.BlockData
		for chunk := 0; chunk < perPart/512; chunk++ {
			b := template
			key, err := pf.Decode(b.Key)
			if err != nil {
				t.Fatal(err)
			}
			key.Ordinal = uint32(chunk)
			b.Key, err = pf.Encode(key)
			if err != nil {
				t.Fatal(err)
			}
			b.Samples = make([]pr.Sample, 512)
			for i := range b.Samples {
				b.Samples[i] = pr.Sample{Time: int64((chunk*512 + i) % 60), Bits: uint64(i), Sequence: uint64(chunk*512 + i)}
			}
			blocks = append(blocks, b)
		}
		refs = append(refs, r.Envelope.Parts[0])
		r.Envelope.Parts = append([]Ref(nil), refs...)
		r.Added[r.Envelope.Parts[len(r.Envelope.Parts)-1].Name] = blocks
		start := time.Now()
		plan, err := p.PlanSeal(r)
		planNS += time.Since(start).Nanoseconds()
		if err != nil {
			t.Fatal(err)
		}
		start = time.Now()
		plan.Commit()
		commitNS += time.Since(start).Nanoseconds()
	}
	// Identical active parts/frontier: compare one catalog-stable publication
	// with a semantics-preserving catalog-byte change that forces descriptor
	// reopening and CRC verification of every active part.
	r := testRequest(t, parts+1, "unused", 1)
	r.Added = nil
	r.Envelope.Parts = append([]Ref(nil), refs...)
	start := time.Now()
	plan, err := p.PlanSeal(r)
	stable := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit()
	r.Envelope.Generation++
	r.Envelope.Frontier[0]++
	r.Envelope.Catalog = append(r.Envelope.Catalog, ' ')
	start = time.Now()
	plan, err = p.PlanSeal(r)
	reopen := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit()
	if err = p.Collect(); err != nil {
		t.Fatal(err)
	}
	out := map[string]any{"stage": "publication_cost", "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339Nano), "parts": parts, "points_per_seal": perPart, "publication_ns_per_point": float64(planNS+commitNS) / (parts * perPart), "plan_ns": planNS, "commit_ns": commitNS, "stable_catalog_ms": float64(stable) / float64(time.Millisecond), "catalog_reopen_ms": float64(reopen) / float64(time.Millisecond), "reopened_parts": parts, "reopened_payload_bytes": parts * perPart * 24, "fsyncs_per_added_part_source_count": 5, "note": "fsync count derives from PublishBlockPart and PlanSeal; syscall count must be measured separately"}
	if err = json.NewEncoder(os.Stdout).Encode(out); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "cost.json"), mustCostJSON(t, out), 0600); err != nil {
		t.Fatal(err)
	}
}
func mustCostJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return b
}
