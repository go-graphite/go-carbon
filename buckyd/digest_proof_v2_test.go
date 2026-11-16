package buckyd

import (
	"context"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestDigestWindowMutationProof(t *testing.T) {
	s, db := testService(t)
	ctx := context.Background()
	c := chunkstore.MetricConfig{Name: "bounded.proof", Retentions: []chunkstore.Retention{{Step: 10, Count: 1200}}, AggregationMethod: chunkstore.Average}
	if _, e := db.Create(ctx, c); e != nil {
		t.Fatal(e)
	}
	get := func(from, until int64) DigestResult {
		t.Helper()
		r := DigestRequest{Names: []string{c.Name}, Mode: "proof-v2", From: from, Until: until}
		if e := r.validate(); e != nil {
			t.Fatal(e)
		}
		out, e := s.digest(ctx, r)
		if e != nil {
			t.Fatal(e)
		}
		if out[0].Mode != "proof-v2" || len(out[0].Proof) != 64 || out[0].Reason == "" {
			t.Fatalf("invalid proof: %+v", out)
		}
		return out[0]
	}
	before := get(9890, 9910)
	if e := db.UpdateMany(ctx, c.Name, []chunkstore.Point{{Timestamp: 9920, Value: 2}}); e != nil {
		t.Fatal(e)
	}
	if get(9890, 9910).Proof != before.Proof {
		t.Fatal("outside-window update invalidated proof")
	}
	if e := db.UpdateMany(ctx, c.Name, []chunkstore.Point{{Timestamp: 9900, Value: 1}}); e != nil {
		t.Fatal(e)
	}
	if get(9890, 9910).Proof == before.Proof {
		t.Fatal("in-window update reused proof")
	}
	if get(9890, 9910).Proof == get(9880, 9910).Proof {
		t.Fatal("window bounds missing from proof")
	}
}
func TestDigestProofV2RequiresExplicitBounds(t *testing.T) {
	for _, r := range []DigestRequest{{Names: []string{"m"}, Mode: "proof-v2"}, {Names: []string{"m"}, Mode: "proof-v2", From: 2, Until: 1}} {
		if r.validate() == nil {
			t.Fatal("invalid bounds accepted")
		}
	}
}
