package whisperio

import (
	"context"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestContinuationMutationProofPreservesUnchangedWindow(t *testing.T) {

	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1700000000, 0) }
	codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
	s, e := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: codec, Now: clock})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c := store.MetricConfig{Name: "proof", Retentions: []store.Retention{{Step: 10, Count: 1200}}, AggregationMethod: store.Average}
	if _, e = s.Create(ctx, c); e != nil {
		t.Fatal(e)
	}
	if e = s.UpdateMany(ctx, c.Name, []store.Point{{Timestamp: 1699999900, Value: 1}}); e != nil {
		t.Fatal(e)
	}
	get := func() string {
		t.Helper()
		p, r, e := s.MutationProof(ctx, c.Name, 1699999890, 1699999910)
		if e != nil || p == "" {
			t.Fatalf("proof %s: %v", r, e)
		}
		return p
	}
	before := get()
	if e = s.UpdateMany(ctx, c.Name, []store.Point{{Timestamp: 1699999920, Value: 2}}); e != nil {
		t.Fatal(e)
	}
	if get() != before {
		t.Fatal("later point reopened historical proof")
	}
	if e = s.UpdateMany(ctx, c.Name, []store.Point{{Timestamp: 1699999900, Value: 7}}); e != nil {
		t.Fatal(e)
	}
	if get() == before {
		t.Fatal("old point correction failed to invalidate proof")
	}
}
