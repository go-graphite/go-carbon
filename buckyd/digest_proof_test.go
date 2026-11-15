package buckyd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestDigestMutationProof(t *testing.T) {
	s, db := testService(t)
	ctx := context.Background()
	config := chunkstore.MetricConfig{Name: "proof.metric", Retentions: []chunkstore.Retention{{Step: 10, Count: 12}, {Step: 60, Count: 100}}, AggregationMethod: chunkstore.Average}
	request := DigestRequest{Names: []string{config.Name}, Mode: "proof-v1"}
	if err := request.validate(); err != nil {
		t.Fatal(err)
	}
	get := func() DigestResult {
		t.Helper()
		out, err := s.digest(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if out[0].Mode != "proof-v1" || len(out[0].Proof) != 64 || len(out[0].Archives) != 0 {
			t.Fatalf("invalid metadata proof: %+v", out)
		}
		return out[0]
	}
	absent := get()
	if absent.Found {
		t.Fatal("absence reported present")
	}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	created := get()
	if !created.Found || created.Proof == absent.Proof {
		t.Fatal("creation did not invalidate absence")
	}
	if get().Proof != created.Proof {
		t.Fatal("stable metadata changed proof")
	}
	if err := db.UpdateMany(ctx, config.Name, []chunkstore.Point{{Timestamp: 9900, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	written := get()
	if written.Proof == created.Proof {
		t.Fatal("write did not invalidate proof")
	}
	// Two service lifetimes over the same native data cannot share proofs.
	s.proofEpoch = "different service epoch"
	epoch := get()
	if epoch.Proof == written.Proof {
		t.Fatal("epoch did not invalidate proof")
	}
	if err := db.Delete(ctx, config.Name); err != nil {
		t.Fatal(err)
	}
	if get().Found {
		t.Fatal("deleted name reported present")
	}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if get().Proof == epoch.Proof {
		t.Fatal("same-name recreation reused old identity")
	}
}

func TestDigestMutationProofUnknownLegacyAbsence(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := chunkstore.Open(filepath.Join(root, "native"), chunkstore.Options{ExactBlocks: true, IncrementalSource: source})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New(Config{TmpDir: t.TempDir()}, db)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.digest(context.Background(), DigestRequest{Names: []string{"unowned.metric"}, Mode: "proof-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Proof != "" {
		t.Fatal("unowned legacy absence authorized reusable proof")
	}
}
