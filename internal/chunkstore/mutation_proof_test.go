package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func proofTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(10000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func windowProof(t *testing.T, s *Store) (string, string) {
	t.Helper()
	p, r, e := s.MutationProof(context.Background(), "proof", 9890, 9910)
	if e != nil || p == "" {
		t.Fatalf("proof=%q reason=%q err=%v", p, r, e)
	}
	return p, r
}
func seedProof(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Create(ctx, MetricConfig{Name: "proof", Retentions: []Retention{{Step: 10, Count: 1200}}, AggregationMethod: Average}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMany(ctx, "proof", []Point{{Timestamp: 9900, Value: 1}}); err != nil {
		t.Fatal(err)
	}
}
func TestMutationProofExactWindowAndFutureAlias(t *testing.T) {
	s := proofTestStore(t, t.TempDir())
	defer s.Close()
	seedProof(t, s)
	before, _ := windowProof(t, s)
	if err := s.UpdateMany(context.Background(), "proof", []Point{{Timestamp: 9920, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	after, _ := windowProof(t, s)
	if before != after {
		t.Fatal("outside write reopened clean window")
	}
	if err := s.UpdateMany(context.Background(), "proof", []Point{{Timestamp: 21900, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	alias, reason := windowProof(t, s)
	if alias == after || reason != "in-window mutation" {
		t.Fatalf("future circular alias lost old owner: %s", reason)
	}
}
func TestMutationProofCleanRepeatedRestart(t *testing.T) {
	dir := t.TempDir()
	s := proofTestStore(t, dir)
	seedProof(t, s)
	before, _ := windowProof(t, s)
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s = proofTestStore(t, dir)
		after, _ := windowProof(t, s)
		if before != after {
			t.Fatalf("clean restart %d reopened proof (%s)", i, s.proofLineage.Reason)
		}
	}
	defer s.Close()
}
func TestMutationProofBrokenLineageFailsClosed(t *testing.T) {
	for _, kind := range []string{"unclean", "missing_link", "unknown_version", "rollback", "interrupted_publish", "storage_reset", "post_seal_change"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			s := proofTestStore(t, dir)
			seedProof(t, s)
			before, _ := windowProof(t, s)
			old := s.proofLineage
			if kind == "unclean" {
				s.proofLineage.Process = ""
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing_link":
				if err := os.Remove(filepath.Join(dir, "PROOF-"+old.Process)); err != nil {
					t.Fatal(err)
				}
			case "unknown_version":
				data, _ := json.Marshal(proofLineage{Version: 99})
				if err := os.WriteFile(filepath.Join(dir, proofLineageFile), data, 0600); err != nil {
					t.Fatal(err)
				}
			case "interrupted_publish":
				if err := os.WriteFile(filepath.Join(dir, proofLineageFile+".tmp"), []byte("partial"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, proofLineageFile)); err != nil {
					t.Fatal(err)
				}
			case "post_seal_change":
				data, err := os.ReadFile(filepath.Join(dir, proofLineageFile))
				if err != nil {
					t.Fatal(err)
				}
				var sealed proofLineage
				if err = json.Unmarshal(data, &sealed); err != nil {
					t.Fatal(err)
				}
				sealed.Sequence-- // A nonparticipating writer changed the sequence after this seal.
				data, _ = json.Marshal(sealed)
				for _, name := range []string{proofLineageFile, "PROOF-" + sealed.Process} {
					if err = os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "storage_reset":
				if err := os.RemoveAll(dir); err != nil {
					t.Fatal(err)
				}
			}
			s = proofTestStore(t, dir)
			if kind == "storage_reset" {
				seedProof(t, s)
			}
			if kind == "rollback" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				data, _ := json.Marshal(old)
				if err := os.WriteFile(filepath.Join(dir, proofLineageFile), data, 0600); err != nil {
					t.Fatal(err)
				}
				s = proofTestStore(t, dir)
			}
			defer s.Close()
			after, _ := windowProof(t, s)
			if before == after {
				t.Fatalf("%s reused stale proof", kind)
			}
		})
	}
}
func TestMutationProofUnknownPathAndHistoryBound(t *testing.T) {
	s := proofTestStore(t, t.TempDir())
	defer s.Close()
	seedProof(t, s)
	before, _ := windowProof(t, s)
	m, err := s.Metadata(context.Background(), "proof")
	if err != nil {
		t.Fatal(err)
	}
	m.Revision++
	if err = s.db.Set(revisionKey(m), revisionBytes(m), s.writeOptions); err != nil {
		t.Fatal(err)
	}
	unknown, reason := windowProof(t, s)
	if unknown == before || reason != "unknown mutation path" {
		t.Fatal("unsupported mutation escaped revision fence")
	}
	for i := 0; i < mutationHistoryLimit+2; i++ {
		if err = s.UpdateMany(context.Background(), "proof", []Point{{Timestamp: 9920, Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	bounded, _ := windowProof(t, s)
	if bounded != unknown {
		t.Fatal("outside-window history churn reopened proof")
	}
	h, err := readMutationHistory(s.db, m)
	if err != nil || len(h.Entries) > mutationHistoryLimit {
		t.Fatalf("unbounded history: %v", err)
	}
}

func TestMutationProofDeleteRecreateAndNonfinite(t *testing.T) {
	s := proofTestStore(t, t.TempDir())
	defer s.Close()
	seedProof(t, s)
	before, _ := windowProof(t, s)
	if err := s.UpdateMany(context.Background(), "proof", []Point{{Timestamp: 9900, Value: math.NaN()}}); err != nil {
		t.Fatal(err)
	}
	changed, _ := windowProof(t, s)
	if changed == before {
		t.Fatal("nonfinite replacement retained finite-point proof")
	}
	if err := s.Delete(context.Background(), "proof"); err != nil {
		t.Fatal(err)
	}
	absent, _ := windowProof(t, s)
	if absent == changed {
		t.Fatal("deletion retained proof")
	}
	seedProof(t, s)
	recreated, _ := windowProof(t, s)
	if recreated == before || recreated == absent {
		t.Fatal("recreated metric reused old identity")
	}
}

func TestMutationProofNewScopeAfterUnknownWrite(t *testing.T) {
	s := proofTestStore(t, t.TempDir())
	defer s.Close()
	seedProof(t, s)
	old, _ := windowProof(t, s)
	m, e := s.Metadata(context.Background(), "proof")
	if e != nil {
		t.Fatal(e)
	}
	m.Revision++
	if e = s.db.Set(revisionKey(m), revisionBytes(m), s.writeOptions); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.MutationProof(context.Background(), "proof", 9800, 9820); e != nil {
		t.Fatal(e)
	}
	got, _ := windowProof(t, s)
	if got == old {
		t.Fatal("new scope hid unknown mutation")
	}
	if e = s.UpdateMany(context.Background(), "proof", []Point{{Timestamp: 9920, Value: 8}}); e != nil {
		t.Fatal(e)
	}
	if after, _ := windowProof(t, s); after != got {
		t.Fatal("outside write changed repaired scope")
	}
}

func TestMutationProofDeletionReclaimsScope(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			s := proofTestStore(t, t.TempDir())
			defer s.Close()
			seedProof(t, s)
			windowProof(t, s)
			m, e := s.Metadata(context.Background(), "proof")
			if e != nil {
				t.Fatal(e)
			}
			if batch {
				_, e = s.DeleteBatchIfUnchanged(context.Background(), []Metadata{m})
			} else {
				e = s.Delete(context.Background(), "proof")
			}
			if e != nil {
				t.Fatal(e)
			}
			_, closer, e := s.db.Get(mutationKey(m))
			if closer != nil {
				closer.Close()
			}
			if !errors.Is(e, pebble.ErrNotFound) {
				t.Fatalf("deleted mutation scope: %v", e)
			}
		})
	}
}
