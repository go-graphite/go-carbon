package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func releaseProof(t *testing.T, s *Store, r ProofRelease) ProofReleaseResult {
	t.Helper()
	got := s.ReleaseMutationProofs(t.Context(), []ProofRelease{r})
	if len(got) != 1 || got[0].Status == "error" {
		t.Fatalf("release: %+v", got)
	}
	return got[0]
}
func assertNoProofHistory(t *testing.T, s *Store, name string) {
	t.Helper()
	m, err := s.Metadata(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	_, closer, err := s.db.Get(mutationKey(m))
	if closer != nil {
		closer.Close()
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("history survived: %v", err)
	}
}
func proofPointBytes(t *testing.T, s *Store, name string) []byte {
	t.Helper()
	snapshot, err := s.Snapshot(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	for _, a := range snapshot.Archives {
		if err := binary.Write(&b, binary.BigEndian, a.Points); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}
func TestProofReleasePartialFreshAndIdempotent(t *testing.T) {
	s, oracle := proofTestStore(t, t.TempDir()), proofTestStore(t, t.TempDir())
	defer s.Close()
	defer oracle.Close()
	for _, db := range []*Store{s, oracle} {
		seedProof(t, db)
		windowProof(t, db)
	}
	old, _ := windowProof(t, s)
	if _, _, err := s.MutationProof(t.Context(), "proof", 9950, 9970); err != nil {
		t.Fatal(err)
	}
	m, _ := s.Metadata(t.Context(), "proof")
	if r := releaseProof(t, s, ProofRelease{Name: "proof", ID: m.ID, Generation: m.Generation + 1}); r.Status != "conflict" {
		t.Fatal(r)
	}
	if r := releaseProof(t, s, ProofRelease{Name: "proof", From: 9890, Until: 9911}); r.Released != 0 {
		t.Fatal(r)
	}
	if r := releaseProof(t, s, ProofRelease{Name: "proof", ID: m.ID, Generation: m.Generation, From: 9890, Until: 9910}); r.Released != 1 {
		t.Fatal(r)
	}
	h, err := readMutationHistory(s.db, m)
	if err != nil || len(h.Scopes) != 1 || h.Scopes[0].From != 9950 {
		t.Fatalf("partial: %+v %v", h, err)
	}
	releaseProof(t, s, ProofRelease{Name: "proof", From: 9950, Until: 9970})
	assertNoProofHistory(t, s, "proof")
	for range 2 {
		if r := releaseProof(t, s, ProofRelease{Name: "proof"}); r.Status != "absent" {
			t.Fatal(r)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if r := s.ReleaseMutationProofs(ctx, []ProofRelease{{Name: "proof"}}); r[0].Status != "error" {
		t.Fatal(r)
	}
	for _, db := range []*Store{s, oracle} {
		if err := db.UpdateMany(t.Context(), "proof", []Point{{Timestamp: 9900, Value: 7}}); err != nil {
			t.Fatal(err)
		}
	}
	fresh, _ := windowProof(t, s)
	if fresh == old {
		t.Fatal("released token survived intervening in-window write")
	}
	if !bytes.Equal(proofPointBytes(t, s, "proof"), proofPointBytes(t, oracle, "proof")) {
		t.Fatal("point bytes differ from unreleased oracle")
	}
	// A second scope released in the same indexed page sees preceding removals.
	if _, _, err := s.MutationProof(t.Context(), "proof", 9950, 9970); err != nil {
		t.Fatal(err)
	}
	results := s.ReleaseMutationProofs(t.Context(), []ProofRelease{{Name: "proof", From: 9890, Until: 9910}, {Name: "proof", From: 9950, Until: 9970}})
	if results[0].Released != 1 || results[1].Released != 1 {
		t.Fatal(results)
	}
	assertNoProofHistory(t, s, "proof")
	if err := s.Delete(t.Context(), "proof"); err != nil {
		t.Fatal(err)
	}
	seedProof(t, s)
	if r := releaseProof(t, s, ProofRelease{Name: "proof", ID: m.ID, Generation: m.Generation}); r.Status != "conflict" {
		t.Fatal(r)
	}
	if p, _ := windowProof(t, s); p == fresh {
		t.Fatal("recreate retained token")
	}
}

func TestProofResetDurableNamespace(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprint(crash), func(t *testing.T) {
			strict := vfs.NewStrictMem()
			s, _ := openTestStore(t, "/proof-reset", Options{fs: strict, ExactBlocks: true, SyncInterval: time.Hour})
			createTestMetric(t, s, "proof")
			if err := s.UpdateMany(t.Context(), "proof", []Point{{Timestamp: s.now().Unix() - 60, Value: 1.25}}); err != nil {
				t.Fatal(err)
			}
			identityBefore, c, err := s.db.Get(proofIdentityKey)
			if err != nil {
				t.Fatal(err)
			}
			identityBefore = bytes.Clone(identityBefore)
			c.Close()
			lineageBefore, err := s.readProofFile(proofLineageFile)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.MutationProof(t.Context(), "proof", 1, 100); err != nil {
				t.Fatal(err)
			}
			m, _ := s.Metadata(t.Context(), "proof")
			before := proofPointBytes(t, s, "proof")
			lineage := s.proofLineage
			for _, key := range []string{"proof/ranges", "proof/ranges0", "proof/identity-sentinel", "receipt/proof-test", "lineage/proof-test"} {
				if err := s.db.Set([]byte(key), []byte("untouched"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := s.ResetMutationProofs(t.Context())
			if err != nil || receipt.Finished.IsZero() {
				t.Fatal(receipt, err)
			}
			identityAfter, c, err := s.db.Get(proofIdentityKey)
			if err != nil {
				t.Fatal(err)
			}
			sameIdentity := bytes.Equal(identityBefore, identityAfter)
			c.Close()
			lineageAfter, err := s.readProofFile(proofLineageFile)
			if err != nil {
				t.Fatal(err)
			}
			if !sameIdentity || !bytes.Equal(lineageBefore, lineageAfter) {
				t.Fatal("reset changed persisted identity or lineage")
			}
			if s.proofLineage != lineage {
				t.Fatal("reset changed lineage")
			}
			assertNoProofHistory(t, s, "proof")
			closeForRecovery(t, s, strict, crash)
			s, _ = openTestStore(t, "/proof-reset", Options{fs: strict, ExactBlocks: true})
			defer closeTestStore(t, s)
			assertNoProofHistory(t, s, "proof")
			after, _ := s.Metadata(t.Context(), "proof")
			if after.ID != m.ID || after.Generation != m.Generation || after.Revision != m.Revision || !bytes.Equal(before, proofPointBytes(t, s, "proof")) {
				t.Fatal("reset changed metric")
			}
			for _, key := range []string{"proof/ranges", "proof/ranges0", "proof/identity-sentinel", "receipt/proof-test", "lineage/proof-test"} {
				v, c, e := s.db.Get([]byte(key))
				if e != nil {
					t.Fatal(e)
				}
				got := string(v)
				c.Close()
				if got != "untouched" {
					t.Fatal(key)
				}
			}
		})
	}
}

func TestProofReleaseDrainsStagedSameNameUpdates(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(fmt.Sprint(reset), func(t *testing.T) {
			codec := &pausedUpdateCodec{name: "held", entered: make(chan struct{}), release: make(chan struct{})}
			s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: codec, MetadataCacheSize: 8 << 20})
			defer closeTestStore(t, s)
			oracle, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: batchTestCodec{}, MetadataCacheSize: 8 << 20})
			defer closeTestStore(t, oracle)
			createTestMetric(t, oracle, "held")
			if _, _, err := oracle.MutationProof(t.Context(), "held", 1, 100); err != nil {
				t.Fatal(err)
			}
			if err := oracle.UpdateBatch(t.Context(), []MetricUpdate{{Name: "held", Points: []Point{{Value: 1}}}, {Name: "held", Points: []Point{{Value: 2}}}, {Name: "held", Points: []Point{{Value: 3}}}}, nil); err != nil {
				t.Fatal(err)
			}
			createTestMetric(t, s, "held")
			if _, _, err := s.MutationProof(t.Context(), "held", 1, 100); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- s.UpdateBatch(t.Context(), []MetricUpdate{{Name: "held", Points: []Point{{Value: 1}}}, {Name: "held", Points: []Point{{Value: 2}}}}, nil)
			}()
			<-codec.entered
			retired := make(chan error, 1)
			go func() {
				if reset {
					_, err := s.ResetMutationProofs(t.Context())
					retired <- err
				} else {
					r := s.ReleaseMutationProofs(t.Context(), []ProofRelease{{Name: "held"}})
					if r[0].Status != "released" {
						retired <- fmt.Errorf("%+v", r)
					} else {
						retired <- nil
					}
				}
			}()
			select {
			case err := <-retired:
				close(codec.release)
				<-done
				t.Fatalf("retirement overtook staged history: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(codec.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := <-retired; err != nil {
				t.Fatal(err)
			}
			assertNoProofHistory(t, s, "held")
			before := s.ProofStats()
			if err := s.UpdateBatch(t.Context(), []MetricUpdate{{Name: "held", Points: []Point{{Value: 3}}}}, nil); err != nil {
				t.Fatal(err)
			}
			after := s.ProofStats()
			if before.TrackedWrites != 2 || after.TrackedWrites != before.TrackedWrites || after.UntrackedWrites != before.UntrackedWrites+1 {
				t.Fatal(before, after)
			}
			m, _ := s.Metadata(t.Context(), "held")
			v, err := s.newContinuationIO(t.Context(), s.db, nil, m).Get([]byte("point"))
			if err != nil {
				t.Fatal(err)
			}
			om, err := oracle.Metadata(t.Context(), "held")
			if err != nil {
				t.Fatal(err)
			}
			want, err := oracle.newContinuationIO(t.Context(), oracle.db, nil, om).Get([]byte("point"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(v, want) {
				t.Fatalf("same-name point bytes differ from unreleased oracle: %x != %x", v, want)
			}
			assertNoProofHistory(t, s, "held")
		})
	}
}
