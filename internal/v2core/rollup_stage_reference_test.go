package v2core

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Stage IDs follow input order, independently of name/routing order. Resolve
// records through their owners in the reference path, so a wrong stage binding
// in the compact writer cannot silently change both sides of the differential.
func TestPlannedStageReferenceDifferential(t *testing.T) {
	r := newCoreRollups()
	var batches []Batch
	for i, name := range []v2part.ID{"stage.z", "stage.a", "stage.longer.name"} {
		policy, err := EncodeRollupPolicy(RollupPolicy{Name: name, Group: "g", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}}}})
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, Batch{ID: uint64(i + 1), Policy: policy, Order: uint64(i + 1), Clock: 1700000100, Points: []Row{{Time: 1700000000, Bits: uint64(i + 1), Seq: 1}}})
	}
	plan, err := r.planParts(SealInput{Batches: batches})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	for i := range plan.Parts {
		plan.Parts[i].Spec.Source.Incarnation = v2part.PartID{1}
		plan.Parts[i].Spec.Source.Stream = "stage-test"
	}
	files, specs := expandPlannedParts(plan.Parts, plannedOwners(r))
	dir := t.TempDir()
	for i, part := range plan.Parts {
		oldPath, newPath := filepath.Join(dir, "old.sst"), filepath.Join(dir, "new.sst")
		if _, err = writePlannedRollupPart(oldPath, files[i], specs[i]); err != nil {
			t.Fatal(err)
		}
		if _, err = writePlannedRollupTuples(newPath, part); err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile(oldPath)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(newPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("stage reference changed dense bytes")
		}
		if err = os.Remove(oldPath); err != nil {
			t.Fatal(err)
		}
		if err = os.Remove(newPath); err != nil {
			t.Fatal(err)
		}
	}
	if len(plan.Parts) == 0 {
		t.Fatal("no stage references exercised")
	}
}
