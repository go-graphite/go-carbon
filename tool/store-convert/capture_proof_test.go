package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestCaptureProofCrashBetweenCopyAndReplacement(t *testing.T) {
	dir := t.TempDir()
	id := identity{Source: "/original", Copy: "/retained", SourceID: "verified-id", Destination: dir, Live: true}
	if err := publishCaptureReceipt(dir, id, "test verified baseline", 0); err != nil {
		t.Fatal(err)
	}
	p, err := openCaptureProof(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	original := chunkstore.Metadata{ID: 17, Generation: 1, Revision: 1}
	match := func(m chunkstore.Metadata, want bool) {
		t.Helper()
		got, err := p.matches("metric", m)
		if err != nil || got != want {
			t.Fatalf("proof match=%v want=%v error=%v", got, want, err)
		}
	}
	match(original, true)
	if err := p.pending(context.Background(), "metric"); err != nil {
		t.Fatal(err)
	}
	// The source/capture may now match while the destination is still older.
	match(original, false)
	if err := p.db.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = openCaptureProof(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	match(original, false)
	replacement := original
	replacement.Generation++
	replacement.Revision++
	if err := p.verified("metric", replacement); err != nil {
		t.Fatal(err)
	}
	match(replacement, true)
	// A surviving proof cannot validate a lost destination WAL replacement.
	match(original, false)
	updated := replacement
	updated.Revision++
	match(updated, false)
	if err := p.db.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = openCaptureProof(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	defer p.db.Close()
	match(replacement, true)
	match(original, false)
}

func TestCaptureReceiptRejectsWrongIdentityAndPartialAdoption(t *testing.T) {
	dir := t.TempDir()
	id := identity{Source: "/original", Copy: "/retained", SourceID: "verified-id", Destination: dir, Live: true}
	if err := adoptVerifiedBaseline(dir, id); err == nil {
		t.Fatal("unverified/ad hoc destination adopted")
	}
	marker := func(name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("guard"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	marker("NEEDS-CATCHUP")
	marker("CONVERSION-IN-PROGRESS")
	if err := adoptVerifiedBaseline(dir, id); err == nil {
		t.Fatal("partial conversion adopted")
	}
	if err := os.Remove(filepath.Join(dir, "CONVERSION-IN-PROGRESS")); err != nil {
		t.Fatal(err)
	}
	marker("MIRROR-RECONCILE-REQUIRED")
	if err := adoptVerifiedBaseline(dir, id); err == nil {
		t.Fatal("previously served store adopted")
	}
	if err := os.Remove(filepath.Join(dir, "MIRROR-RECONCILE-REQUIRED")); err != nil {
		t.Fatal(err)
	}
	if err := adoptVerifiedBaseline(dir, id); err != nil {
		t.Fatal(err)
	}
	wrong := id
	wrong.SourceID = "different"
	if p, err := openCaptureProof(dir, wrong); err == nil {
		p.db.Close()
		t.Fatal("wrong dataset proof accepted")
	}
}
