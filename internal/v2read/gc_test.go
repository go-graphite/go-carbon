package v2read

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestRetirementPublishesNewSealedGenerationWithoutInput(t *testing.T) {
	root := t.TempDir()
	r, err := New(1, 1<<30, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	e, err := v2core.Open(root, testConfig(), v2core.Hooks{PublishBatches: r.Publish})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID("gc.reader"), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.AppendBatches(0, []v2core.Batch{{ID: 7, Order: 1, Clock: 1700000002, Policy: policy, Points: []v2core.Row{{ID: 7, Time: 1700000000, Bits: 1, Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	old := r.owners[0].Load()
	if !old.retain() {
		t.Fatal("cannot pin old reader")
	}
	defer old.release()
	inventory := func(g *generation) []v2core.SnapshotPart {
		files, err := (v2core.SealedSnapshotReader{Snapshot: g.sealed.snapshot, Directory: filepath.Join(root, "owner-000")}).ReadPartFiles(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	old.sealed.once.Do(old.sealed.load)
	if old.sealed.err != nil {
		t.Fatal(old.sealed.err)
	}
	files := inventory(old)
	if len(files) != 2 {
		t.Fatalf("initial files %d", len(files))
	}
	b, err := os.ReadFile(files[1].Path)
	if err != nil {
		t.Fatal(err)
	}
	d, err := v2part.ParseDescriptor(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.RetireSealedParts(0, []v2part.Digest{d.SHA256}, func(v2part.Descriptor) error { return nil }); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return r.owners[0].Load().sealed != old.sealed })
	current := r.owners[0].Load()
	if !current.retain() {
		t.Fatal("cannot pin current reader")
	}
	defer current.release()
	if current.sealed.number <= old.sealed.number {
		t.Fatal("retirement reused the old sealed generation")
	}
	if len(inventory(current)) != 0 {
		t.Fatal("current reader retains retired inventory")
	}
	if len(inventory(old)) != 2 {
		t.Fatal("pinned reader lost retired inventory")
	}
	current.sealed.once.Do(current.sealed.load)
	if current.sealed.err != nil {
		t.Fatal(current.sealed.err)
	}
	if _, _, ok := current.sealed.index.LookupMetadata("gc.reader"); ok {
		t.Fatal("retired final part resurrected from GCP3 keys")
	}
	if _, _, ok := old.sealed.index.LookupMetadata("gc.reader"); !ok {
		t.Fatal("old pinned directory changed after retirement")
	}
	fresh, err := v2core.NewRollupSnapshot(context.Background(), current.sealed.snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := fresh.LookupMetadata("gc.reader"); ok {
		t.Fatal("fresh directory resurrected a retired part")
	}
	if e.DurableFrontiers()[0][0] != 1 {
		t.Fatal("retirement advanced input frontier")
	}
}
