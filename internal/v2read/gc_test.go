package v2read

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"

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
	// The reader closes each snapshot once parts are open, so the hook reads
	// every sealed publication's files and a fresh directory first.
	var mu sync.Mutex
	files := map[uint64][]v2core.SnapshotPart{}
	freshFound := map[uint64]bool{}
	publish := func(owner int, snap *pebble.Snapshot, pending v2core.PendingView, number uint64, inventory *v2core.RollupInventory) error {
		if snap != nil {
			parts, err := (v2core.SealedSnapshotReader{Snapshot: snap, Directory: filepath.Join(root, "owner-000")}).ReadPartFiles(context.Background())
			if err != nil {
				t.Error(err)
			}
			fresh, err := v2core.NewRollupSnapshot(context.Background(), snap, nil, nil, nil)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			files[number] = parts
			if fresh != nil {
				_, _, freshFound[number] = fresh.LookupMetadata("gc.reader")
			}
			mu.Unlock()
		}
		return r.PublishPending(owner, snap, pending, number, inventory)
	}
	e, err := v2core.Open(root, testConfig(), v2core.Hooks{PublishPending: publish})
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
		mu.Lock()
		defer mu.Unlock()
		return files[g.number]
	}
	if old.load(); old.err != nil {
		t.Fatal(old.err)
	}
	initial := inventory(old)
	if len(initial) != 2 {
		t.Fatalf("initial files %d", len(initial))
	}
	b, err := os.ReadFile(initial[1].Path)
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
	await(t, func() bool { return r.owners[0].Load().number != old.number })
	current := r.owners[0].Load()
	if !current.retain() {
		t.Fatal("cannot pin current reader")
	}
	defer current.release()
	if current.number <= old.number {
		t.Fatal("retirement reused the old sealed generation")
	}
	if len(inventory(current)) != 0 {
		t.Fatal("current reader retains retired inventory")
	}
	if old.snapshot != nil || current.snapshot != nil {
		t.Fatal("reader kept a snapshot after opening parts")
	}
	if current.load(); current.err != nil {
		t.Fatal(current.err)
	}
	if _, _, ok := current.sealed.LookupMetadata("gc.reader"); ok {
		t.Fatal("retired final part resurrected from GCP3 keys")
	}
	if _, _, ok := old.sealed.LookupMetadata("gc.reader"); !ok {
		t.Fatal("old pinned directory changed after retirement")
	}
	// The retired part stays readable through the old publication's registry
	// references, with no snapshot behind it.
	if records, err := old.sealed.Records(context.Background(), "gc.reader"); err != nil || len(records) == 0 {
		t.Fatalf("pinned reader lost the retired part: %d records, %v", len(records), err)
	}
	mu.Lock()
	resurrected := freshFound[current.number]
	mu.Unlock()
	if resurrected {
		t.Fatal("fresh directory resurrected a retired part")
	}
	if e.DurableFrontiers()[0][0] != 1 {
		t.Fatal("retirement advanced input frontier")
	}
}
