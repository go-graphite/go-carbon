package chunkstore_test

import (
	"context"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Explicit externally supplied real wire fixtures stay out of the repository.
// Their donor and target bytes are the independent production differential.
func TestAnchorRepairProductionWire(t *testing.T) {
	dir := os.Getenv("ANCHOR_PRODUCTION_EVIDENCE")
	if dir == "" {
		t.Skip("set ANCHOR_PRODUCTION_EVIDENCE to saved source/destination wires")
	}
	read := func(side string) []store.TransferMetric {
		t.Helper()
		f, err := os.Open(filepath.Join(dir, side+".wire"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		r, err := store.ReadTransfer(f, 160<<20)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	donor, original := read("source"), read("destination")
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncached", true: "cached"}[cached], func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1793984000, 0)
			var budget int64
			if cached {
				budget = 8 << 20
			}
			target := transferStoreCachedAt(t, true, true, budget, now)
			oracle := transferStoreCachedAt(t, true, true, budget, now)
			for _, db := range []*store.Store{target, oracle} {
				if _, err := db.ImportTransfer(ctx, original); err != nil {
					t.Fatal(err)
				}
			}
			name := original[0].Config.Name
			before, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range before.Archives {
				series, err := target.Fetch(ctx, name, int(now.Unix())-a.Retention.Step*a.Retention.Count+2*a.Retention.Step, int(now.Unix())-1)
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range series.Values {
					if !math.IsNaN(v) {
						t.Fatal("fixture does not reproduce empty render")
					}
				}
			}
			dry, err := target.RepairArchiveAnchors(ctx, name, false)
			if err != nil {
				t.Fatal(err)
			}
			if dry.Applied || len(dry.Archives) == 0 || !dry.Archives[0].Changed {
				t.Fatalf("dry result %+v", dry)
			}
			unchanged, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(before, unchanged); err != nil {
				t.Fatal(err)
			}
			applied, err := target.RepairArchiveAnchors(ctx, name, true)
			if err != nil || !applied.Applied {
				t.Fatalf("apply %+v %v", applied, err)
			}
			after, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(before, after); err != nil {
				t.Fatal("repair changed physical points", err)
			}
			if _, err := oracle.ImportTransfer(ctx, donor); err != nil {
				t.Fatal(err)
			}
			expectation, err := oracle.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(expectation, after); err != nil {
				t.Fatal("fixed importer differential", err)
			}
			for _, a := range after.Archives {
				from := int(now.Unix()) - a.Retention.Step*a.Retention.Count + 2*a.Retention.Step
				until := int(now.Unix()) - 1
				got, err := target.Fetch(ctx, name, from, until)
				if err != nil {
					t.Fatal(err)
				}
				want, err := oracle.Fetch(ctx, name, from, until)
				if err != nil {
					t.Fatal(err)
				}
				if got.Step != want.Step || got.FromTime != want.FromTime || len(got.Values) != len(want.Values) {
					t.Fatal("render shape mismatch")
				}
				count := 0
				for i, v := range got.Values {
					if math.Float64bits(v) != math.Float64bits(want.Values[i]) {
						t.Fatalf("render bit mismatch %d", i)
					}
					if !math.IsNaN(v) {
						count++
					}
				}
				if count != 10 {
					t.Fatalf("got %d render points, want all 10", count)
				}
			}
			again, err := target.RepairArchiveAnchors(ctx, name, true)
			if err != nil || again.Applied {
				t.Fatalf("not idempotent %+v %v", again, err)
			}
			if err := target.UpdateManyForArchive(ctx, name, []store.Point{{Timestamp: 1793983800, Value: 123}}, original[0].Config.Retentions[0].Step*original[0].Config.Retentions[0].Count); err != nil {
				t.Fatal(err)
			}
			last, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, p := range last.Archives[0].Points {
				if p.Timestamp == 1793983800 && p.Value == 123 {
					found = true
				}
			}
			if !found {
				t.Fatal("foreground point lost")
			}
		})
	}
}

type racingAnchorCodec struct {
	*whisperio.ContinuationCodec
	once sync.Once
	race func()
}

func (c *racingAnchorCodec) PlanArchiveAnchors(ctx context.Context, kv store.ContinuationIO) (store.AnchorRepairPlan, error) {
	p, e := c.ContinuationCodec.PlanArchiveAnchors(ctx, kv)
	c.once.Do(func() {
		if c.race != nil {
			c.race()
		}
	})
	return p, e
}

func TestAnchorRepairRetriesRacingLiveWrite(t *testing.T) {
	dir := os.Getenv("ANCHOR_PRODUCTION_EVIDENCE")
	if dir == "" {
		t.Skip("explicit production wire fixture required")
	}
	f, err := os.Open(filepath.Join(dir, "destination.wire"))
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.ReadTransfer(f, 160<<20)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	codec := &racingAnchorCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
	db, err := store.Open(filepath.Join(t.TempDir(), "pool-slow"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, CacheSize: 1 << 20, MemTableSize: 1 << 20, MetadataCacheSize: 8 << 20, Now: func() time.Time { return time.Unix(1793984000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	name := records[0].Config.Name
	var raced store.Snapshot
	codec.race = func() {
		if err := db.UpdateManyForArchive(ctx, name, []store.Point{{Timestamp: 1793983800, Value: 123}}, records[0].Config.Retentions[0].Step*records[0].Config.Retentions[0].Count); err != nil {
			t.Fatal(err)
		}
		raced, err = db.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err := db.RepairArchiveAnchors(ctx, name, true)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(raced, snap); err != nil {
		t.Fatal("racing repair changed stored points", err)
	}
	found := false
	for _, p := range snap.Archives[0].Points {
		if p.Timestamp == 1793983800 && p.Value == 123 {
			found = true
		}
	}
	if !found {
		t.Fatalf("racing write lost %+v", result)
	}
}

func TestAnchorRepairDurableBeforeAcknowledgement(t *testing.T) {
	evidence := os.Getenv("ANCHOR_PRODUCTION_EVIDENCE")
	if evidence == "" {
		t.Skip("explicit production wire fixture required")
	}
	root := os.Getenv("ANCHOR_CRASH_ROOT")
	open := func(root string) *store.Store {
		t.Helper()
		db, err := store.Open(root, store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(1793984000, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	if root != "" {
		db := open(root)
		name := os.Getenv("ANCHOR_CRASH_NAME")
		result, err := db.RepairArchiveAnchors(context.Background(), name, true)
		if err != nil || !result.Applied {
			t.Fatalf("repair %+v %v", result, err)
		}
		// Deliberately bypass Close and its WAL sync, like sudden process loss
		// immediately after the API has acknowledged durable publication.
		os.Exit(0)
	}
	root = filepath.Join(t.TempDir(), "pool-slow")
	db := open(root)
	f, err := os.Open(filepath.Join(evidence, "destination.wire"))
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.ReadTransfer(f, 160<<20)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	name := records[0].Config.Name
	if _, err := db.ImportTransfer(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	before, err := db.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAnchorRepairDurableBeforeAcknowledgement$")
	cmd.Env = append(os.Environ(), "ANCHOR_CRASH_ROOT="+root, "ANCHOR_CRASH_NAME="+name)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash subprocess %v: %s", err, output)
	}
	db = open(root)
	defer db.Close()
	after, err := db.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
	dry, err := db.RepairArchiveAnchors(context.Background(), name, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range dry.Archives {
		if a.Changed || a.Blocked != "" {
			t.Fatalf("repair was not durable %+v", dry)
		}
	}
}
