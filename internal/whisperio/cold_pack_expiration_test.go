package whisperio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Use an actual imported file: a raw catalog record intentionally exists
// before ownership publication, with a complete namespace and manifest.
func TestColdPackUnpublishedWhisperCandidate(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(source, "staged.wsp")
	o := coldOptions(&ContinuationCodec{})
	o.IncrementalSource = source
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1m:7d,1h:1y"), whisper.Average, 0,
		&whisper.Options{Compressed: true, OutOfOrder: true, Now: o.Now, PointsPerBlock: 256})
	if err != nil {
		t.Fatal(err)
	}
	var points []*whisper.TimeSeriesPoint
	for i := range 1800 {
		points = append(points, &whisper.TimeSeriesPoint{Time: int(coldBase) + i*60, Value: float64(i) / 7})
	}
	if err := errors.Join(w.UpdateMany(points), w.Close()); err != nil {
		t.Fatal(err)
	}
	capture, err := ReadSnapshotIn(ctx, "staged", path, root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := openTestStore(filepath.Join(root, "store"), o)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	defer db.Close()
	staged, err := db.StageFileSnapshot(ctx, capture)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", CPUFraction: 1, BytesPerSecond: 1 << 40, WriteWindow: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := db.RunColdPack(ctx); err != nil {
		t.Fatalf("unpublished candidate poisoned pass: %v", err)
	}
	if st := db.ColdPackStats(); st.Errors != 0 || st.Blocks != 0 || st.Passes != 1 {
		t.Fatalf("unpublished state was rewritten: %+v", st)
	}
	if _, err := db.Fetch(ctx, "staged", int(coldBase), int(coldBase+1800*60)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unverified state exposed to reads: %v", err)
	}
	if _, err := db.PublishFileOwnership(ctx, capture, staged); err != nil {
		t.Fatalf("maintenance changed staged writer state: %v", err)
	}
	coldSweep(t, db, "exact")
	got, err := db.Snapshot(ctx, "staged")
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, capture, got, false)
}

// A saved descriptor cursor names only a position. Expiration removes its
// entire generation atomically; recreation gets a fresh ID and complete state.
func TestColdPackExpirationRecreation(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			ctx := context.Background()
			db, err := openTestStore(t.TempDir(), coldOptions(&ContinuationCodec{ReuseBlockKeys: true}))
			if err != nil {
				t.Fatal(err)
			}
			db.StopBackground()
			defer db.Close()
			want := coldFixture(t, db)
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", CPUFraction: 1, BytesPerSecond: 1 << 40, WriteWindow: time.Second}); err != nil {
				t.Fatal(err)
			}
			for n := 0; db.ColdPackStats().BlockCursor == 0 && n < 10; n++ {
				if _, err := db.ColdPackStep(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if db.ColdPackStats().BlockCursor == 0 {
				t.Fatal("fixture did not save an intra-metric cursor")
			}
			old, err := db.Metadata(ctx, want.Metadata.Name)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, err := db.DeleteBatchIfUnchanged(ctx, []store.Metadata{old})
			if err != nil || len(outcomes) != 1 || outcomes[0] != nil {
				t.Fatalf("expiration: %v %v", outcomes, err)
			}
			if _, err := db.Snapshot(ctx, old.Name); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("deleted state still readable: %v", err)
			}
			if recreate {
				want = coldFixture(t, db)
				if want.Metadata.ID == old.ID {
					t.Fatal("recreation reused expired identity")
				}
			}
			for range 2 { // finish saved cursor, then visit recreated names from zero
				if err := db.RunColdPack(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if recreate {
				got, err := db.Snapshot(ctx, old.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, want, got, false)
				if _, err := db.Fetch(ctx, old.Name, int(coldBase), int(coldBase+1800*60)); err != nil {
					t.Fatalf("recreated render failed: %v", err)
				}
			}
			if st := db.ColdPackStats(); st.Errors != 0 || st.Passes != 2 {
				t.Fatalf("deleted cursor poisoned passes: %+v", st)
			}
		})
	}
}
