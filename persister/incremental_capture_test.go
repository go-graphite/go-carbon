package persister

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestIncrementalCaptureShardsKeepRootIdleDuringConcurrentConversions(t *testing.T) {
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	source := filepath.Join(p.rootPath, "metric.wsp")
	want, err := whisperio.ReadSnapshot(ctx, "metric", source)
	if err != nil {
		t.Fatal(err)
	}
	main, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	sidecar, err := os.ReadFile(whisper.OutOfOrderSidecarPath(source))
	if err != nil {
		t.Fatal("fixture has no OOO sidecar: ", err)
	}
	const metrics = 16
	names := []string{"metric"}
	for i := 1; i < metrics; i++ {
		name := fmt.Sprintf("metric%d", i)
		path := filepath.Join(p.rootPath, name+".wsp")
		if err := os.WriteFile(path, main, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(whisper.OutOfOrderSidecarPath(path), sidecar, 0600); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	entries, err := os.ReadDir(p.incrementalScratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 256 {
		t.Fatalf("capture root has %d entries, want 256 shard parents", len(entries))
	}
	// A fixed old mtime detects even transient mkdir/rmdir entries that a
	// before/after directory listing would miss, without polling or sleeps.
	stamp := time.Unix(946684800, 0)
	shards := make(map[string]os.FileInfo, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			t.Fatal("capture root contains a non-directory: ", entry.Name())
		}
		path := filepath.Join(p.incrementalScratch, entry.Name())
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
		shards[path], err = os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(p.incrementalScratch, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(p.incrementalScratch)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, metrics)
	for _, name := range names {
		group.Go(func() {
			<-start
			_, err := p.ConvertFile(ctx, name)
			errs <- err
		})
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(p.incrementalScratch)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("conversions created or removed entries directly under the capture root")
	}
	used := 0
	for path, before := range shards {
		after, err := os.Stat(path)
		if err != nil || !os.SameFile(before, after) {
			t.Fatalf("conversion removed/recreated shard parent %s: %v", path, err)
		}
		if !before.ModTime().Equal(after.ModTime()) {
			used++
		}
		if entries, err := os.ReadDir(path); err != nil || len(entries) != 0 {
			t.Fatalf("private capture not cleaned up under %s: %v %v", path, entries, err)
		}
	}
	if used < 2 {
		t.Fatalf("conversions used only %d shard parents", used)
	}
	for _, name := range names {
		got, err := db.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		want.Metadata.Name = name
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		assertOriginalsReclaimed(t, p, name)
	}
	t.Logf("%d OOO conversions used %d shard parents; capture root unchanged", metrics, used)
}

// The shared store uses the same store-open cleanup: recursively remove the reserved root,
// without interpreting its entries. Flat and sharded leftovers are therefore
// disposable in both versions, regardless of the durable ownership boundary.
func TestIncrementalCaptureScratchRecoveryAndR21Layout(t *testing.T) {
	for _, boundary := range []string{"staged", "owned"} {
		t.Run(boundary, func(t *testing.T) {
			p, _, db, destination := incrementalPersisterFixture(t)
			t.Cleanup(func() {
				if db != nil {
					_ = db.Close()
				}
			})
			ctx := context.Background()
			want, err := whisperio.ReadSnapshot(ctx, "metric", filepath.Join(p.rootPath, "metric.wsp"))
			if err != nil {
				t.Fatal(err)
			}
			stop := errors.New("interrupted conversion")
			if _, err := p.convertFile(ctx, "metric", func(stage string) error {
				if stage == boundary {
					return stop
				}
				return nil
			}); !errors.Is(err, stop) {
				t.Fatalf("did not interrupt %s: %v", boundary, err)
			}
			err = db.Close()
			db = nil
			if err != nil {
				t.Fatal(err)
			}
			// A crash can leave the old flat layout alongside newer shards.
			for _, relative := range []string{"go-whisper-ooo-snapshot-flat", "7f/go-whisper-ooo-snapshot-sharded"} {
				dir := filepath.Join(p.incrementalScratch, filepath.FromSlash(relative))
				if err := os.MkdirAll(dir, 0750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "partial.wsp"), []byte("not a valid capture"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			keep := filepath.Join(destination, "operator-receipt.json")
			if err := os.WriteFile(keep, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath, SyncInterval: time.Hour}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(p.incrementalScratch); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("startup retained abandoned scratch: %v", err)
			}
			if data, err := os.ReadFile(keep); err != nil || string(data) != "keep" {
				t.Fatalf("startup changed an unrelated file: %q %v", data, err)
			}
			p.SetMetricStore(db)
			if err := p.SetIncrementalMigration(db, destination); err != nil {
				t.Fatal(err)
			}
			result, err := p.ConvertFile(ctx, "metric")
			if err != nil || result.PreviouslyOwned != (boundary == "owned") {
				t.Fatalf("resume %s: %+v %v", boundary, result, err)
			}
			got, err := db.Snapshot(ctx, "metric")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal("scratch cleanup changed durable history: ", err)
			}
			assertOriginalsReclaimed(t, p, "metric")
		})
	}
}
