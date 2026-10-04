package persister

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

// Migration is also persistence: imports must include an OOO sidecar, preserve
// the source, and produce classic-readable exports after restarting Pebble.
func TestStorageTransferRoundTrip(t *testing.T) {
	now := storageTestClock(t)
	ctx := context.Background()
	for _, kind := range storageBackends[:3] {
		t.Run(kind, func(t *testing.T) {
			source := newStorageBackend(t, kind, now)
			oracle := newStorageBackend(t, "classic", now)
			candidate := newStorageBackend(t, "pebble-chunk", now)
			c := storageConfig("metric", "1s:10m", whisper.Average, 0.5)
			for _, engine := range []*storageBackend{source, oracle} {
				storageMust(t, engine.create(c))
				for _, batch := range storageRecoveryBatches(kind) {
					storageMust(t, engine.update(c.Name, batch))
				}
			}
			paths := []string{source.path(c.Name)}
			if kind == "cwhisper-ooo" {
				paths = append(paths, whisper.OutOfOrderSidecarPath(source.path(c.Name)))
			}
			before := make(map[string][32]byte)
			for _, path := range paths {
				data, err := os.ReadFile(path)
				storageMust(t, err)
				before[path] = sha256.Sum256(data)
			}
			_, err := whisperio.New(candidate.db).ImportWSP(ctx, c.Name, source.path(c.Name), false)
			storageMust(t, err)
			storageMust(t, candidate.reopen())
			storageCompare(t, oracle, candidate, c, "imported-and-reopened")
			exported := &storageBackend{kind: "classic", dir: t.TempDir(), now: now}
			storageMust(t, whisperio.New(candidate.db).ExportWSP(ctx, c.Name, exported.path(c.Name)))
			storageCompare(t, oracle, exported, c, "exported")
			for _, path := range paths {
				data, err := os.ReadFile(path)
				storageMust(t, err)
				if sha256.Sum256(data) != before[path] {
					t.Fatalf("import modified source %s", filepath.Base(path))
				}
			}
		})
	}
}

func TestStoragePebbleArchivePersistence(t *testing.T) {
	now := storageTestClock(t)
	ctx := context.Background()
	s := newStorageBackend(t, "pebble-chunk", now)
	c := storageConfig("metric", "1s:1m,10s:10m,60s:1h", whisper.Sum, 0.5)
	oracle := newStorageBackend(t, "classic", now)
	storageMust(t, oracle.create(c))
	// Populate each archive separately, including non-overlapping historical
	// ranges that a render-based export would silently resample or omit.
	for _, input := range [][]whisper.TimeSeriesPoint{
		storagePoints(storageEpoch-3000, 30, 60),
		storagePoints(storageEpoch-500, 30, 10),
		storagePoints(storageEpoch-50, 40, 1),
	} {
		storageMust(t, oracle.update(c.Name, input))
	}
	_, err := whisperio.New(s.db).ImportWSP(ctx, c.Name, oracle.path(c.Name), false)
	storageMust(t, err)
	want, err := s.db.Snapshot(ctx, c.Name)
	storageMust(t, err)
	for i, archive := range want.Archives {
		if len(archive.Points) == 0 {
			t.Fatalf("archive %d was not exercised", i)
		}
	}
	storageMust(t, s.compact([]string{c.Name}))
	storageMust(t, s.reopen())
	got, err := s.db.Snapshot(ctx, c.Name)
	storageMust(t, err)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("metadata or physical archive slots changed after compaction/reopen")
	}
	exported := &storageBackend{kind: "classic", dir: t.TempDir(), now: now}
	storageMust(t, whisperio.New(s.db).ExportWSP(ctx, c.Name, exported.path(c.Name)))
	w, err := whisper.Open(exported.path(c.Name))
	storageMust(t, err)
	t.Cleanup(func() { storageMust(t, w.Close()) })
	for i, archive := range want.Archives {
		points, err := w.ArchivePoints(i)
		storageMust(t, err)
		if !reflect.DeepEqual(whisperArchivePoints(archive), points) {
			t.Fatalf("export changed physical archive %d", i)
		}
	}
	storageCompare(t, oracle, s, c, "archive-import-reopen")
	storageCompare(t, oracle, exported, c, "archive-export")

	// Delete/recreate the name twice: old generations must never resurrect after
	// reopening.
	for cycle := 0; cycle < 2; cycle++ {
		storageMust(t, s.db.Delete(ctx, c.Name))
		storageMust(t, s.reopen())
		if _, err := s.db.Metadata(ctx, c.Name); !errors.Is(err, chunkstore.ErrNotFound) {
			t.Fatalf("deleted metric remains: %v", err)
		}
		storageMust(t, s.create(c))
		storageMust(t, s.update(c.Name, []whisper.TimeSeriesPoint{{Time: storageEpoch - 1, Value: float64(cycle + 1)}}))
		storageMust(t, s.reopen())
		snap, err := s.db.Snapshot(ctx, c.Name)
		storageMust(t, err)
		if len(snap.Archives[0].Points) != 1 || snap.Archives[0].Points[0].Value != float64(cycle+1) {
			t.Fatalf("cycle=%d resurrected old generation: %v", cycle, snap.Archives[0].Points)
		}
		for i := 1; i < len(snap.Archives); i++ {
			if len(snap.Archives[i].Points) != 0 {
				t.Fatalf("cycle=%d resurrected coarse archive %d", cycle, i)
			}
		}
	}
}

func whisperArchivePoints(archive chunkstore.Archive) []whisper.TimeSeriesPoint {
	points := make([]whisper.TimeSeriesPoint, len(archive.Points))
	for i, point := range archive.Points {
		points[i] = whisper.TimeSeriesPoint{Time: int(point.Timestamp), Value: point.Value}
	}
	return points
}
