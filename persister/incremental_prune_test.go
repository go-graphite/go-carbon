package persister

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestDeferredReclaimPrunesAfterSyncBeforeCompletion(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "metric.wsp")
	for _, p := range originalPaths(path) {
		if err := os.WriteFile(p, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	db := &reclaimCompletionStore{}
	db.complete = func(_ store.Metadata) error {
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("directory remains at completion: %v", err)
		}
		return nil
	}
	r := reclaimTestQueue()
	r.queued.Store(1)
	p := &Whisper{rootPath: root, incrementalStore: db}
	p.reclaimDirectory(r, dir, []reclaimJob{{name: "a.b.metric", path: path, id: 1, generation: 1}})
	if db.completed.Load() != 1 {
		t.Fatal("cleanup not completed")
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatal("data root removed", err)
	}
}

func TestDeferredReclaimClosesPinnedRootOnStop(t *testing.T) {
	p := &Whisper{rootPath: t.TempDir()}
	p.SetDeferredReclaim(time.Hour)
	file := p.reclaim.root
	if file == nil {
		t.Fatal("reclaim did not pin its root")
	}
	p.StopDeferredReclaim()
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("root fd remains open: %v", err)
	}
}
