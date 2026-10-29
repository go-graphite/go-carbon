package storecopy

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/inputlog"
)

type emptyInput struct{}

func (emptyInput) TakeLogEntries(_ int, _ uint64, spare []inputlog.Entry) []inputlog.Entry {
	return spare[:0]
}
func (emptyInput) LogLowWater() uint64         { return 0 }
func (emptyInput) RelogBefore(uint64, int) int { return 0 }
func (emptyInput) LogDropped() uint64          { return 0 }

func TestSourceDiskGuardReleasesRetentionWithoutDeletingMetrics(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "store")
	store, err := chunkstore.Open(directory, chunkstore.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Create(context.Background(), chunkstore.MetricConfig{Name: "kept.metric", AggregationMethod: chunkstore.Average, Retentions: []chunkstore.Retention{{Step: 1, Count: 60}}}); err != nil {
		t.Fatal(err)
	}
	logDir := filepath.Join(root, "log")
	log, err := inputlog.Open(logDir, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	log.Start(emptyInput{}, 32)
	manager := New(store, log, logDir, directory, nil, nil)
	defer manager.Close()
	manager.SetMinimumFreeBytes(1)
	w := httptest.NewRecorder()
	manager.ServeHTTP(w, httptest.NewRequest(http.MethodPost, Endpoint, nil))
	if w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	manager.mu.RLock()
	id, snapshot := manager.manifest.ID, manager.dir
	manager.mu.RUnlock()
	manager.SetMinimumFreeBytes(math.MaxInt64)
	manager.watch(id)
	if manager.Active() {
		t.Fatal("disk guard left copy active")
	}
	if _, err := os.Stat(snapshot); !os.IsNotExist(err) {
		t.Fatal("snapshot links were not released", err)
	}
	if _, err := log.PinCopy(); err != nil {
		t.Fatal("input retention was not released", err)
	}
	if err := log.ReleaseCopy(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Metadata(context.Background(), "kept.metric"); err != nil {
		t.Fatal("source metric was removed", err)
	}
}
