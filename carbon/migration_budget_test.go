package carbon

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

func writeMigrationLease(t *testing.T, path string, workers int, generation uint64, expires time.Time) {
	t.Helper()
	data, err := json.Marshal(migrationControl{Version: 1, Generation: generation, Workers: workers, ExpiresAt: expires.Unix()})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+".tmp", data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationBudgetResizesAndExpires(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	now := time.Now()
	path := filepath.Join(t.TempDir(), "control.json")
	b := newMigrationBudget(4)
	defer b.stop()
	apply := func(workers int, generation uint64) {
		writeMigrationLease(t, path, workers, generation, now.Add(time.Minute))
		b.refresh(path, now)
	}
	apply(1, 1)
	if !b.acquire() {
		t.Fatal("initial permit unavailable")
	}
	waiter := make(chan bool, 1)
	go func() { waiter <- b.acquire() }()
	select {
	case <-waiter:
		t.Fatal("exceeded the active limit")
	case <-time.After(20 * time.Millisecond):
	}
	apply(2, 2)
	select {
	case ok := <-waiter:
		if !ok {
			t.Fatal("resize stopped the worker")
		}
	case <-time.After(time.Second):
		t.Fatal("increase did not admit the waiting worker")
	}
	apply(0, 3)
	b.release()
	b.release()
	go func() { waiter <- b.acquire() }()
	select {
	case <-waiter:
		t.Fatal("paused migration admitted work")
	case <-time.After(20 * time.Millisecond):
	}
	apply(4, 4)
	select {
	case ok := <-waiter:
		if !ok {
			t.Fatal("resume stopped the worker")
		}
	case <-time.After(time.Second):
		t.Fatal("resume lost its waiting work")
	}
	b.release()
	apply(3, 2) // A stale controller cannot raise the limit.
	if limit, _, _ := b.status(); limit != 1 {
		t.Fatal("stale generation accepted")
	}
	apply(4, 5)
	b.refresh(path, now.Add(2*time.Minute))
	if limit, _, _ := b.status(); limit != 1 {
		t.Fatal("expired lease retained extra resources")
	}
	apply(0, 6)
	go func() { waiter <- b.acquire() }()
	b.stop()
	select {
	case ok := <-waiter:
		if ok {
			t.Fatal("stopped budget admitted work")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation stranded a waiting worker")
	}
}

func TestMigrationControlsPreserveTraversalAndLiveWrites(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	control := filepath.Join(t.TempDir(), "control.json")
	expires := time.Now().Add(2 * time.Minute)
	writeMigrationLease(t, control, 0, 1, expires)
	app, cfg, cold := liveMigrationFixture(t, func(cfg *Config) {
		cfg.Whisper.StoreMigrationWorkers = 4
		cfg.Whisper.StoreMigrationControlFile = control
	})
	m := app.liveStoreMigration.Load()
	waitMigration(t, func() bool { return m.budget.Load() != nil })
	b := m.budget.Load()
	if limit, _, _ := b.status(); limit != 0 {
		t.Fatal("initial pause ignored")
	}
	now := time.Now().Unix() - 20
	app.Cache.Add(points.OnePoint("live", 73.5, now))
	waitMigration(t, func() bool {
		x, err := app.MetricStore.Snapshot(context.Background(), "live")
		return err == nil && len(x.Archives[0].Points) == 1 && x.Archives[0].Points[0].Value == 73.5
	})
	writeMigrationLease(t, control, 2, 2, expires)
	waitMigration(t, func() bool { limit, active, _ := b.status(); return limit == 2 && active > 0 })
	writeMigrationLease(t, control, 0, 3, expires)
	waitMigration(t, func() bool { limit, _, _ := b.status(); return limit == 0 })
	if err := syscall.Flock(int(cold.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	waitMigration(t, func() bool { _, active, _ := b.status(); return active == 0 })
	if app.sharedStoreReads.Load() {
		t.Fatal("paused audit published shared reads")
	}
	writeMigrationLease(t, control, 4, 4, expires)
	waitMigration(t, app.sharedStoreReads.Load)
	if app.liveStoreMigration.Load() != m {
		t.Fatal("resource change replaced traversal state")
	}
	want, err := whisperio.ReadSnapshot(context.Background(), "cold", filepath.Join(cfg.Whisper.DataDir, "cold.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := app.MetricStore.Snapshot(context.Background(), "cold")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EqualSnapshots(want, got); err != nil {
		t.Fatal("resized traversal did not repair its original", err)
	}
}
