package persister

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestIncrementalMigrationIngestsConcurrentFileStages(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
	p.SetFLock(true)
	ctx := context.Background()
	now := time.Now().Unix()
	const metrics = 96
	names := make([]string, metrics)
	want := make(map[string]store.Snapshot, metrics)
	var wantMu sync.Mutex
	prepare := func(i int) error {
		names[i] = fmt.Sprintf("ingest.group%d.metric%d", i%7, i)
		for j := 0; j < 5+i%40; j++ {
			value := float64(i*1000+j) / 8
			if j%5 == 0 {
				value = math.Float64frombits(uint64(i)<<32 | uint64(j))
			}
			cache.add(names[i], now-int64(200-3*j), value)
			p.store(names[i])
		}
		if i%3 == 0 {
			// Out-of-order history creates a sidecar that must be folded exactly.
			cache.add(names[i], now-190, -float64(i))
			p.store(names[i])
		}
		snapshot, err := whisperio.ReadSnapshot(ctx, names[i], filepath.Join(p.rootPath, filepath.FromSlash(fmt.Sprintf("ingest/group%d/metric%d.wsp", i%7, i))))
		if err != nil {
			return err
		}
		wantMu.Lock()
		want[names[i]] = snapshot
		wantMu.Unlock()
		return nil
	}
	var setup sync.WaitGroup
	setupErrors := make(chan error, metrics)
	for w := 0; w < 12; w++ {
		setup.Add(1)
		go func(w int) {
			defer setup.Done()
			for i := w; i < metrics; i += 12 {
				if err := prepare(i); err != nil {
					setupErrors <- err
				}
			}
		}(w)
	}
	setup.Wait()
	close(setupErrors)
	for err := range setupErrors {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "exact")
	open := func() *store.Store {
		db, err := store.Open(destination, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath, IngestFileStages: true, IngestLinger: 20 * time.Millisecond, SyncInterval: time.Hour, TimeBuckets: os.Getenv("GO_CARBON_TEST_TIME_BUCKETS") == "1"})
		if err != nil {
			t.Fatal(err)
		}
		p.SetMetricStore(db)
		if err := p.SetIncrementalMigration(db, destination); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	// Fewer I/O slots than workers: waiting for a group must release them.
	p.SetConversionIOSlots(2)
	// One live non-ingested metric interleaves ordinary ID allocation.
	cache.add("ingest.fresh", now-5, 42)
	p.store("ingest.fresh")
	var wg sync.WaitGroup
	errs := make(chan error, metrics)
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < metrics; i += 12 {
				if _, err := p.ConvertFile(ctx, names[i]); err != nil {
					errs <- fmt.Errorf("%s: %w", names[i], err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	ids := make(map[uint64]string)
	check := func(db *store.Store) {
		t.Helper()
		for _, name := range names {
			m, err := db.Metadata(ctx, name)
			if err != nil || m.FileMigration != store.FileReclaimed || !m.Continuation {
				t.Fatalf("%s: metadata %+v %v", name, m, err)
			}
			if other, ok := ids[m.ID]; ok && other != name {
				t.Fatalf("%s and %s share ID %d", name, other, m.ID)
			}
			ids[m.ID] = name
			got, err := db.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want[name], got); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			assertOriginalsReclaimed(t, p, name)
		}
	}
	check(db)
	if st := db.Stats(); st.IngestTables == 0 || st.IngestEntries != metrics || st.IngestL0Bytes != 0 || st.IngestMemtableOverlaps != 0 {
		t.Fatalf("stages were not ingested below L0 without memtable overlap: %+v", st)
	}
	if fresh, err := db.Metadata(ctx, "ingest.fresh"); err != nil {
		t.Fatal(err)
	} else if other, ok := ids[fresh.ID]; ok {
		t.Fatalf("live metric reused ingested ID %d of %s", fresh.ID, other)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	defer db.Close()
	clear(ids)
	check(db)
	// Converted history continues to accept ordinary shared writes.
	sharedBefore, original := p.WriteStats()
	if original.Updates == 0 || original.Points == 0 {
		t.Fatalf("original-file writes were not counted: %+v", original)
	}
	cache.add(names[5], now-1, 7.5)
	p.store(names[5])
	if shared, _ := p.WriteStats(); shared.Updates != sharedBefore.Updates+1 || shared.Points != sharedBefore.Points+1 {
		t.Fatalf("shared write not counted: %+v -> %+v", sharedBefore, shared)
	}
	series, err := db.Fetch(ctx, names[5], int(now-10), int(now))
	if err != nil {
		t.Fatal(err)
	}
	if i := int(now-1-int64(series.FromTime)) / series.Step; i < 0 || i >= len(series.Values) || series.Values[i] != 7.5 {
		t.Fatalf("shared write after ingestion: %v", series.Values)
	}
	// A new metric created after restart must not collide with ingested IDs.
	cache.add("ingest.after", now-1, 1)
	p.store("ingest.after")
	if m, err := db.Metadata(ctx, "ingest.after"); err != nil {
		t.Fatal(err)
	} else if other, ok := ids[m.ID]; ok {
		t.Fatalf("new metric reused ingested ID %d of %s", m.ID, other)
	}
}

func TestIncrementalConversionDefersOnlyItsOwnWriteouts(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	now := time.Now().Unix()
	// A neighbour sharing the converting metric's store stripe.
	neighbour := ""
	for i := 0; neighbour == ""; i++ {
		if name := fmt.Sprintf("neighbour%d", i); storeMutexIndex(name) == storeMutexIndex("metric") {
			neighbour = name
		}
	}
	staged, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := p.convertFile(ctx, "metric", func(stage string) error {
			if stage == "staged" {
				close(staged)
				<-release
			}
			return nil
		})
		finished <- err
	}()
	<-staged
	cache.add(neighbour, now-3, 5)
	cache.add("metric", now-3, 6)
	stored := make(chan struct{})
	go func() {
		p.store(neighbour)
		p.store("metric")
		close(stored)
	}()
	select {
	case <-stored:
	case <-time.After(10 * time.Second):
		t.Fatal("writeouts blocked behind a long conversion")
	}
	if got, ok := cache.pop("metric"); !ok || len(got.Data) != 1 {
		t.Fatalf("converting metric's points were not left cached: %v %v", got, ok)
	} else {
		cache.add("metric", got.Data[0].Timestamp, got.Data[0].Value)
	}
	// A new metric has no original, so its writeout creates it in the store.
	if _, err := db.Metadata(ctx, neighbour); err != nil {
		t.Fatalf("neighbour was not persisted during the conversion: %v", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	p.store("metric")
	series, err := db.Fetch(ctx, "metric", int(now-10), int(now))
	if err != nil {
		t.Fatal(err)
	}
	if i := int(now-3-int64(series.FromTime)) / series.Step; series.Values[i] != 6 {
		t.Fatalf("deferred writeout lost after conversion: %v", series.Values)
	}
	if _, err := p.convertFile(ctx, "metric", nil); err != nil {
		t.Fatalf("finished conversion left its marker: %v", err)
	}
}

func TestReclaimedDirectorySyncsCoalesce(t *testing.T) {
	root := t.TempDir()
	dirs := []string{filepath.Join(root, "a"), filepath.Join(root, "a", "b"), filepath.Join(root, "missing", "leaf")}
	for _, dir := range dirs[:2] {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	var syncs directorySyncs
	var wg sync.WaitGroup
	errs := make(chan error, 300)
	for i := range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- syncs.sync(dirs[i%len(dirs)], root)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(syncs.dirs) != 0 {
		t.Fatalf("idle directories retained: %d", len(syncs.dirs))
	}
}
