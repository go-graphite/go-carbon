package chunkstore

import (
	"fmt"
	"testing"
	"time"
)

// Slow publication to one metadata cache shard must not retain any sibling
// metric stripe. The committed batch is already atomic in Pebble.
func TestPublicationCleanupDoesNotHoldMetricStripes(t *testing.T) {
	db, _ := openTestStore(t, t.TempDir(), Options{MetadataCacheSize: 8 << 20})
	defer closeTestStore(t, db)
	names := []string{"cleanup.a", "cleanup.b"}
	for db.metricStripe(names[0]) >= db.metricStripe(names[1]) {
		names[1] += ".b"
	}
	db.lockTransfer(names)
	finish := db.releaseImportStripes(names, nil)
	sh := &db.stateCache.shards[db.metricStripe(names[0])]
	sh.mu.Lock()
	hash := db.stateCache.hash
	entered := make(chan struct{})
	db.stateCache.hash = func(name string) uint64 {
		if name == names[0] {
			close(entered)
		}
		return hash(name)
	}
	done := make(chan struct{})
	go func() { finish(); close(done) }()
	defer func() { sh.mu.Unlock(); <-done }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup did not start")
	}
	for _, name := range names {
		stripe := &db.metricMu[db.metricStripe(name)]
		if !stripe.TryLock() {
			t.Fatalf("slow cache publication retained metric stripe for %s", name)
		}
		stripe.Unlock()
	}
}

func TestUpdateDecodesStateWithoutMetricStripe(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			db, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: batchTestCodec{}, MetadataCacheSize: 8 << 20})
			defer closeTestStore(t, db)
			const name = "decode.unlocked"
			createTestMetric(t, db, name)
			hash := db.stateCache.hash
			entered, release := make(chan struct{}), make(chan struct{})
			calls := 0
			db.stateCache.hash = func(s string) uint64 {
				if s == name {
					calls++
					if calls == 2 {
						close(entered)
						<-release
					}
				}
				return hash(s)
			}
			done := make(chan error, 1)
			go func() {
				if batch {
					done <- db.UpdateBatch(t.Context(), []MetricUpdate{{Name: name, Points: []Point{{Value: 1}}}}, nil)
				} else {
					done <- db.UpdateMany(t.Context(), name, []Point{{Value: 1}})
				}
			}()
			defer func() {
				close(release)
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("state lookup not reached")
			}
			stripe := &db.metricMu[db.metricStripe(name)]
			if !stripe.TryLock() {
				t.Fatal("state decoding retained the writer stripe")
			}
			if db.importPendingLocked(name) == nil {
				stripe.Unlock()
				t.Fatal("state decoding lost its name reservation")
			}
			stripe.Unlock()
		})
	}
}
