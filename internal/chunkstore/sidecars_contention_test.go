package chunkstore

import (
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestSidecarCommitDoesNotBlockOtherNamesOrMetrics(t *testing.T) {
	db, err := Open("store", Options{fs: vfs.NewMem(), CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		unlock := db.lockMetricMutation("slow")
		defer unlock()
		b := db.db.NewBatch()
		defer b.Close()
		done <- db.commitSidecars(b, "slow", &SidecarSummary{Present: true, Points: 10, Oldest: 100}, func(b *pebble.Batch) error {
			close(entered)
			<-release
			return errors.New("injected failed WAL")
		})
	}()
	<-entered
	fast := make(chan error, 1)
	go func() {
		unlock := db.lockMetricMutation("fast")
		defer unlock()
		b := db.db.NewBatch()
		defer b.Close()
		err := db.commitSidecars(b, "fast", &SidecarSummary{Present: true, Points: 7, Oldest: 200}, db.commit)
		if err == nil && db.Sidecars().Points != 7 {
			err = errors.New("uncommitted sidecar entered inventory")
		}
		fast <- err
	}()
	select {
	case err := <-fast:
		close(release)
		<-done
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		<-done
		<-fast
		t.Fatal("sidecar commit held global inventory lock across storage I/O")
	}
	if got := db.Sidecars(); got.Count != 1 || got.Points != 7 || got.Oldest != 200 {
		t.Fatal(got)
	}
}
