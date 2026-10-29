package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func awaitCatalogMembership(t *testing.T, s *Store) {
	t.Helper()
	select {
	case <-s.catalogMembership.done:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog snapshot scan did not finish")
	}
	if !s.catalogMembership.ready.Load() {
		t.Fatal("complete catalog scan did not enable membership checks")
	}
}

func TestCatalogMembershipConcurrentInsertions(t *testing.T) {
	f := newCatalogMembership(10000)
	if !f.mayContain("unknown") {
		t.Fatal("incomplete scan claimed absence")
	}
	f.ready.Store(true)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for i := range 1250 {
				name := fmt.Sprintf("metric.%d.%d", worker, i)
				f.add(name)
				if !f.mayContain(name) {
					t.Errorf("completed insertion was hidden: %s", name)
				}
			}
		})
	}
	wg.Wait()
	positives := 0
	for i := range 10000 {
		if f.mayContain(fmt.Sprintf("absent.%d", i)) {
			positives++
		}
	}
	if positives > 100 {
		t.Fatalf("membership filter admitted %d/10000 absent names", positives)
	}
	if len(newCatalogMembership(^uint64(0)).words)*8 != 64<<20 || len(newCatalogMembership(0).words)*8 != 4096 {
		t.Fatal("catalog filter exceeded its memory envelope")
	}
}

func TestCatalogMembershipJoinsSnapshotAndConcurrentCatalogWrites(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	awaitCatalogMembership(t, s)
	ctx := context.Background()
	createTestMetric(t, s, "before.snapshot")
	// Drive the real scan with an older snapshot, while real Create calls
	// publish newer catalog entries before the scanner is allowed to run.
	old := s.catalogMembership
	old.halt()
	view := s.db.NewSnapshot()
	f := newCatalogMembership(1024)
	s.catalogMembership = f
	createTestMetric(t, s, "after.snapshot")
	go s.loadCatalogMembership(view, f)
	awaitCatalogMembership(t, s)
	for _, name := range []string{"before.snapshot", "after.snapshot"} {
		if _, err := s.Metadata(ctx, name); err != nil {
			t.Fatalf("lost catalog entry %s: %v", name, err)
		}
	}
	if err := s.Delete(ctx, "after.snapshot"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Metadata(ctx, "after.snapshot"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted metric remains visible: %v", err)
	}
	createTestMetric(t, s, "after.snapshot")
	if _, err := s.Metadata(ctx, "after.snapshot"); err != nil {
		t.Fatalf("recreated metric was hidden: %v", err)
	}
}

func TestCatalogMembershipSkipsUnrelatedMetricLock(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	awaitCatalogMembership(t, s)
	name := "unconverted.metric"
	if s.catalogMembership.mayContain(name) {
		t.Fatal("empty catalog cannot contain an original-only metric")
	}
	unlock := s.lockMetric(name)
	result := make(chan error, 1)
	go func() { _, err := s.Metadata(context.Background(), name); result <- err }()
	select {
	case err := <-result:
		unlock()
		if !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		unlock()
		<-result
		t.Fatal("known-absent original waited for a shared metric stripe")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Metadata(canceled, name); !errors.Is(err, context.Canceled) {
		t.Fatalf("membership masked cancellation: %v", err)
	}
	failure := errors.New("ownership failure")
	s.ownershipFailure.Store(&ownershipError{failure})
	if _, err := s.Metadata(context.Background(), name); !errors.Is(err, failure) {
		t.Fatalf("membership masked ownership failure: %v", err)
	}
}

func TestCatalogMembershipPreservesStagedOwnershipAndReopen(t *testing.T) {
	fs := vfs.NewMem()
	s := openFileMigrationStore(t, fs)
	awaitCatalogMembership(t, s)
	ctx := context.Background()
	want := fileMigrationSnapshot()
	staged, err := s.StageFileSnapshot(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	assertUnownedFile(t, s, want.Metadata.Name)
	owned, err := s.PublishFileOwnership(ctx, want, staged)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteFileCleanup(ctx, owned); err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	s = openFileMigrationStore(t, fs)
	defer closeTestStore(t, s)
	awaitCatalogMembership(t, s)
	got, err := s.Snapshot(ctx, want.Metadata.Name)
	if err != nil || EqualSnapshots(want, got) != nil {
		t.Fatalf("published archive bits changed across reopen: %v", err)
	}
	if _, err := s.Metadata(ctx, want.Metadata.Name); err != nil {
		t.Fatalf("reopened catalog entry was hidden: %v", err)
	}
}

// Exercise the same reads with the filter enabled and with the ordinary catalog
// lookup, including incomplete scans, staged ownership, deletion and recreation.
func TestCatalogMembershipFetchMatchesCatalogLookup(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	awaitCatalogMembership(t, s)
	s.now = func() time.Time { return time.Unix(1790000100, 0) }
	ctx := context.Background()
	compare := func(name string) {
		t.Helper()
		s.catalogMembership.ready.Store(false)
		want, wantErr := s.Fetch(ctx, name, 1789999900, 1790000100)
		s.catalogMembership.ready.Store(true)
		got, gotErr := s.Fetch(ctx, name, 1789999900, 1790000100)
		if got != nil && want != nil {
			gotBits, wantBits := make([]uint64, len(got.Values)), make([]uint64, len(want.Values))
			for i, value := range got.Values {
				gotBits[i] = math.Float64bits(value)
			}
			for i, value := range want.Values {
				wantBits[i] = math.Float64bits(value)
			}
			if !reflect.DeepEqual(gotBits, wantBits) {
				t.Fatalf("filtered fetch %s changed exact value bits", name)
			}
			gotCopy, wantCopy := *got, *want
			gotCopy.Values, wantCopy.Values = nil, nil
			if !reflect.DeepEqual(gotCopy, wantCopy) {
				t.Fatalf("filtered fetch %s changed series metadata", name)
			}
		}
		if (got == nil) != (want == nil) || !errors.Is(gotErr, wantErr) {
			t.Fatalf("filtered fetch %s differs: got %v/%v, want %v/%v", name, got, gotErr, want, wantErr)
		}
	}
	compare("absent.metric")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Fetch(canceled, "absent.metric", 0, 1000); !errors.Is(err, context.Canceled) {
		t.Fatalf("membership masked cancellation: %v", err)
	}
	if _, err := s.Fetch(ctx, "absent.metric", 1000, 0); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("membership masked invalid range: %v", err)
	}
	want := fileMigrationSnapshot()
	staged, err := s.StageFileSnapshot(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	compare(want.Metadata.Name)
	assertUnownedFile(t, s, want.Metadata.Name)
	owned, err := s.PublishFileOwnership(ctx, want, staged)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteFileCleanup(ctx, owned); err != nil {
		t.Fatal(err)
	}
	compare(want.Metadata.Name)
	if got, err := s.Fetch(ctx, want.Metadata.Name, 1789999900, 1790000100); err != nil || got == nil || len(got.Values) == 0 {
		t.Fatalf("published metric must remain readable: %v/%v", got, err)
	}
	if err := s.Delete(ctx, want.Metadata.Name); err != nil {
		t.Fatal(err)
	}
	compare(want.Metadata.Name)
	createTestMetric(t, s, want.Metadata.Name)
	compare(want.Metadata.Name)
	failure := errors.New("ownership failure")
	s.ownershipFailure.Store(&ownershipError{failure})
	if _, err := s.Fetch(ctx, "absent.metric", 0, 1000); !errors.Is(err, failure) {
		t.Fatalf("membership masked ownership failure: %v", err)
	}
}

// pausedCatalogStore reopens a store whose catalog spans several scan
// iterators, with the scan paused as it opens each one. The returned channel
// receives each pause; send on resume to continue, or set free to stop
// pausing.
func pausedCatalogStore(t *testing.T) (s *Store, names []string, paused <-chan struct{}, resume chan<- struct{}, free *atomic.Bool) {
	t.Helper()
	fs := vfs.NewMem()
	s = openFileMigrationStore(t, fs)
	awaitCatalogMembership(t, s)
	for i := range 4 * 1024 {
		names = append(names, fmt.Sprintf("catalog.metric.%05d", i))
		createTestMetric(t, s, names[i])
	}
	writeFiller(t, s, 0)
	if err := s.db.Compact([]byte{0}, []byte{255}, true); err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)

	pauses, resumes := make(chan struct{}), make(chan struct{})
	free = new(atomic.Bool)
	refresh := membershipRefresh
	membershipRefresh = 0 // a new iterator every 1024 names
	catalogScanPause = func(stop <-chan struct{}) {
		if free.Load() {
			return
		}
		select {
		case pauses <- struct{}{}:
		case <-stop:
			return
		}
		select {
		case <-resumes:
		case <-stop:
		}
	}
	t.Cleanup(func() { catalogScanPause, membershipRefresh = nil, refresh })
	return openFileMigrationStore(t, fs), names, pauses, resumes, free
}

func writeFiller(t *testing.T, s *Store, version byte) {
	t.Helper()
	b := s.db.NewBatch()
	for i := range 20000 {
		if err := b.Set(fmt.Appendf(nil, "x/%06d", i), []byte{version}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func awaitPause(t *testing.T, paused <-chan struct{}) {
	t.Helper()
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		t.Fatal("catalog scan did not reach its next iterator")
	}
}

// The scan reads one consistent snapshot through a series of iterators, so
// tables that compactions replace during a long scan are released as it
// goes, not all at once when it ends or the store closes.
func TestCatalogScanReleasesReplacedTables(t *testing.T) {
	s, names, paused, resume, free := pausedCatalogStore(t)
	defer closeTestStore(t, s)
	awaitPause(t, paused)
	// Replace every table the scan's first iterator was opened on.
	writeFiller(t, s, 1)
	if err := s.db.Compact([]byte{0}, []byte{255}, true); err != nil {
		t.Fatal(err)
	}
	if zombies := s.db.Metrics().Table.ZombieCount; zombies == 0 {
		t.Fatal("the scan's iterator pinned no replaced tables; the test needs some")
	}
	resume <- struct{}{}
	awaitPause(t, paused)
	if zombies := s.db.Metrics().Table.ZombieCount; zombies != 0 {
		t.Fatalf("scan still pins %d replaced tables after moving to a new iterator", zombies)
	}
	free.Store(true)
	resume <- struct{}{}
	awaitCatalogMembership(t, s)
	for _, name := range names {
		if !s.catalogMembership.mayContain(name) {
			t.Fatalf("scan across iterators missed %s", name)
		}
	}
	if s.catalogMembership.mayContain("absent.metric") {
		t.Fatal("membership admitted an absent name")
	}
}

// StopBackground ends the scan before Close, which then has nothing to wait
// for, and both may run at once.
func TestStopBackgroundEndsCatalogScanBeforeClose(t *testing.T) {
	s, _, paused, _, _ := pausedCatalogStore(t)
	awaitPause(t, paused)
	if s.BackgroundStopped() {
		t.Fatal("background reported stopped while the scan runs")
	}
	s.StopBackground()
	select {
	case <-s.catalogMembership.done:
	default:
		t.Fatal("StopBackground returned with the scan running")
	}
	if !s.BackgroundStopped() || s.catalogMembership.ready.Load() || !s.catalogMembership.mayContain("absent.metric") {
		t.Fatal("a stopped scan must leave membership checks disabled")
	}
	again := make(chan struct{})
	go func() { defer close(again); s.StopBackground() }()
	closeTestStore(t, s)
	<-again
}

func TestWALBytesTrackWrites(t *testing.T) {
	t.Parallel()
	s, err := Open(t.TempDir(), Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	before := s.WALBytes()
	writeFiller(t, s, 0)
	written := s.WALBytes()
	if written <= before {
		t.Fatal("WAL bytes did not grow with a write", before, written)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.logSync(); err != nil {
		t.Fatal(err)
	}
	if err := s.syncWAL(); err != nil {
		t.Fatal(err)
	}
	if got := s.WALBytes(); got != written {
		t.Fatal("WAL bytes changed without a write", written, got)
	}
}

// The dirty check compares two observations of the WAL. Empty sync records
// must never temporarily inflate one: counting that record later could cancel
// out a real write and hide it from the second observation.
func TestWALBytesWaitsForSyncAccounting(t *testing.T) {
	t.Parallel()
	s, err := Open(t.TempDir(), Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	before := s.WALBytes()
	s.walSyncMu.Lock()
	b := s.db.NewBatch()
	_ = b.LogData(nil, nil)
	size := uint64(len(b.Repr()))
	if err := b.Commit(pebble.Sync); err != nil {
		s.walSyncMu.Unlock()
		t.Fatal(err)
	}
	_ = b.Close()
	observed := make(chan uint64, 1)
	go func() { observed <- s.WALBytes() }()
	select {
	case got := <-observed:
		s.walSyncMu.Unlock()
		t.Fatalf("observed an unaccounted sync record: %d, want %d", got, before)
	case <-time.After(20 * time.Millisecond):
	}
	s.syncWALBytes += size
	s.walSyncMu.Unlock()
	select {
	case got := <-observed:
		if got != before {
			t.Fatalf("empty sync changed the write position: %d, want %d", got, before)
		}
	case <-time.After(time.Second):
		t.Fatal("WAL observation did not resume after the sync")
	}
}
